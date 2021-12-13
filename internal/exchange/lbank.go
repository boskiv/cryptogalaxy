package exchange

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/milkywaybrain/cryptogalaxy/internal/config"
	"github.com/milkywaybrain/cryptogalaxy/internal/connector"
	"github.com/milkywaybrain/cryptogalaxy/internal/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

// StartLBank is for starting lbank exchange functions.
func StartLBank(appCtx context.Context, markets []config.Market, retry *config.Retry, connCfg *config.Connection) error {

	// If any error occurs or connection is lost, retry the exchange functions with a time gap till it reaches
	// a configured number of retry.
	// Retry counter will be reset back to zero if the elapsed time since the last retry is greater than the configured one.
	var retryCount int
	lastRetryTime := time.Now()

	for {
		err := newLBank(appCtx, markets, connCfg)
		if err != nil {
			log.Error().Err(err).Str("exchange", "lbank").Msg("error occurred")
			if retry.Number == 0 {
				return errors.New("not able to connect lbank exchange. please check the log for details")
			}
			if retry.ResetSec == 0 || time.Since(lastRetryTime).Seconds() < float64(retry.ResetSec) {
				retryCount++
			} else {
				retryCount = 1
			}
			lastRetryTime = time.Now()
			if retryCount > retry.Number {
				err = fmt.Errorf("not able to connect lbank exchange even after %d retry", retry.Number)
				log.Error().Err(err).Str("exchange", "lbank").Msg("")
				return err
			}

			log.Error().Str("exchange", "lbank").Int("retry", retryCount).Msg(fmt.Sprintf("retrying functions in %d seconds", retry.GapSec))
			tick := time.NewTicker(time.Duration(retry.GapSec) * time.Second)
			select {
			case <-tick.C:
				tick.Stop()

			// Return, if there is any error from another exchange.
			case <-appCtx.Done():
				log.Error().Str("exchange", "lbank").Msg("ctx canceled, return from StartLBank")
				return appCtx.Err()
			}
		}
	}
}

type lbank struct {
	ws                  connector.Websocket
	rest                *connector.REST
	connCfg             *config.Connection
	cfgMap              map[cfgLookupKey]cfgLookupVal
	ter                 *storage.Terminal
	es                  *storage.ElasticSearch
	mysql               *storage.MySQL
	influx              *storage.InfluxDB
	nats                *storage.NATS
	clickhouse          *storage.ClickHouse
	s3                  *storage.S3
	wsTerTickers        chan []storage.Ticker
	wsTerTrades         chan []storage.Trade
	wsMysqlTickers      chan []storage.Ticker
	wsMysqlTrades       chan []storage.Trade
	wsEsTickers         chan []storage.Ticker
	wsEsTrades          chan []storage.Trade
	wsInfluxTickers     chan []storage.Ticker
	wsInfluxTrades      chan []storage.Trade
	wsNatsTickers       chan []storage.Ticker
	wsNatsTrades        chan []storage.Trade
	wsClickHouseTickers chan []storage.Ticker
	wsClickHouseTrades  chan []storage.Trade
	wsS3Tickers         chan []storage.Ticker
	wsS3Trades          chan []storage.Trade
}

type wsSubLBank struct {
	Action    string `json:"action"`
	Subscribe string `json:"subscribe"`
	Pair      string `json:"pair"`
}

type wsRespLBank struct {
	Type          string        `json:"type"`
	Pair          string        `json:"pair"`
	Trade         respDataLBank `json:"trade"`
	Ticker        respDataLBank `json:"tick"`
	Action        string        `json:"action"`
	PingID        string        `json:"ping"`
	mktCommitName string
}

type restRespTickerLBank struct {
	Data []restRespTickerDetailLBank `json:"data"`
}

type restRespTickerDetailLBank struct {
	Ticker respDataLBank `json:"ticker"`
}

type restRespTradeLBank struct {
	Data []respDataLBank `json:"data"`
}

type respDataLBank struct {
	TradeID     string  `json:"tid"`
	Direction   string  `json:"direction"`
	Volume      float64 `json:"volume"`
	Amount      float64 `json:"amount"`
	TickerPrice float64 `json:"latest"`
	TradePrice  float64 `json:"price"`
	WsTime      string  `json:"TS"`
	RESTTime    int64   `json:"date_ms"`
}

func newLBank(appCtx context.Context, markets []config.Market, connCfg *config.Connection) error {

	// If any exchange function fails, force all the other functions to stop and return.
	lbankErrGroup, ctx := errgroup.WithContext(appCtx)

	l := lbank{connCfg: connCfg}

	err := l.cfgLookup(markets)
	if err != nil {
		return err
	}

	var (
		wsCount   int
		restCount int
	)

	for _, market := range markets {
		for _, info := range market.Info {
			switch info.Connector {
			case "websocket":
				if wsCount == 0 {

					err = l.connectWs(ctx)
					if err != nil {
						return err
					}

					lbankErrGroup.Go(func() error {
						return l.closeWsConnOnError(ctx)
					})

					lbankErrGroup.Go(func() error {
						return l.readWs(ctx)
					})

					if l.ter != nil {
						lbankErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, l.ter, l.wsTerTickers)
						})
						lbankErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, l.ter, l.wsTerTrades)
						})
					}

					if l.mysql != nil {
						lbankErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, l.mysql, l.wsMysqlTickers)
						})
						lbankErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, l.mysql, l.wsMysqlTrades)
						})
					}

					if l.es != nil {
						lbankErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, l.es, l.wsEsTickers)
						})
						lbankErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, l.es, l.wsEsTrades)
						})
					}

					if l.influx != nil {
						lbankErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, l.influx, l.wsInfluxTickers)
						})
						lbankErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, l.influx, l.wsInfluxTrades)
						})
					}

					if l.nats != nil {
						lbankErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, l.nats, l.wsNatsTickers)
						})
						lbankErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, l.nats, l.wsNatsTrades)
						})
					}

					if l.clickhouse != nil {
						lbankErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, l.clickhouse, l.wsClickHouseTickers)
						})
						lbankErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, l.clickhouse, l.wsClickHouseTrades)
						})
					}

					if l.s3 != nil {
						lbankErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, l.s3, l.wsS3Tickers)
						})
						lbankErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, l.s3, l.wsS3Trades)
						})
					}
				}

				err = l.subWsChannel(market.ID, info.Channel)
				if err != nil {
					return err
				}

				wsCount++
			case "rest":
				if restCount == 0 {
					err = l.connectRest()
					if err != nil {
						return err
					}
				}

				var mktCommitName string
				if market.CommitName != "" {
					mktCommitName = market.CommitName
				} else {
					mktCommitName = market.ID
				}
				mktID := market.ID
				channel := info.Channel
				restPingIntSec := info.RESTPingIntSec
				lbankErrGroup.Go(func() error {
					return l.processREST(ctx, mktID, mktCommitName, channel, restPingIntSec)
				})

				restCount++
			}
		}
	}

	err = lbankErrGroup.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (l *lbank) cfgLookup(markets []config.Market) error {

	// Configurations flat map is prepared for easy lookup later in the app.
	l.cfgMap = make(map[cfgLookupKey]cfgLookupVal)
	for _, market := range markets {
		var marketCommitName string
		if market.CommitName != "" {
			marketCommitName = market.CommitName
		} else {
			marketCommitName = market.ID
		}
		for _, info := range market.Info {
			key := cfgLookupKey{market: market.ID, channel: info.Channel}
			val := cfgLookupVal{}
			val.connector = info.Connector
			val.wsConsiderIntSec = info.WsConsiderIntSec
			for _, str := range info.Storages {
				switch str {
				case "terminal":
					val.terStr = true
					if l.ter == nil {
						l.ter = storage.GetTerminal()
						l.wsTerTickers = make(chan []storage.Ticker, 1)
						l.wsTerTrades = make(chan []storage.Trade, 1)
					}
				case "mysql":
					val.mysqlStr = true
					if l.mysql == nil {
						l.mysql = storage.GetMySQL()
						l.wsMysqlTickers = make(chan []storage.Ticker, 1)
						l.wsMysqlTrades = make(chan []storage.Trade, 1)
					}
				case "elastic_search":
					val.esStr = true
					if l.es == nil {
						l.es = storage.GetElasticSearch()
						l.wsEsTickers = make(chan []storage.Ticker, 1)
						l.wsEsTrades = make(chan []storage.Trade, 1)
					}
				case "influxdb":
					val.influxStr = true
					if l.influx == nil {
						l.influx = storage.GetInfluxDB()
						l.wsInfluxTickers = make(chan []storage.Ticker, 1)
						l.wsInfluxTrades = make(chan []storage.Trade, 1)
					}
				case "nats":
					val.natsStr = true
					if l.nats == nil {
						l.nats = storage.GetNATS()
						l.wsNatsTickers = make(chan []storage.Ticker, 1)
						l.wsNatsTrades = make(chan []storage.Trade, 1)
					}
				case "clickhouse":
					val.clickHouseStr = true
					if l.clickhouse == nil {
						l.clickhouse = storage.GetClickHouse()
						l.wsClickHouseTickers = make(chan []storage.Ticker, 1)
						l.wsClickHouseTrades = make(chan []storage.Trade, 1)
					}
				case "s3":
					val.s3Str = true
					if l.s3 == nil {
						l.s3 = storage.GetS3()
						l.wsS3Tickers = make(chan []storage.Ticker, 1)
						l.wsS3Trades = make(chan []storage.Trade, 1)
					}
				}
			}
			val.mktCommitName = marketCommitName
			l.cfgMap[key] = val
		}
	}
	return nil
}

func (l *lbank) connectWs(ctx context.Context) error {
	ws, err := connector.NewWebsocket(ctx, &l.connCfg.WS, config.LBankWebsocketURL)
	if err != nil {
		if !errors.Is(err, ctx.Err()) {
			logErrStack(err)
		}
		return err
	}
	l.ws = ws
	log.Info().Str("exchange", "lbank").Msg("websocket connected")
	return nil
}

// closeWsConnOnError closes websocket connection if there is any error in app context.
// This will unblock all read and writes on websocket.
func (l *lbank) closeWsConnOnError(ctx context.Context) error {
	<-ctx.Done()
	err := l.ws.Conn.Close()
	if err != nil {
		return err
	}
	return ctx.Err()
}

// pongWs sends pong response to websocket server upon receiving ping.
func (l *lbank) pongWs(pingID string) error {
	frame, err := jsoniter.Marshal(map[string]string{"action": "pong", "pong": pingID})
	if err != nil {
		logErrStack(err)
		return err
	}
	err = l.ws.Write(frame)
	if err != nil {
		if errors.Is(err, net.ErrClosed) {
			err = errors.New("context canceled")
		} else {
			logErrStack(err)
		}
		return err
	}
	return nil
}

// subWsChannel sends channel subscription requests to the websocket server.
func (l *lbank) subWsChannel(market string, channel string) error {
	if channel == "ticker" {
		channel = "tick"
	}
	sub := wsSubLBank{
		Action:    "subscribe",
		Subscribe: channel,
		Pair:      market,
	}
	frame, err := jsoniter.Marshal(&sub)
	if err != nil {
		logErrStack(err)
		return err
	}
	err = l.ws.Write(frame)
	if err != nil {
		if errors.Is(err, net.ErrClosed) {
			err = errors.New("context canceled")
		} else {
			logErrStack(err)
		}
		return err
	}
	return nil
}

// readWs reads ticker / trade data from websocket channels.
func (l *lbank) readWs(ctx context.Context) error {

	// To avoid data race, creating a new local lookup map.
	cfgLookup := make(map[cfgLookupKey]cfgLookupVal, len(l.cfgMap))
	for k, v := range l.cfgMap {
		cfgLookup[k] = v
	}

	// See influxTimeVal struct doc for details.
	itv := influxTimeVal{}
	if l.influx != nil {
		itv.TickerMap = make(map[string]int64)
		itv.TradeMap = make(map[string]int64)
	}

	cd := commitData{
		terTickers:        make([]storage.Ticker, 0, l.connCfg.Terminal.TickerCommitBuf),
		terTrades:         make([]storage.Trade, 0, l.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers:      make([]storage.Ticker, 0, l.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:       make([]storage.Trade, 0, l.connCfg.MySQL.TradeCommitBuf),
		esTickers:         make([]storage.Ticker, 0, l.connCfg.ES.TickerCommitBuf),
		esTrades:          make([]storage.Trade, 0, l.connCfg.ES.TradeCommitBuf),
		influxTickers:     make([]storage.Ticker, 0, l.connCfg.InfluxDB.TickerCommitBuf),
		influxTrades:      make([]storage.Trade, 0, l.connCfg.InfluxDB.TradeCommitBuf),
		natsTickers:       make([]storage.Ticker, 0, l.connCfg.NATS.TickerCommitBuf),
		natsTrades:        make([]storage.Trade, 0, l.connCfg.NATS.TradeCommitBuf),
		clickHouseTickers: make([]storage.Ticker, 0, l.connCfg.ClickHouse.TickerCommitBuf),
		clickHouseTrades:  make([]storage.Trade, 0, l.connCfg.ClickHouse.TradeCommitBuf),
		s3Tickers:         make([]storage.Ticker, 0, l.connCfg.S3.TickerCommitBuf),
		s3Trades:          make([]storage.Trade, 0, l.connCfg.S3.TradeCommitBuf),
	}

	log.Debug().Str("exchange", "lbank").Str("func", "readWs").Msg("unlike other exchanges lbank does not send channel subscribed success message in a proper format")

	for {
		select {
		default:
			frame, err := l.ws.Read()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					err = errors.New("context canceled")
				} else {
					if err == io.EOF {
						err = errors.Wrap(err, "connection close by exchange server")
					}
					logErrStack(err)
				}
				return err
			}
			if len(frame) == 0 {
				continue
			}

			wr := wsRespLBank{}
			err = jsoniter.Unmarshal(frame, &wr)
			if err != nil {
				logErrStack(err)
				return err
			}

			if wr.Action == "ping" && wr.PingID != "" {
				err := l.pongWs(wr.PingID)
				if err != nil {
					return err
				}
				continue
			}

			if wr.Type == "tick" {
				wr.Type = "ticker"
			}

			// Consider frame only in configured interval, otherwise ignore it.
			switch wr.Type {
			case "ticker", "trade":
				key := cfgLookupKey{market: wr.Pair, channel: wr.Type}
				val := cfgLookup[key]
				if val.wsConsiderIntSec == 0 || time.Since(val.wsLastUpdated).Seconds() >= float64(val.wsConsiderIntSec) {
					val.wsLastUpdated = time.Now()
					wr.mktCommitName = val.mktCommitName
					cfgLookup[key] = val
				} else {
					continue
				}

				err := l.processWs(ctx, &wr, &cd, &itv)
				if err != nil {
					return err
				}
			}

		// Return, if there is any error from another function or exchange.
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// processWs receives ticker / trade data,
// transforms it to a common ticker / trade store format,
// buffers the same in memory and
// then sends it to different storage systems for commit through go channels.
func (l *lbank) processWs(ctx context.Context, wr *wsRespLBank, cd *commitData, itv *influxTimeVal) error {
	switch wr.Type {
	case "ticker":
		ticker := storage.Ticker{}
		ticker.Exchange = "lbank"
		ticker.MktID = wr.Pair
		ticker.MktCommitName = wr.mktCommitName
		ticker.Price = wr.Ticker.TickerPrice
		ticker.Timestamp = time.Now().UTC()

		key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
		val := l.cfgMap[key]
		if val.terStr {
			cd.terTickersCount++
			cd.terTickers = append(cd.terTickers, ticker)
			if cd.terTickersCount == l.connCfg.Terminal.TickerCommitBuf {
				select {
				case l.wsTerTickers <- cd.terTickers:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.terTickersCount = 0
				cd.terTickers = nil
			}
		}
		if val.mysqlStr {
			cd.mysqlTickersCount++
			cd.mysqlTickers = append(cd.mysqlTickers, ticker)
			if cd.mysqlTickersCount == l.connCfg.MySQL.TickerCommitBuf {
				select {
				case l.wsMysqlTickers <- cd.mysqlTickers:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.mysqlTickersCount = 0
				cd.mysqlTickers = nil
			}
		}
		if val.esStr {
			cd.esTickersCount++
			cd.esTickers = append(cd.esTickers, ticker)
			if cd.esTickersCount == l.connCfg.ES.TickerCommitBuf {
				select {
				case l.wsEsTickers <- cd.esTickers:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.esTickersCount = 0
				cd.esTickers = nil
			}
		}
		if val.influxStr {
			val := itv.TickerMap[ticker.MktCommitName]
			if val == 0 || val == 999999 {
				val = 1
			} else {
				val++
			}
			itv.TickerMap[ticker.MktCommitName] = val
			ticker.InfluxVal = val

			cd.influxTickersCount++
			cd.influxTickers = append(cd.influxTickers, ticker)
			if cd.influxTickersCount == l.connCfg.InfluxDB.TickerCommitBuf {
				select {
				case l.wsInfluxTickers <- cd.influxTickers:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.influxTickersCount = 0
				cd.influxTickers = nil
			}
		}
		if val.natsStr {
			cd.natsTickersCount++
			cd.natsTickers = append(cd.natsTickers, ticker)
			if cd.natsTickersCount == l.connCfg.NATS.TickerCommitBuf {
				select {
				case l.wsNatsTickers <- cd.natsTickers:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.natsTickersCount = 0
				cd.natsTickers = nil
			}
		}
		if val.clickHouseStr {
			cd.clickHouseTickersCount++
			cd.clickHouseTickers = append(cd.clickHouseTickers, ticker)
			if cd.clickHouseTickersCount == l.connCfg.ClickHouse.TickerCommitBuf {
				select {
				case l.wsClickHouseTickers <- cd.clickHouseTickers:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.clickHouseTickersCount = 0
				cd.clickHouseTickers = nil
			}
		}
		if val.s3Str {
			cd.s3TickersCount++
			cd.s3Tickers = append(cd.s3Tickers, ticker)
			if cd.s3TickersCount == l.connCfg.S3.TickerCommitBuf {
				select {
				case l.wsS3Tickers <- cd.s3Tickers:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.s3TickersCount = 0
				cd.s3Tickers = nil
			}
		}
	case "trade":
		trade := storage.Trade{}
		trade.Exchange = "lbank"
		trade.MktID = wr.Pair
		trade.MktCommitName = wr.mktCommitName

		if wr.Trade.Direction == "buy_market" {
			wr.Trade.Direction = "buy"
		} else if wr.Trade.Direction == "sell_market" {
			wr.Trade.Direction = "sell"
		}
		trade.Side = wr.Trade.Direction

		trade.Size = wr.Trade.Volume
		trade.Price = wr.Trade.TradePrice

		// Time sent is in string format.
		timestamp, err := time.Parse("2006-01-02T15:04:05.999", wr.Trade.WsTime)
		if err != nil {
			logErrStack(err)
			return err
		}
		trade.Timestamp = timestamp

		key := cfgLookupKey{market: trade.MktID, channel: "trade"}
		val := l.cfgMap[key]
		if val.terStr {
			cd.terTradesCount++
			cd.terTrades = append(cd.terTrades, trade)
			if cd.terTradesCount == l.connCfg.Terminal.TradeCommitBuf {
				select {
				case l.wsTerTrades <- cd.terTrades:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.terTradesCount = 0
				cd.terTrades = nil
			}
		}
		if val.mysqlStr {
			cd.mysqlTradesCount++
			cd.mysqlTrades = append(cd.mysqlTrades, trade)
			if cd.mysqlTradesCount == l.connCfg.MySQL.TradeCommitBuf {
				select {
				case l.wsMysqlTrades <- cd.mysqlTrades:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.mysqlTradesCount = 0
				cd.mysqlTrades = nil
			}
		}
		if val.esStr {
			cd.esTradesCount++
			cd.esTrades = append(cd.esTrades, trade)
			if cd.esTradesCount == l.connCfg.ES.TradeCommitBuf {
				select {
				case l.wsEsTrades <- cd.esTrades:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.esTradesCount = 0
				cd.esTrades = nil
			}
		}
		if val.influxStr {
			val := itv.TradeMap[trade.MktCommitName]
			if val == 0 || val == 999999 {
				val = 1
			} else {
				val++
			}
			itv.TradeMap[trade.MktCommitName] = val
			trade.InfluxVal = val

			cd.influxTradesCount++
			cd.influxTrades = append(cd.influxTrades, trade)
			if cd.influxTradesCount == l.connCfg.InfluxDB.TradeCommitBuf {
				select {
				case l.wsInfluxTrades <- cd.influxTrades:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.influxTradesCount = 0
				cd.influxTrades = nil
			}
		}
		if val.natsStr {
			cd.natsTradesCount++
			cd.natsTrades = append(cd.natsTrades, trade)
			if cd.natsTradesCount == l.connCfg.NATS.TradeCommitBuf {
				select {
				case l.wsNatsTrades <- cd.natsTrades:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.natsTradesCount = 0
				cd.natsTrades = nil
			}
		}
		if val.clickHouseStr {
			cd.clickHouseTradesCount++
			cd.clickHouseTrades = append(cd.clickHouseTrades, trade)
			if cd.clickHouseTradesCount == l.connCfg.ClickHouse.TradeCommitBuf {
				select {
				case l.wsClickHouseTrades <- cd.clickHouseTrades:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.clickHouseTradesCount = 0
				cd.clickHouseTrades = nil
			}
		}
		if val.s3Str {
			cd.s3TradesCount++
			cd.s3Trades = append(cd.s3Trades, trade)
			if cd.s3TradesCount == l.connCfg.S3.TradeCommitBuf {
				select {
				case l.wsS3Trades <- cd.s3Trades:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.s3TradesCount = 0
				cd.s3Trades = nil
			}
		}
	}
	return nil
}

func (l *lbank) connectRest() error {
	rest, err := connector.GetREST()
	if err != nil {
		logErrStack(err)
		return err
	}
	l.rest = rest
	log.Info().Str("exchange", "lbank").Msg("REST connection setup is done")
	return nil
}

// processREST queries exchange for ticker / trade data through REST API in configured intervals,
// transforms it to a common ticker / trade store format,
// buffers the same in memory and
// then sends it to different storage systems for commit through go channels.
func (l *lbank) processREST(ctx context.Context, mktID string, mktCommitName string, channel string, interval int) error {
	var (
		req *http.Request
		q   url.Values
		err error

		// See influxTimeVal (exchange.go) struct doc for details.
		influxTickerTime int64
		influxTradeTime  int64
	)

	cd := commitData{
		terTickers:        make([]storage.Ticker, 0, l.connCfg.Terminal.TickerCommitBuf),
		terTrades:         make([]storage.Trade, 0, l.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers:      make([]storage.Ticker, 0, l.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:       make([]storage.Trade, 0, l.connCfg.MySQL.TradeCommitBuf),
		esTickers:         make([]storage.Ticker, 0, l.connCfg.ES.TickerCommitBuf),
		esTrades:          make([]storage.Trade, 0, l.connCfg.ES.TradeCommitBuf),
		influxTickers:     make([]storage.Ticker, 0, l.connCfg.InfluxDB.TickerCommitBuf),
		influxTrades:      make([]storage.Trade, 0, l.connCfg.InfluxDB.TradeCommitBuf),
		natsTickers:       make([]storage.Ticker, 0, l.connCfg.NATS.TickerCommitBuf),
		natsTrades:        make([]storage.Trade, 0, l.connCfg.NATS.TradeCommitBuf),
		clickHouseTickers: make([]storage.Ticker, 0, l.connCfg.ClickHouse.TickerCommitBuf),
		clickHouseTrades:  make([]storage.Trade, 0, l.connCfg.ClickHouse.TradeCommitBuf),
		s3Tickers:         make([]storage.Ticker, 0, l.connCfg.S3.TickerCommitBuf),
		s3Trades:          make([]storage.Trade, 0, l.connCfg.S3.TradeCommitBuf),
	}

	switch channel {
	case "ticker":
		req, err = l.rest.Request(ctx, "GET", config.LBankRESTBaseURL+"ticker.do")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)
	case "trade":
		req, err = l.rest.Request(ctx, "GET", config.LBankRESTBaseURL+"trades.do")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)

		// Querying for 100 trades.
		// If the configured interval gap is big, then maybe it will not return all the trades.
		// Better to use websocket.
		q.Add("size", strconv.Itoa(100))
	}

	tick := time.NewTicker(time.Duration(interval) * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:

			switch channel {
			case "ticker":
				req.URL.RawQuery = q.Encode()
				resp, err := l.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := restRespTickerLBank{}
				if err := jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				r := rr.Data[0].Ticker
				ticker := storage.Ticker{
					Exchange:      "lbank",
					MktID:         mktID,
					MktCommitName: mktCommitName,
					Price:         r.TickerPrice,
					Timestamp:     time.Now().UTC(),
				}

				key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
				val := l.cfgMap[key]
				if val.terStr {
					cd.terTickersCount++
					cd.terTickers = append(cd.terTickers, ticker)
					if cd.terTickersCount == l.connCfg.Terminal.TickerCommitBuf {
						err := l.ter.CommitTickers(ctx, cd.terTickers)
						if err != nil {
							if !errors.Is(err, ctx.Err()) {
								logErrStack(err)
							}
							return err
						}
						cd.terTickersCount = 0
						cd.terTickers = nil
					}
				}
				if val.mysqlStr {
					cd.mysqlTickersCount++
					cd.mysqlTickers = append(cd.mysqlTickers, ticker)
					if cd.mysqlTickersCount == l.connCfg.MySQL.TickerCommitBuf {
						err := l.mysql.CommitTickers(ctx, cd.mysqlTickers)
						if err != nil {
							if !errors.Is(err, ctx.Err()) {
								logErrStack(err)
							}
							return err
						}
						cd.mysqlTickersCount = 0
						cd.mysqlTickers = nil
					}
				}
				if val.esStr {
					cd.esTickersCount++
					cd.esTickers = append(cd.esTickers, ticker)
					if cd.esTickersCount == l.connCfg.ES.TickerCommitBuf {
						err := l.es.CommitTickers(ctx, cd.esTickers)
						if err != nil {
							if !errors.Is(err, ctx.Err()) {
								logErrStack(err)
							}
							return err
						}
						cd.esTickersCount = 0
						cd.esTickers = nil
					}
				}
				if val.influxStr {
					if influxTickerTime == 0 || influxTickerTime == 999999 {
						influxTickerTime = 1
					} else {
						influxTickerTime++
					}
					ticker.InfluxVal = influxTickerTime

					cd.influxTickersCount++
					cd.influxTickers = append(cd.influxTickers, ticker)
					if cd.influxTickersCount == l.connCfg.InfluxDB.TickerCommitBuf {
						err := l.influx.CommitTickers(ctx, cd.influxTickers)
						if err != nil {
							if !errors.Is(err, ctx.Err()) {
								logErrStack(err)
							}
							return err
						}
						cd.influxTickersCount = 0
						cd.influxTickers = nil
					}
				}
				if val.natsStr {
					cd.natsTickersCount++
					cd.natsTickers = append(cd.natsTickers, ticker)
					if cd.natsTickersCount == l.connCfg.NATS.TickerCommitBuf {
						err := l.nats.CommitTickers(ctx, cd.natsTickers)
						if err != nil {
							return err
						}
						cd.natsTickersCount = 0
						cd.natsTickers = nil
					}
				}
				if val.clickHouseStr {
					cd.clickHouseTickersCount++
					cd.clickHouseTickers = append(cd.clickHouseTickers, ticker)
					if cd.clickHouseTickersCount == l.connCfg.ClickHouse.TickerCommitBuf {
						err := l.clickhouse.CommitTickers(ctx, cd.clickHouseTickers)
						if err != nil {
							return err
						}
						cd.clickHouseTickersCount = 0
						cd.clickHouseTickers = nil
					}
				}
				if val.s3Str {
					cd.s3TickersCount++
					cd.s3Tickers = append(cd.s3Tickers, ticker)
					if cd.s3TickersCount == l.connCfg.S3.TickerCommitBuf {
						err := l.s3.CommitTickers(ctx, cd.s3Tickers)
						if err != nil {
							return err
						}
						cd.s3TickersCount = 0
						cd.s3Tickers = nil
					}
				}
			case "trade":
				req.URL.RawQuery = q.Encode()
				resp, err := l.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := restRespTradeLBank{}
				if err := jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				for i := range rr.Data {
					r := rr.Data[i]

					// Time sent is in milliseconds.
					timestamp := time.Unix(0, r.RESTTime*int64(time.Millisecond)).UTC()

					trade := storage.Trade{
						Exchange:      "lbank",
						MktID:         mktID,
						MktCommitName: mktCommitName,
						TradeID:       r.TradeID,
						Side:          r.Direction,
						Size:          r.Amount,
						Price:         r.TradePrice,
						Timestamp:     timestamp,
					}

					key := cfgLookupKey{market: trade.MktID, channel: "trade"}
					val := l.cfgMap[key]
					if val.terStr {
						cd.terTradesCount++
						cd.terTrades = append(cd.terTrades, trade)
						if cd.terTradesCount == l.connCfg.Terminal.TradeCommitBuf {
							err := l.ter.CommitTrades(ctx, cd.terTrades)
							if err != nil {
								if !errors.Is(err, ctx.Err()) {
									logErrStack(err)
								}
								return err
							}
							cd.terTradesCount = 0
							cd.terTrades = nil
						}
					}
					if val.mysqlStr {
						cd.mysqlTradesCount++
						cd.mysqlTrades = append(cd.mysqlTrades, trade)
						if cd.mysqlTradesCount == l.connCfg.MySQL.TradeCommitBuf {
							err := l.mysql.CommitTrades(ctx, cd.mysqlTrades)
							if err != nil {
								if !errors.Is(err, ctx.Err()) {
									logErrStack(err)
								}
								return err
							}
							cd.mysqlTradesCount = 0
							cd.mysqlTrades = nil
						}
					}
					if val.esStr {
						cd.esTradesCount++
						cd.esTrades = append(cd.esTrades, trade)
						if cd.esTradesCount == l.connCfg.ES.TradeCommitBuf {
							err := l.es.CommitTrades(ctx, cd.esTrades)
							if err != nil {
								if !errors.Is(err, ctx.Err()) {
									logErrStack(err)
								}
								return err
							}
							cd.esTradesCount = 0
							cd.esTrades = nil
						}
					}
					if val.influxStr {
						if influxTradeTime == 0 || influxTradeTime == 999999 {
							influxTradeTime = 1
						} else {
							influxTradeTime++
						}
						trade.InfluxVal = influxTradeTime

						cd.influxTradesCount++
						cd.influxTrades = append(cd.influxTrades, trade)
						if cd.influxTradesCount == l.connCfg.InfluxDB.TradeCommitBuf {
							err := l.influx.CommitTrades(ctx, cd.influxTrades)
							if err != nil {
								if !errors.Is(err, ctx.Err()) {
									logErrStack(err)
								}
								return err
							}
							cd.influxTradesCount = 0
							cd.influxTrades = nil
						}
					}
					if val.natsStr {
						cd.natsTradesCount++
						cd.natsTrades = append(cd.natsTrades, trade)
						if cd.natsTradesCount == l.connCfg.NATS.TradeCommitBuf {
							err := l.nats.CommitTrades(ctx, cd.natsTrades)
							if err != nil {
								return err
							}
							cd.natsTradesCount = 0
							cd.natsTrades = nil
						}
					}
					if val.clickHouseStr {
						cd.clickHouseTradesCount++
						cd.clickHouseTrades = append(cd.clickHouseTrades, trade)
						if cd.clickHouseTradesCount == l.connCfg.ClickHouse.TradeCommitBuf {
							err := l.clickhouse.CommitTrades(ctx, cd.clickHouseTrades)
							if err != nil {
								if !errors.Is(err, ctx.Err()) {
									logErrStack(err)
								}
								return err
							}
							cd.clickHouseTradesCount = 0
							cd.clickHouseTrades = nil
						}
					}
					if val.s3Str {
						cd.s3TradesCount++
						cd.s3Trades = append(cd.s3Trades, trade)
						if cd.s3TradesCount == l.connCfg.S3.TradeCommitBuf {
							err := l.s3.CommitTrades(ctx, cd.s3Trades)
							if err != nil {
								if !errors.Is(err, ctx.Err()) {
									logErrStack(err)
								}
								return err
							}
							cd.s3TradesCount = 0
							cd.s3Trades = nil
						}
					}
				}
			}

		// Return, if there is any error from another function or exchange.
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
