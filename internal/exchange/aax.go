package exchange

import (
	"context"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/milkywaybrain/cryptogalaxy/internal/config"
	"github.com/milkywaybrain/cryptogalaxy/internal/connector"
	"github.com/milkywaybrain/cryptogalaxy/internal/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

type aax struct {
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

type respAAX struct {
	Event         string `json:"e"`
	TradeID       string `json:"i"`
	Size          string `json:"q"`
	TickerPrice   string `json:"c"`
	TradePrice    string `json:"p"`
	Timestamp     int64  `json:"t"`
	mktID         string
	mktCommitName string
}

type restTradeRespAAX struct {
	Event  string    `json:"e"`
	Trades []respAAX `json:"trades"`
}

func NewAAX(appCtx context.Context, markets []config.Market, connCfg *config.Connection) error {

	// If any exchange function fails, force all the other functions to stop and return.
	aaxErrGroup, ctx := errgroup.WithContext(appCtx)

	a := aax{connCfg: connCfg}

	err := a.cfgLookup(markets)
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

					err = a.connectWs(ctx)
					if err != nil {
						return err
					}

					aaxErrGroup.Go(func() error {
						return a.closeWsConnOnError(ctx)
					})

					aaxErrGroup.Go(func() error {
						return a.readWs(ctx)
					})

					if a.ter != nil {
						aaxErrGroup.Go(func() error {
							return a.wsTickersToTerminal(ctx)
						})
						aaxErrGroup.Go(func() error {
							return a.wsTradesToTerminal(ctx)
						})
					}

					if a.mysql != nil {
						aaxErrGroup.Go(func() error {
							return a.wsTickersToMySQL(ctx)
						})
						aaxErrGroup.Go(func() error {
							return a.wsTradesToMySQL(ctx)
						})
					}

					if a.es != nil {
						aaxErrGroup.Go(func() error {
							return a.wsTickersToES(ctx)
						})
						aaxErrGroup.Go(func() error {
							return a.wsTradesToES(ctx)
						})
					}

					if a.influx != nil {
						aaxErrGroup.Go(func() error {
							return a.wsTickersToInflux(ctx)
						})
						aaxErrGroup.Go(func() error {
							return a.wsTradesToInflux(ctx)
						})
					}

					if a.nats != nil {
						aaxErrGroup.Go(func() error {
							return a.wsTickersToNats(ctx)
						})
						aaxErrGroup.Go(func() error {
							return a.wsTradesToNats(ctx)
						})
					}

					if a.clickhouse != nil {
						aaxErrGroup.Go(func() error {
							return a.wsTickersToClickHouse(ctx)
						})
						aaxErrGroup.Go(func() error {
							return a.wsTradesToClickHouse(ctx)
						})
					}

					if a.s3 != nil {
						aaxErrGroup.Go(func() error {
							return a.wsTickersToS3(ctx)
						})
						aaxErrGroup.Go(func() error {
							return a.wsTradesToS3(ctx)
						})
					}
				}

				err = a.subWsChannel(market.ID, info.Channel)
				if err != nil {
					return err
				}

				wsCount++
			case "rest":
				if restCount == 0 {
					err = a.connectRest()
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
				aaxErrGroup.Go(func() error {
					return a.processREST(ctx, mktID, mktCommitName, channel, restPingIntSec)
				})

				restCount++
			}
		}
	}

	err = aaxErrGroup.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (a *aax) cfgLookup(markets []config.Market) error {

	// Configurations flat map is prepared for easy lookup later in the app.
	a.cfgMap = make(map[cfgLookupKey]cfgLookupVal)
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
					if a.ter == nil {
						a.ter = storage.GetTerminal()
						a.wsTerTickers = make(chan []storage.Ticker, 1)
						a.wsTerTrades = make(chan []storage.Trade, 1)
					}
				case "mysql":
					val.mysqlStr = true
					if a.mysql == nil {
						a.mysql = storage.GetMySQL()
						a.wsMysqlTickers = make(chan []storage.Ticker, 1)
						a.wsMysqlTrades = make(chan []storage.Trade, 1)
					}
				case "elastic_search":
					val.esStr = true
					if a.es == nil {
						a.es = storage.GetElasticSearch()
						a.wsEsTickers = make(chan []storage.Ticker, 1)
						a.wsEsTrades = make(chan []storage.Trade, 1)
					}
				case "influxdb":
					val.influxStr = true
					if a.influx == nil {
						a.influx = storage.GetInfluxDB()
						a.wsInfluxTickers = make(chan []storage.Ticker, 1)
						a.wsInfluxTrades = make(chan []storage.Trade, 1)
					}
				case "nats":
					val.natsStr = true
					if a.nats == nil {
						a.nats = storage.GetNATS()
						a.wsNatsTickers = make(chan []storage.Ticker, 1)
						a.wsNatsTrades = make(chan []storage.Trade, 1)
					}
				case "clickhouse":
					val.clickHouseStr = true
					if a.clickhouse == nil {
						a.clickhouse = storage.GetClickHouse()
						a.wsClickHouseTickers = make(chan []storage.Ticker, 1)
						a.wsClickHouseTrades = make(chan []storage.Trade, 1)
					}
				case "s3":
					val.s3Str = true
					if a.s3 == nil {
						a.s3 = storage.GetS3()
						a.wsS3Tickers = make(chan []storage.Ticker, 1)
						a.wsS3Trades = make(chan []storage.Trade, 1)
					}
				}
			}
			val.mktCommitName = marketCommitName
			a.cfgMap[key] = val
		}
	}
	return nil
}

func (a *aax) connectWs(ctx context.Context) error {
	ws, err := connector.NewWebsocket(ctx, &a.connCfg.WS, config.AAXWebsocketURL)
	if err != nil {
		if !errors.Is(err, ctx.Err()) {
			logErrStack(err)
		}
		return err
	}
	a.ws = ws
	log.Info().Str("exchange", "aax").Msg("websocket connected")
	return nil
}

// closeWsConnOnError closes websocket connection if there is any error in app context.
// This will unblock all read and writes on websocket.
func (a *aax) closeWsConnOnError(ctx context.Context) error {
	<-ctx.Done()
	err := a.ws.Conn.Close()
	if err != nil {
		return err
	}
	return ctx.Err()
}

// subWsChannel sends channel subscription requests to the websocket server.
func (a *aax) subWsChannel(market string, channel string) error {
	if channel == "ticker" {
		channel = "1m_candles"
	}
	frame, err := jsoniter.Marshal(map[string]string{
		"e":      "subscribe",
		"stream": market + "@" + channel,
	})
	if err != nil {
		logErrStack(err)
		return err
	}
	err = a.ws.Write(frame)
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
func (a *aax) readWs(ctx context.Context) error {

	// To avoid data race, creating a new local lookup map.
	cfgLookup := make(map[cfgLookupKey]cfgLookupVal, len(a.cfgMap))
	for k, v := range a.cfgMap {
		cfgLookup[k] = v
	}

	// See influxTimeVal struct doc for details.
	itv := influxTimeVal{}
	if a.influx != nil {
		itv.TickerMap = make(map[string]int64)
		itv.TradeMap = make(map[string]int64)
	}

	cd := commitData{
		terTickers:        make([]storage.Ticker, 0, a.connCfg.Terminal.TickerCommitBuf),
		terTrades:         make([]storage.Trade, 0, a.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers:      make([]storage.Ticker, 0, a.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:       make([]storage.Trade, 0, a.connCfg.MySQL.TradeCommitBuf),
		esTickers:         make([]storage.Ticker, 0, a.connCfg.ES.TickerCommitBuf),
		esTrades:          make([]storage.Trade, 0, a.connCfg.ES.TradeCommitBuf),
		influxTickers:     make([]storage.Ticker, 0, a.connCfg.InfluxDB.TickerCommitBuf),
		influxTrades:      make([]storage.Trade, 0, a.connCfg.InfluxDB.TradeCommitBuf),
		natsTickers:       make([]storage.Ticker, 0, a.connCfg.NATS.TickerCommitBuf),
		natsTrades:        make([]storage.Trade, 0, a.connCfg.NATS.TradeCommitBuf),
		clickHouseTickers: make([]storage.Ticker, 0, a.connCfg.ClickHouse.TickerCommitBuf),
		clickHouseTrades:  make([]storage.Trade, 0, a.connCfg.ClickHouse.TradeCommitBuf),
		s3Tickers:         make([]storage.Ticker, 0, a.connCfg.S3.TickerCommitBuf),
		s3Trades:          make([]storage.Trade, 0, a.connCfg.S3.TradeCommitBuf),
	}

	log.Debug().Str("exchange", "aax").Str("func", "readWs").Msg("unlike other exchanges aax does not send channel subscribed success message in a proper format")

	for {
		select {
		default:
			frame, err := a.ws.Read()
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

			wr := respAAX{}
			err = jsoniter.Unmarshal(frame, &wr)
			if err != nil {
				logErrStack(err)
				return err
			}

			switch wr.Event {
			case "empty", "system", "error", "reply":
				continue
			default:
				s := strings.Split(wr.Event, "@")
				wr.mktID = s[0]
				if s[1] == "1m_candles" {
					wr.Event = "ticker"
				} else {
					wr.Event = "trade"
				}
			}

			// Consider frame only in configured interval, otherwise ignore it.
			switch wr.Event {
			case "ticker", "trade":
				key := cfgLookupKey{market: wr.mktID, channel: wr.Event}
				val := cfgLookup[key]
				if val.wsConsiderIntSec == 0 || time.Since(val.wsLastUpdated).Seconds() >= float64(val.wsConsiderIntSec) {
					val.wsLastUpdated = time.Now()
					wr.mktCommitName = val.mktCommitName
					cfgLookup[key] = val
				} else {
					continue
				}

				err := a.processWs(ctx, &wr, &cd, &itv)
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
func (a *aax) processWs(ctx context.Context, wr *respAAX, cd *commitData, itv *influxTimeVal) error {
	switch wr.Event {
	case "ticker":
		ticker := storage.Ticker{}
		ticker.Exchange = "aax"
		ticker.MktID = wr.mktID
		ticker.MktCommitName = wr.mktCommitName

		price, err := strconv.ParseFloat(wr.TickerPrice, 64)
		if err != nil {
			logErrStack(err)
			return err
		}
		ticker.Price = price

		ticker.Timestamp = time.Now().UTC()

		key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
		val := a.cfgMap[key]
		if val.terStr {
			cd.terTickersCount++
			cd.terTickers = append(cd.terTickers, ticker)
			if cd.terTickersCount == a.connCfg.Terminal.TickerCommitBuf {
				select {
				case a.wsTerTickers <- cd.terTickers:
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
			if cd.mysqlTickersCount == a.connCfg.MySQL.TickerCommitBuf {
				select {
				case a.wsMysqlTickers <- cd.mysqlTickers:
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
			if cd.esTickersCount == a.connCfg.ES.TickerCommitBuf {
				select {
				case a.wsEsTickers <- cd.esTickers:
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
			if cd.influxTickersCount == a.connCfg.InfluxDB.TickerCommitBuf {
				select {
				case a.wsInfluxTickers <- cd.influxTickers:
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
			if cd.natsTickersCount == a.connCfg.NATS.TickerCommitBuf {
				select {
				case a.wsNatsTickers <- cd.natsTickers:
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
			if cd.clickHouseTickersCount == a.connCfg.ClickHouse.TickerCommitBuf {
				select {
				case a.wsClickHouseTickers <- cd.clickHouseTickers:
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
			if cd.s3TickersCount == a.connCfg.S3.TickerCommitBuf {
				select {
				case a.wsS3Tickers <- cd.s3Tickers:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.s3TickersCount = 0
				cd.s3Tickers = nil
			}
		}
	case "trade":
		trade := storage.Trade{}
		trade.Exchange = "aax"
		trade.MktID = wr.mktID
		trade.MktCommitName = wr.mktCommitName
		trade.TradeID = wr.TradeID

		size, err := strconv.ParseFloat(wr.Size, 64)
		if err != nil {
			logErrStack(err)
			return err
		}
		trade.Size = size

		price, err := strconv.ParseFloat(wr.TradePrice, 64)
		if err != nil {
			logErrStack(err)
			return err
		}
		if price >= 0 {
			trade.Side = "buy"
		} else {
			trade.Side = "sell"
		}
		trade.Price = math.Abs(price)

		// Time sent is in milliseconds.
		trade.Timestamp = time.Unix(0, wr.Timestamp*int64(time.Millisecond)).UTC()

		key := cfgLookupKey{market: trade.MktID, channel: "trade"}
		val := a.cfgMap[key]
		if val.terStr {
			cd.terTradesCount++
			cd.terTrades = append(cd.terTrades, trade)
			if cd.terTradesCount == a.connCfg.Terminal.TradeCommitBuf {
				select {
				case a.wsTerTrades <- cd.terTrades:
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
			if cd.mysqlTradesCount == a.connCfg.MySQL.TradeCommitBuf {
				select {
				case a.wsMysqlTrades <- cd.mysqlTrades:
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
			if cd.esTradesCount == a.connCfg.ES.TradeCommitBuf {
				select {
				case a.wsEsTrades <- cd.esTrades:
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
			if cd.influxTradesCount == a.connCfg.InfluxDB.TradeCommitBuf {
				select {
				case a.wsInfluxTrades <- cd.influxTrades:
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
			if cd.natsTradesCount == a.connCfg.NATS.TradeCommitBuf {
				select {
				case a.wsNatsTrades <- cd.natsTrades:
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
			if cd.clickHouseTradesCount == a.connCfg.ClickHouse.TradeCommitBuf {
				select {
				case a.wsClickHouseTrades <- cd.clickHouseTrades:
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
			if cd.s3TradesCount == a.connCfg.S3.TradeCommitBuf {
				select {
				case a.wsS3Trades <- cd.s3Trades:
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

func (a *aax) wsTickersToTerminal(ctx context.Context) error {
	for {
		select {
		case data := <-a.wsTerTickers:
			a.ter.CommitTickers(data)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (a *aax) wsTradesToTerminal(ctx context.Context) error {
	for {
		select {
		case data := <-a.wsTerTrades:
			a.ter.CommitTrades(data)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (a *aax) wsTickersToMySQL(ctx context.Context) error {
	for {
		select {
		case data := <-a.wsMysqlTickers:
			err := a.mysql.CommitTickers(ctx, data)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					logErrStack(err)
				}
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (a *aax) wsTradesToMySQL(ctx context.Context) error {
	for {
		select {
		case data := <-a.wsMysqlTrades:
			err := a.mysql.CommitTrades(ctx, data)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					logErrStack(err)
				}
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (a *aax) wsTickersToES(ctx context.Context) error {
	for {
		select {
		case data := <-a.wsEsTickers:
			err := a.es.CommitTickers(ctx, data)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					logErrStack(err)
				}
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (a *aax) wsTradesToES(ctx context.Context) error {
	for {
		select {
		case data := <-a.wsEsTrades:
			err := a.es.CommitTrades(ctx, data)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					logErrStack(err)
				}
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (a *aax) wsTickersToInflux(ctx context.Context) error {
	for {
		select {
		case data := <-a.wsInfluxTickers:
			err := a.influx.CommitTickers(ctx, data)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					logErrStack(err)
				}
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (a *aax) wsTradesToInflux(ctx context.Context) error {
	for {
		select {
		case data := <-a.wsInfluxTrades:
			err := a.influx.CommitTrades(ctx, data)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					logErrStack(err)
				}
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (a *aax) wsTickersToNats(ctx context.Context) error {
	for {
		select {
		case data := <-a.wsNatsTickers:
			err := a.nats.CommitTickers(data)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (a *aax) wsTradesToNats(ctx context.Context) error {
	for {
		select {
		case data := <-a.wsNatsTrades:
			err := a.nats.CommitTrades(data)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (a *aax) wsTickersToClickHouse(ctx context.Context) error {
	for {
		select {
		case data := <-a.wsClickHouseTickers:
			err := a.clickhouse.CommitTickers(ctx, data)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					logErrStack(err)
				}
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (a *aax) wsTradesToClickHouse(ctx context.Context) error {
	for {
		select {
		case data := <-a.wsClickHouseTrades:
			err := a.clickhouse.CommitTrades(ctx, data)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					logErrStack(err)
				}
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (a *aax) wsTickersToS3(ctx context.Context) error {
	for {
		select {
		case data := <-a.wsS3Tickers:
			err := a.s3.CommitTickers(ctx, data)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					logErrStack(err)
				}
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (a *aax) wsTradesToS3(ctx context.Context) error {
	for {
		select {
		case data := <-a.wsS3Trades:
			err := a.s3.CommitTrades(ctx, data)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					logErrStack(err)
				}
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (a *aax) connectRest() error {
	rest, err := connector.GetREST()
	if err != nil {
		logErrStack(err)
		return err
	}
	a.rest = rest
	log.Info().Str("exchange", "aax").Msg("REST connection setup is done")
	return nil
}

// processREST queries exchange for ticker / trade data through REST API in configured intervals,
// transforms it to a common ticker / trade store format,
// buffers the same in memory and
// then sends it to different storage systems for commit through go channels.
func (a *aax) processREST(ctx context.Context, mktID string, mktCommitName string, channel string, interval int) error {
	var (
		req  *http.Request
		q    url.Values
		err  error
		side string

		// See influxTimeVal (exchange.go) struct doc for details.
		influxTickerTime int64
		influxTradeTime  int64
	)

	cd := commitData{
		terTickers:        make([]storage.Ticker, 0, a.connCfg.Terminal.TickerCommitBuf),
		terTrades:         make([]storage.Trade, 0, a.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers:      make([]storage.Ticker, 0, a.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:       make([]storage.Trade, 0, a.connCfg.MySQL.TradeCommitBuf),
		esTickers:         make([]storage.Ticker, 0, a.connCfg.ES.TickerCommitBuf),
		esTrades:          make([]storage.Trade, 0, a.connCfg.ES.TradeCommitBuf),
		influxTickers:     make([]storage.Ticker, 0, a.connCfg.InfluxDB.TickerCommitBuf),
		influxTrades:      make([]storage.Trade, 0, a.connCfg.InfluxDB.TradeCommitBuf),
		natsTickers:       make([]storage.Ticker, 0, a.connCfg.NATS.TickerCommitBuf),
		natsTrades:        make([]storage.Trade, 0, a.connCfg.NATS.TradeCommitBuf),
		clickHouseTickers: make([]storage.Ticker, 0, a.connCfg.ClickHouse.TickerCommitBuf),
		clickHouseTrades:  make([]storage.Trade, 0, a.connCfg.ClickHouse.TradeCommitBuf),
		s3Tickers:         make([]storage.Ticker, 0, a.connCfg.S3.TickerCommitBuf),
		s3Trades:          make([]storage.Trade, 0, a.connCfg.S3.TradeCommitBuf),
	}

	switch channel {
	case "ticker":
		req, err = a.rest.Request(ctx, "GET", config.AAXRESTBaseURL+"market/candles")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)
		q.Add("timeFrame", "1m")
	case "trade":
		req, err = a.rest.Request(ctx, "GET", config.AAXRESTBaseURL+"market/trades")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)

		// Querying for 100 trades.
		// If the configured interval gap is big, then maybe it will not return all the trades
		// and if the gap is too small, maybe it will return duplicate ones.
		// Cursor pagination is not implemented.
		// Better to use websocket.
		q.Add("limit", strconv.Itoa(100))
	}

	tick := time.NewTicker(time.Duration(interval) * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:

			switch channel {
			case "ticker":
				req.URL.RawQuery = q.Encode()
				resp, err := a.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := respAAX{}
				if err = jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				if rr.Event == "empty" || rr.Event == "system" || rr.Event == "error" {
					continue
				}

				price, err := strconv.ParseFloat(rr.TickerPrice, 64)
				if err != nil {
					logErrStack(err)
					return err
				}

				ticker := storage.Ticker{
					Exchange:      "aax",
					MktID:         mktID,
					MktCommitName: mktCommitName,
					Price:         price,
					Timestamp:     time.Now().UTC(),
				}

				key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
				val := a.cfgMap[key]
				if val.terStr {
					cd.terTickersCount++
					cd.terTickers = append(cd.terTickers, ticker)
					if cd.terTickersCount == a.connCfg.Terminal.TickerCommitBuf {
						a.ter.CommitTickers(cd.terTickers)
						cd.terTickersCount = 0
						cd.terTickers = nil
					}
				}
				if val.mysqlStr {
					cd.mysqlTickersCount++
					cd.mysqlTickers = append(cd.mysqlTickers, ticker)
					if cd.mysqlTickersCount == a.connCfg.MySQL.TickerCommitBuf {
						err := a.mysql.CommitTickers(ctx, cd.mysqlTickers)
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
					if cd.esTickersCount == a.connCfg.ES.TickerCommitBuf {
						err := a.es.CommitTickers(ctx, cd.esTickers)
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
					if cd.influxTickersCount == a.connCfg.InfluxDB.TickerCommitBuf {
						err := a.influx.CommitTickers(ctx, cd.influxTickers)
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
					if cd.natsTickersCount == a.connCfg.NATS.TickerCommitBuf {
						err := a.nats.CommitTickers(cd.natsTickers)
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
					if cd.clickHouseTickersCount == a.connCfg.ClickHouse.TickerCommitBuf {
						err := a.clickhouse.CommitTickers(ctx, cd.clickHouseTickers)
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
					if cd.s3TickersCount == a.connCfg.S3.TickerCommitBuf {
						err := a.s3.CommitTickers(ctx, cd.s3Tickers)
						if err != nil {
							return err
						}
						cd.s3TickersCount = 0
						cd.s3Tickers = nil
					}
				}
			case "trade":
				req.URL.RawQuery = q.Encode()
				resp, err := a.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := restTradeRespAAX{}
				if err := jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				if rr.Event == "empty" || rr.Event == "system" || rr.Event == "error" {
					continue
				}

				for i := range rr.Trades {
					r := rr.Trades[i]

					size, err := strconv.ParseFloat(r.Size, 64)
					if err != nil {
						logErrStack(err)
						return err
					}

					price, err := strconv.ParseFloat(r.TradePrice, 64)
					if err != nil {
						logErrStack(err)
						return err
					}
					if price >= 0 {
						side = "buy"
					} else {
						side = "sell"
					}

					// Time sent is in milliseconds.
					timestamp := time.Unix(0, r.Timestamp*int64(time.Millisecond)).UTC()

					trade := storage.Trade{
						Exchange:      "aax",
						MktID:         mktID,
						MktCommitName: mktCommitName,
						TradeID:       r.TradeID,
						Side:          side,
						Size:          size,
						Price:         math.Abs(price),
						Timestamp:     timestamp,
					}

					key := cfgLookupKey{market: trade.MktID, channel: "trade"}
					val := a.cfgMap[key]
					if val.terStr {
						cd.terTradesCount++
						cd.terTrades = append(cd.terTrades, trade)
						if cd.terTradesCount == a.connCfg.Terminal.TradeCommitBuf {
							a.ter.CommitTrades(cd.terTrades)
							cd.terTradesCount = 0
							cd.terTrades = nil
						}
					}
					if val.mysqlStr {
						cd.mysqlTradesCount++
						cd.mysqlTrades = append(cd.mysqlTrades, trade)
						if cd.mysqlTradesCount == a.connCfg.MySQL.TradeCommitBuf {
							err := a.mysql.CommitTrades(ctx, cd.mysqlTrades)
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
						if cd.esTradesCount == a.connCfg.ES.TradeCommitBuf {
							err := a.es.CommitTrades(ctx, cd.esTrades)
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
						if cd.influxTradesCount == a.connCfg.InfluxDB.TradeCommitBuf {
							err := a.influx.CommitTrades(ctx, cd.influxTrades)
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
						if cd.natsTradesCount == a.connCfg.NATS.TradeCommitBuf {
							err := a.nats.CommitTrades(cd.natsTrades)
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
						if cd.clickHouseTradesCount == a.connCfg.ClickHouse.TradeCommitBuf {
							err := a.clickhouse.CommitTrades(ctx, cd.clickHouseTrades)
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
						if cd.s3TradesCount == a.connCfg.S3.TradeCommitBuf {
							err := a.s3.CommitTrades(ctx, cd.s3Trades)
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
