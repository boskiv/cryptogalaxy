package exchange

import (
	"context"
	"fmt"
	"io"
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

// StartBTSE is for starting btse exchange functions.
func StartBTSE(appCtx context.Context, markets []config.Market, retry *config.Retry, connCfg *config.Connection) error {

	// If any error occurs or connection is lost, retry the exchange functions with a time gap till it reaches
	// a configured number of retry.
	// Retry counter will be reset back to zero if the elapsed time since the last retry is greater than the configured one.
	var retryCount int
	lastRetryTime := time.Now()

	for {
		err := newBTSE(appCtx, markets, connCfg)
		if err != nil {
			log.Error().Err(err).Str("exchange", "btse").Msg("error occurred")
			if retry.Number == 0 {
				return errors.New("not able to connect btse exchange. please check the log for details")
			}
			if retry.ResetSec == 0 || time.Since(lastRetryTime).Seconds() < float64(retry.ResetSec) {
				retryCount++
			} else {
				retryCount = 1
			}
			lastRetryTime = time.Now()
			if retryCount > retry.Number {
				err = fmt.Errorf("not able to connect btse exchange even after %d retry", retry.Number)
				log.Error().Err(err).Str("exchange", "btse").Msg("")
				return err
			}

			log.Error().Str("exchange", "btse").Int("retry", retryCount).Msg(fmt.Sprintf("retrying functions in %d seconds", retry.GapSec))
			tick := time.NewTicker(time.Duration(retry.GapSec) * time.Second)
			select {
			case <-tick.C:
				tick.Stop()

			// Return, if there is any error from another exchange.
			case <-appCtx.Done():
				log.Error().Str("exchange", "btse").Msg("ctx canceled, return from StartBTSE")
				return appCtx.Err()
			}
		}
	}
}

type btse struct {
	ws                  connector.Websocket
	rest                *connector.REST
	connCfg             *config.Connection
	cfgMap              map[cfgLookupKey]cfgLookupVal
	channelIds          map[int][2]string
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

type wsSubBTSE struct {
	Op   string    `json:"op"`
	Args [1]string `json:"args"`
}

type wsRespBTSE struct {
	Event         string         `json:"event"`
	Channel       [1]string      `json:"channel"`
	Topic         string         `json:"topic"`
	Data          []respDataBTSE `json:"data"`
	channelType   string
	mktID         string
	mktCommitName string
}

type respDataBTSE struct {
	RESTTradeID uint64  `json:"serialId"`
	WSTradeID   uint64  `json:"tradeId"`
	Side        string  `json:"side"`
	Size        float64 `json:"size"`
	TickerPrice float64 `json:"lastPrice"`
	TradePrice  float64 `json:"price"`
	Timestamp   int64   `json:"timestamp"`
}

func newBTSE(appCtx context.Context, markets []config.Market, connCfg *config.Connection) error {

	// If any exchange function fails, force all the other functions to stop and return.
	btseErrGroup, ctx := errgroup.WithContext(appCtx)

	b := btse{connCfg: connCfg}

	err := b.cfgLookup(markets)
	if err != nil {
		return err
	}

	var (
		wsCount   int
		restCount int
	)
	subChannels := make(map[string]bool)

	for _, market := range markets {
		for _, info := range market.Info {
			switch info.Connector {
			case "websocket":
				if wsCount == 0 {

					err = b.connectWs(ctx)
					if err != nil {
						return err
					}

					btseErrGroup.Go(func() error {
						return b.closeWsConnOnError(ctx)
					})

					btseErrGroup.Go(func() error {
						return b.readWs(ctx)
					})

					if b.ter != nil {
						btseErrGroup.Go(func() error {
							return b.wsTickersToTerminal(ctx)
						})
						btseErrGroup.Go(func() error {
							return b.wsTradesToTerminal(ctx)
						})
					}

					if b.mysql != nil {
						btseErrGroup.Go(func() error {
							return b.wsTickersToMySQL(ctx)
						})
						btseErrGroup.Go(func() error {
							return b.wsTradesToMySQL(ctx)
						})
					}

					if b.es != nil {
						btseErrGroup.Go(func() error {
							return b.wsTickersToES(ctx)
						})
						btseErrGroup.Go(func() error {
							return b.wsTradesToES(ctx)
						})
					}

					if b.influx != nil {
						btseErrGroup.Go(func() error {
							return b.wsTickersToInflux(ctx)
						})
						btseErrGroup.Go(func() error {
							return b.wsTradesToInflux(ctx)
						})
					}

					if b.nats != nil {
						btseErrGroup.Go(func() error {
							return b.wsTickersToNats(ctx)
						})
						btseErrGroup.Go(func() error {
							return b.wsTradesToNats(ctx)
						})
					}

					if b.clickhouse != nil {
						btseErrGroup.Go(func() error {
							return b.wsTickersToClickHouse(ctx)
						})
						btseErrGroup.Go(func() error {
							return b.wsTradesToClickHouse(ctx)
						})
					}

					if b.s3 != nil {
						btseErrGroup.Go(func() error {
							return b.wsTickersToS3(ctx)
						})
						btseErrGroup.Go(func() error {
							return b.wsTradesToS3(ctx)
						})
					}
				}

				// There is only one channel provided for both ticker and trade data,
				// so need to subscribe only once.
				_, pres := subChannels[market.ID]
				if !pres {
					err = b.subWsChannel(market.ID)
					if err != nil {
						return err
					}
					subChannels[market.ID] = true
				}

				wsCount++
			case "rest":
				if restCount == 0 {
					err = b.connectRest()
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
				btseErrGroup.Go(func() error {
					return b.processREST(ctx, mktID, mktCommitName, channel, restPingIntSec)
				})

				restCount++
			}
		}
	}

	err = btseErrGroup.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (b *btse) cfgLookup(markets []config.Market) error {

	// Configurations flat map is prepared for easy lookup later in the app.
	b.cfgMap = make(map[cfgLookupKey]cfgLookupVal)
	b.channelIds = make(map[int][2]string)
	for _, market := range markets {
		var mktCommitName string
		if market.CommitName != "" {
			mktCommitName = market.CommitName
		} else {
			mktCommitName = market.ID
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
					if b.ter == nil {
						b.ter = storage.GetTerminal()
						b.wsTerTickers = make(chan []storage.Ticker, 1)
						b.wsTerTrades = make(chan []storage.Trade, 1)
					}
				case "mysql":
					val.mysqlStr = true
					if b.mysql == nil {
						b.mysql = storage.GetMySQL()
						b.wsMysqlTickers = make(chan []storage.Ticker, 1)
						b.wsMysqlTrades = make(chan []storage.Trade, 1)
					}
				case "elastic_search":
					val.esStr = true
					if b.es == nil {
						b.es = storage.GetElasticSearch()
						b.wsEsTickers = make(chan []storage.Ticker, 1)
						b.wsEsTrades = make(chan []storage.Trade, 1)
					}
				case "influxdb":
					val.influxStr = true
					if b.influx == nil {
						b.influx = storage.GetInfluxDB()
						b.wsInfluxTickers = make(chan []storage.Ticker, 1)
						b.wsInfluxTrades = make(chan []storage.Trade, 1)
					}
				case "nats":
					val.natsStr = true
					if b.nats == nil {
						b.nats = storage.GetNATS()
						b.wsNatsTickers = make(chan []storage.Ticker, 1)
						b.wsNatsTrades = make(chan []storage.Trade, 1)
					}
				case "clickhouse":
					val.clickHouseStr = true
					if b.clickhouse == nil {
						b.clickhouse = storage.GetClickHouse()
						b.wsClickHouseTickers = make(chan []storage.Ticker, 1)
						b.wsClickHouseTrades = make(chan []storage.Trade, 1)
					}
				case "s3":
					val.s3Str = true
					if b.s3 == nil {
						b.s3 = storage.GetS3()
						b.wsS3Tickers = make(chan []storage.Ticker, 1)
						b.wsS3Trades = make(chan []storage.Trade, 1)
					}
				}
			}
			val.mktCommitName = mktCommitName
			b.cfgMap[key] = val
		}
	}
	return nil
}

func (b *btse) connectWs(ctx context.Context) error {
	ws, err := connector.NewWebsocket(ctx, &b.connCfg.WS, config.BTSEWebsocketURL)
	if err != nil {
		if !errors.Is(err, ctx.Err()) {
			logErrStack(err)
		}
		return err
	}
	b.ws = ws
	log.Info().Str("exchange", "btse").Msg("websocket connected")
	return nil
}

// closeWsConnOnError closes websocket connection if there is any error in app context.
// This will unblock all read and writes on websocket.
func (b *btse) closeWsConnOnError(ctx context.Context) error {
	<-ctx.Done()
	err := b.ws.Conn.Close()
	if err != nil {
		return err
	}
	return ctx.Err()
}

// subWsChannel sends channel subscription requests to the websocket server.
func (b *btse) subWsChannel(market string) error {
	channel := "tradeHistoryApi:" + market
	sub := wsSubBTSE{
		Op:   "subscribe",
		Args: [1]string{channel},
	}
	frame, err := jsoniter.Marshal(sub)
	if err != nil {
		logErrStack(err)
		return err
	}
	err = b.ws.Write(frame)
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
func (b *btse) readWs(ctx context.Context) error {

	// To avoid data race, creating a new local lookup map.
	cfgLookup := make(map[cfgLookupKey]cfgLookupVal, len(b.cfgMap))
	for k, v := range b.cfgMap {
		cfgLookup[k] = v
	}

	// See influxTimeVal struct doc for details.
	itv := influxTimeVal{}
	if b.influx != nil {
		itv.TickerMap = make(map[string]int64)
		itv.TradeMap = make(map[string]int64)
	}

	cd := commitData{
		terTickers:        make([]storage.Ticker, 0, b.connCfg.Terminal.TickerCommitBuf),
		terTrades:         make([]storage.Trade, 0, b.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers:      make([]storage.Ticker, 0, b.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:       make([]storage.Trade, 0, b.connCfg.MySQL.TradeCommitBuf),
		esTickers:         make([]storage.Ticker, 0, b.connCfg.ES.TickerCommitBuf),
		esTrades:          make([]storage.Trade, 0, b.connCfg.ES.TradeCommitBuf),
		influxTickers:     make([]storage.Ticker, 0, b.connCfg.InfluxDB.TickerCommitBuf),
		influxTrades:      make([]storage.Trade, 0, b.connCfg.InfluxDB.TradeCommitBuf),
		natsTickers:       make([]storage.Ticker, 0, b.connCfg.NATS.TickerCommitBuf),
		natsTrades:        make([]storage.Trade, 0, b.connCfg.NATS.TradeCommitBuf),
		clickHouseTickers: make([]storage.Ticker, 0, b.connCfg.ClickHouse.TickerCommitBuf),
		clickHouseTrades:  make([]storage.Trade, 0, b.connCfg.ClickHouse.TradeCommitBuf),
		s3Tickers:         make([]storage.Ticker, 0, b.connCfg.S3.TickerCommitBuf),
		s3Trades:          make([]storage.Trade, 0, b.connCfg.S3.TradeCommitBuf),
	}

	for {
		select {
		default:
			frame, err := b.ws.Read()
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

			wr := wsRespBTSE{}
			err = jsoniter.Unmarshal(frame, &wr)
			if err != nil {
				logErrStack(err)
				return err
			}

			if wr.Event == "subscribe" {
				s := strings.Split(wr.Channel[0], ":")
				wr.mktID = s[1]

				// There is only one channel provided for both ticker and trade data.
				key := cfgLookupKey{market: wr.mktID, channel: "ticker"}
				val, pres := cfgLookup[key]
				if pres && val.connector == "websocket" {
					log.Debug().Str("exchange", "btse").Str("func", "readWs").Str("market", wr.mktID).Str("channel", "ticker").Msg("channel subscribed")
				}
				key = cfgLookupKey{market: wr.mktID, channel: "trade"}
				val, pres = cfgLookup[key]
				if pres && val.connector == "websocket" {
					log.Debug().Str("exchange", "btse").Str("func", "readWs").Str("market", wr.mktID).Str("channel", "trade").Msg("channel subscribed")
				}
				continue
			}

			// API sends duplicate data, so filtering based on the length.
			if len(wr.Data) > 1 {
				continue
			}

			// There is only one channel provided for both ticker and trade data,
			// so need to duplicate it manually if there is a subscription to both the channels by user.
			s := strings.Split(wr.Topic, ":")
			wr.mktID = s[1]

			// Ticker.
			key := cfgLookupKey{market: wr.mktID, channel: "ticker"}
			val, pres := cfgLookup[key]
			if pres && val.connector == "websocket" {
				if val.wsConsiderIntSec == 0 || time.Since(val.wsLastUpdated).Seconds() >= float64(val.wsConsiderIntSec) {

					// Consider frame only in configured interval, otherwise ignore it.
					val.wsLastUpdated = time.Now()
					wr.mktCommitName = val.mktCommitName
					wr.channelType = "ticker"
					cfgLookup[key] = val

					err := b.processWs(ctx, &wr, &cd, &itv)
					if err != nil {
						return err
					}
				}
			}

			// Trade.
			key = cfgLookupKey{market: wr.mktID, channel: "trade"}
			val, pres = cfgLookup[key]
			if pres && val.connector == "websocket" {
				if val.wsConsiderIntSec == 0 || time.Since(val.wsLastUpdated).Seconds() >= float64(val.wsConsiderIntSec) {

					// Consider frame only in configured interval, otherwise ignore it.
					val.wsLastUpdated = time.Now()
					wr.mktCommitName = val.mktCommitName
					wr.channelType = "trade"
					cfgLookup[key] = val

					err := b.processWs(ctx, &wr, &cd, &itv)
					if err != nil {
						return err
					}
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
func (b *btse) processWs(ctx context.Context, wr *wsRespBTSE, cd *commitData, itv *influxTimeVal) error {
	switch wr.channelType {
	case "ticker":
		ticker := storage.Ticker{}
		ticker.Exchange = "btse"
		ticker.MktID = wr.mktID
		ticker.MktCommitName = wr.mktCommitName
		ticker.Price = wr.Data[0].TradePrice

		// Time sent is in milliseconds.
		ticker.Timestamp = time.Unix(0, wr.Data[0].Timestamp*int64(time.Millisecond)).UTC()

		key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
		val := b.cfgMap[key]
		if val.terStr {
			cd.terTickersCount++
			cd.terTickers = append(cd.terTickers, ticker)
			if cd.terTickersCount == b.connCfg.Terminal.TickerCommitBuf {
				select {
				case b.wsTerTickers <- cd.terTickers:
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
			if cd.mysqlTickersCount == b.connCfg.MySQL.TickerCommitBuf {
				select {
				case b.wsMysqlTickers <- cd.mysqlTickers:
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
			if cd.esTickersCount == b.connCfg.ES.TickerCommitBuf {
				select {
				case b.wsEsTickers <- cd.esTickers:
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
			if cd.influxTickersCount == b.connCfg.InfluxDB.TickerCommitBuf {
				select {
				case b.wsInfluxTickers <- cd.influxTickers:
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
			if cd.natsTickersCount == b.connCfg.NATS.TickerCommitBuf {
				select {
				case b.wsNatsTickers <- cd.natsTickers:
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
			if cd.clickHouseTickersCount == b.connCfg.ClickHouse.TickerCommitBuf {
				select {
				case b.wsClickHouseTickers <- cd.clickHouseTickers:
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
			if cd.s3TickersCount == b.connCfg.S3.TickerCommitBuf {
				select {
				case b.wsS3Tickers <- cd.s3Tickers:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.s3TickersCount = 0
				cd.s3Tickers = nil
			}
		}
	case "trade":
		for _, data := range wr.Data {
			trade := storage.Trade{}
			trade.Exchange = "btse"
			trade.MktID = wr.mktID
			trade.MktCommitName = wr.mktCommitName
			trade.TradeID = strconv.FormatUint(data.WSTradeID, 10)

			if data.Side == "BUY" {
				trade.Side = "buy"
			} else {
				trade.Side = "sell"
			}

			trade.Size = data.Size
			trade.Price = data.TradePrice

			// Time sent is in milliseconds.
			trade.Timestamp = time.Unix(0, wr.Data[0].Timestamp*int64(time.Millisecond)).UTC()

			key := cfgLookupKey{market: trade.MktID, channel: "trade"}
			val := b.cfgMap[key]
			if val.terStr {
				cd.terTradesCount++
				cd.terTrades = append(cd.terTrades, trade)
				if cd.terTradesCount == b.connCfg.Terminal.TradeCommitBuf {
					select {
					case b.wsTerTrades <- cd.terTrades:
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
				if cd.mysqlTradesCount == b.connCfg.MySQL.TradeCommitBuf {
					select {
					case b.wsMysqlTrades <- cd.mysqlTrades:
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
				if cd.esTradesCount == b.connCfg.ES.TradeCommitBuf {
					select {
					case b.wsEsTrades <- cd.esTrades:
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
				if cd.influxTradesCount == b.connCfg.InfluxDB.TradeCommitBuf {
					select {
					case b.wsInfluxTrades <- cd.influxTrades:
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
				if cd.natsTradesCount == b.connCfg.NATS.TradeCommitBuf {
					select {
					case b.wsNatsTrades <- cd.natsTrades:
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
				if cd.clickHouseTradesCount == b.connCfg.ClickHouse.TradeCommitBuf {
					select {
					case b.wsClickHouseTrades <- cd.clickHouseTrades:
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
				if cd.s3TradesCount == b.connCfg.S3.TradeCommitBuf {
					select {
					case b.wsS3Trades <- cd.s3Trades:
					case <-ctx.Done():
						return ctx.Err()
					}
					cd.s3TradesCount = 0
					cd.s3Trades = nil
				}
			}
		}
	}
	return nil
}

func (b *btse) wsTickersToTerminal(ctx context.Context) error {
	for {
		select {
		case data := <-b.wsTerTickers:
			b.ter.CommitTickers(data)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (b *btse) wsTradesToTerminal(ctx context.Context) error {
	for {
		select {
		case data := <-b.wsTerTrades:
			b.ter.CommitTrades(data)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (b *btse) wsTickersToMySQL(ctx context.Context) error {
	for {
		select {
		case data := <-b.wsMysqlTickers:
			err := b.mysql.CommitTickers(ctx, data)
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

func (b *btse) wsTradesToMySQL(ctx context.Context) error {
	for {
		select {
		case data := <-b.wsMysqlTrades:
			err := b.mysql.CommitTrades(ctx, data)
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

func (b *btse) wsTickersToES(ctx context.Context) error {
	for {
		select {
		case data := <-b.wsEsTickers:
			err := b.es.CommitTickers(ctx, data)
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

func (b *btse) wsTradesToES(ctx context.Context) error {
	for {
		select {
		case data := <-b.wsEsTrades:
			err := b.es.CommitTrades(ctx, data)
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

func (b *btse) wsTickersToInflux(ctx context.Context) error {
	for {
		select {
		case data := <-b.wsInfluxTickers:
			err := b.influx.CommitTickers(ctx, data)
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

func (b *btse) wsTradesToInflux(ctx context.Context) error {
	for {
		select {
		case data := <-b.wsInfluxTrades:
			err := b.influx.CommitTrades(ctx, data)
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

func (b *btse) wsTickersToNats(ctx context.Context) error {
	for {
		select {
		case data := <-b.wsNatsTickers:
			err := b.nats.CommitTickers(data)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (b *btse) wsTradesToNats(ctx context.Context) error {
	for {
		select {
		case data := <-b.wsNatsTrades:
			err := b.nats.CommitTrades(data)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (b *btse) wsTickersToClickHouse(ctx context.Context) error {
	for {
		select {
		case data := <-b.wsClickHouseTickers:
			err := b.clickhouse.CommitTickers(ctx, data)
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

func (b *btse) wsTradesToClickHouse(ctx context.Context) error {
	for {
		select {
		case data := <-b.wsClickHouseTrades:
			err := b.clickhouse.CommitTrades(ctx, data)
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

func (b *btse) wsTickersToS3(ctx context.Context) error {
	for {
		select {
		case data := <-b.wsS3Tickers:
			err := b.s3.CommitTickers(ctx, data)
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

func (b *btse) wsTradesToS3(ctx context.Context) error {
	for {
		select {
		case data := <-b.wsS3Trades:
			err := b.s3.CommitTrades(ctx, data)
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

func (b *btse) connectRest() error {
	rest, err := connector.GetREST()
	if err != nil {
		logErrStack(err)
		return err
	}
	b.rest = rest
	log.Info().Str("exchange", "btse").Msg("REST connection setup is done")
	return nil
}

// processREST queries exchange for ticker / trade data through REST API in configured intervals,
// transforms it to a common ticker / trade store format,
// buffers the same in memory and
// then sends it to different storage systems for commit through go channels.
func (b *btse) processREST(ctx context.Context, mktID string, mktCommitName string, channel string, interval int) error {
	var (
		req *http.Request
		q   url.Values
		err error

		// See influxTimeVal (exchange.go) struct doc for details.
		influxTickerTime int64
		influxTradeTime  int64
	)

	cd := commitData{
		terTickers:        make([]storage.Ticker, 0, b.connCfg.Terminal.TickerCommitBuf),
		terTrades:         make([]storage.Trade, 0, b.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers:      make([]storage.Ticker, 0, b.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:       make([]storage.Trade, 0, b.connCfg.MySQL.TradeCommitBuf),
		esTickers:         make([]storage.Ticker, 0, b.connCfg.ES.TickerCommitBuf),
		esTrades:          make([]storage.Trade, 0, b.connCfg.ES.TradeCommitBuf),
		influxTickers:     make([]storage.Ticker, 0, b.connCfg.InfluxDB.TickerCommitBuf),
		influxTrades:      make([]storage.Trade, 0, b.connCfg.InfluxDB.TradeCommitBuf),
		natsTickers:       make([]storage.Ticker, 0, b.connCfg.NATS.TickerCommitBuf),
		natsTrades:        make([]storage.Trade, 0, b.connCfg.NATS.TradeCommitBuf),
		clickHouseTickers: make([]storage.Ticker, 0, b.connCfg.ClickHouse.TickerCommitBuf),
		clickHouseTrades:  make([]storage.Trade, 0, b.connCfg.ClickHouse.TradeCommitBuf),
		s3Tickers:         make([]storage.Ticker, 0, b.connCfg.S3.TickerCommitBuf),
		s3Trades:          make([]storage.Trade, 0, b.connCfg.S3.TradeCommitBuf),
	}

	switch channel {
	case "ticker":
		req, err = b.rest.Request(ctx, "GET", config.BTSERESTBaseURL+"price")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)
	case "trade":
		req, err = b.rest.Request(ctx, "GET", config.BTSERESTBaseURL+"trades")
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
		// Better to use websocket.
		q.Add("count", strconv.Itoa(100))
	}

	tick := time.NewTicker(time.Duration(interval) * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:

			switch channel {
			case "ticker":
				req.URL.RawQuery = q.Encode()
				resp, err := b.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := []respDataBTSE{}
				if err = jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				ticker := storage.Ticker{
					Exchange:      "btse",
					MktID:         mktID,
					MktCommitName: mktCommitName,
					Price:         rr[0].TickerPrice,
					Timestamp:     time.Now().UTC(),
				}

				key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
				val := b.cfgMap[key]
				if val.terStr {
					cd.terTickersCount++
					cd.terTickers = append(cd.terTickers, ticker)
					if cd.terTickersCount == b.connCfg.Terminal.TickerCommitBuf {
						b.ter.CommitTickers(cd.terTickers)
						cd.terTickersCount = 0
						cd.terTickers = nil
					}
				}
				if val.mysqlStr {
					cd.mysqlTickersCount++
					cd.mysqlTickers = append(cd.mysqlTickers, ticker)
					if cd.mysqlTickersCount == b.connCfg.MySQL.TickerCommitBuf {
						err := b.mysql.CommitTickers(ctx, cd.mysqlTickers)
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
					if cd.esTickersCount == b.connCfg.ES.TickerCommitBuf {
						err := b.es.CommitTickers(ctx, cd.esTickers)
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
					if cd.influxTickersCount == b.connCfg.InfluxDB.TickerCommitBuf {
						err := b.influx.CommitTickers(ctx, cd.influxTickers)
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
					if cd.natsTickersCount == b.connCfg.NATS.TickerCommitBuf {
						err := b.nats.CommitTickers(cd.natsTickers)
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
					if cd.clickHouseTickersCount == b.connCfg.ClickHouse.TickerCommitBuf {
						err := b.clickhouse.CommitTickers(ctx, cd.clickHouseTickers)
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
					if cd.s3TickersCount == b.connCfg.S3.TickerCommitBuf {
						err := b.s3.CommitTickers(ctx, cd.s3Tickers)
						if err != nil {
							return err
						}
						cd.s3TickersCount = 0
						cd.s3Tickers = nil
					}
				}
			case "trade":
				req.URL.RawQuery = q.Encode()
				resp, err := b.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := []respDataBTSE{}
				if err := jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				for i := range rr {
					r := rr[i]
					var side string
					if r.Side == "BUY" {
						side = "buy"
					} else {
						side = "sell"
					}

					// Time sent is in milliseconds.
					timestamp := time.Unix(0, r.Timestamp*int64(time.Millisecond)).UTC()

					trade := storage.Trade{
						Exchange:      "btse",
						MktID:         mktID,
						MktCommitName: mktCommitName,
						TradeID:       strconv.FormatUint(r.RESTTradeID, 10),
						Side:          side,
						Size:          r.Size,
						Price:         r.TradePrice,
						Timestamp:     timestamp,
					}

					key := cfgLookupKey{market: trade.MktID, channel: "trade"}
					val := b.cfgMap[key]
					if val.terStr {
						cd.terTradesCount++
						cd.terTrades = append(cd.terTrades, trade)
						if cd.terTradesCount == b.connCfg.Terminal.TradeCommitBuf {
							b.ter.CommitTrades(cd.terTrades)
							cd.terTradesCount = 0
							cd.terTrades = nil
						}
					}
					if val.mysqlStr {
						cd.mysqlTradesCount++
						cd.mysqlTrades = append(cd.mysqlTrades, trade)
						if cd.mysqlTradesCount == b.connCfg.MySQL.TradeCommitBuf {
							err := b.mysql.CommitTrades(ctx, cd.mysqlTrades)
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
						if cd.esTradesCount == b.connCfg.ES.TradeCommitBuf {
							err := b.es.CommitTrades(ctx, cd.esTrades)
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
						if cd.influxTradesCount == b.connCfg.InfluxDB.TradeCommitBuf {
							err := b.influx.CommitTrades(ctx, cd.influxTrades)
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
						if cd.natsTradesCount == b.connCfg.NATS.TradeCommitBuf {
							err := b.nats.CommitTrades(cd.natsTrades)
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
						if cd.clickHouseTradesCount == b.connCfg.ClickHouse.TradeCommitBuf {
							err := b.clickhouse.CommitTrades(ctx, cd.clickHouseTrades)
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
						if cd.s3TradesCount == b.connCfg.S3.TradeCommitBuf {
							err := b.s3.CommitTrades(ctx, cd.s3Trades)
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
