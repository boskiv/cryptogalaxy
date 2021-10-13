package exchange

import (
	"bytes"
	"context"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"

	jsoniter "github.com/json-iterator/go"
	"github.com/milkywaybrain/cryptogalaxy/internal/config"
	"github.com/milkywaybrain/cryptogalaxy/internal/connector"
	"github.com/milkywaybrain/cryptogalaxy/internal/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

type bitfinex struct {
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

type respBitfinex []interface{}

type wsRespInfoBitfinex struct {
	market        string
	channel       string
	respBitfinex  respBitfinex
	mktCommitName string
}

type wsEventRespBitfinex struct {
	Event     string `json:"event"`
	Channel   string `json:"channel"`
	ChannelID int    `json:"chanId"`
	Symbol    string `json:"symbol"`
	Code      int    `json:"code"`
	Msg       string `json:"msg"`
	Version   int    `json:"version"`
	Platform  struct {
		Status int `json:"status"`
	} `json:"platform"`
}

func NewBitfinex(appCtx context.Context, markets []config.Market, connCfg *config.Connection) error {

	// If any exchange function fails, force all the other functions to stop and return.
	bitfinexErrGroup, ctx := errgroup.WithContext(appCtx)

	e := bitfinex{connCfg: connCfg}

	err := e.cfgLookup(markets)
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

					err = e.connectWs(ctx)
					if err != nil {
						return err
					}

					bitfinexErrGroup.Go(func() error {
						return e.closeWsConnOnError(ctx)
					})

					bitfinexErrGroup.Go(func() error {
						return e.readWs(ctx)
					})

					if e.ter != nil {
						bitfinexErrGroup.Go(func() error {
							return storage.TickersToStorage(ctx, e.ter, e.wsTerTickers)
						})
						bitfinexErrGroup.Go(func() error {
							return storage.TradesToStorage(ctx, e.ter, e.wsTerTrades)
						})
					}

					if e.mysql != nil {
						bitfinexErrGroup.Go(func() error {
							return storage.TickersToStorage(ctx, e.mysql, e.wsMysqlTickers)
						})
						bitfinexErrGroup.Go(func() error {
							return storage.TradesToStorage(ctx, e.mysql, e.wsMysqlTrades)
						})
					}

					if e.es != nil {
						bitfinexErrGroup.Go(func() error {
							return storage.TickersToStorage(ctx, e.es, e.wsEsTickers)
						})
						bitfinexErrGroup.Go(func() error {
							return storage.TradesToStorage(ctx, e.es, e.wsEsTrades)
						})
					}

					if e.influx != nil {
						bitfinexErrGroup.Go(func() error {
							return storage.TickersToStorage(ctx, e.influx, e.wsInfluxTickers)
						})
						bitfinexErrGroup.Go(func() error {
							return storage.TradesToStorage(ctx, e.influx, e.wsInfluxTrades)
						})
					}

					if e.nats != nil {
						bitfinexErrGroup.Go(func() error {
							return storage.TickersToStorage(ctx, e.nats, e.wsNatsTickers)
						})
						bitfinexErrGroup.Go(func() error {
							return storage.TradesToStorage(ctx, e.nats, e.wsNatsTrades)
						})
					}

					if e.clickhouse != nil {
						bitfinexErrGroup.Go(func() error {
							return storage.TickersToStorage(ctx, e.clickhouse, e.wsClickHouseTickers)
						})
						bitfinexErrGroup.Go(func() error {
							return storage.TradesToStorage(ctx, e.clickhouse, e.wsClickHouseTrades)
						})
					}

					if e.s3 != nil {
						bitfinexErrGroup.Go(func() error {
							return storage.TickersToStorage(ctx, e.s3, e.wsS3Tickers)
						})
						bitfinexErrGroup.Go(func() error {
							return storage.TradesToStorage(ctx, e.s3, e.wsS3Trades)
						})
					}
				}

				err = e.subWsChannel(market.ID, info.Channel)
				if err != nil {
					return err
				}
				wsCount++
			case "rest":
				if restCount == 0 {
					client, err := connectRest("bitfinex")
					if err != nil {
						return err
					}
					e.rest = client
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
				bitfinexErrGroup.Go(func() error {
					return e.processREST(ctx, mktID, mktCommitName, channel, restPingIntSec)
				})

				restCount++
			}
		}
	}

	err = bitfinexErrGroup.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (e *bitfinex) cfgLookup(markets []config.Market) error {

	// Configurations flat map is prepared for easy lookup later in the app.
	e.cfgMap = make(map[cfgLookupKey]cfgLookupVal)
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
					if e.ter == nil {
						e.ter = storage.GetTerminal()
						e.wsTerTickers = make(chan []storage.Ticker, 1)
						e.wsTerTrades = make(chan []storage.Trade, 1)
					}
				case "mysql":
					val.mysqlStr = true
					if e.mysql == nil {
						e.mysql = storage.GetMySQL()
						e.wsMysqlTickers = make(chan []storage.Ticker, 1)
						e.wsMysqlTrades = make(chan []storage.Trade, 1)
					}
				case "elastic_search":
					val.esStr = true
					if e.es == nil {
						e.es = storage.GetElasticSearch()
						e.wsEsTickers = make(chan []storage.Ticker, 1)
						e.wsEsTrades = make(chan []storage.Trade, 1)
					}
				case "influxdb":
					val.influxStr = true
					if e.influx == nil {
						e.influx = storage.GetInfluxDB()
						e.wsInfluxTickers = make(chan []storage.Ticker, 1)
						e.wsInfluxTrades = make(chan []storage.Trade, 1)
					}
				case "nats":
					val.natsStr = true
					if e.nats == nil {
						e.nats = storage.GetNATS()
						e.wsNatsTickers = make(chan []storage.Ticker, 1)
						e.wsNatsTrades = make(chan []storage.Trade, 1)
					}
				case "clickhouse":
					val.clickHouseStr = true
					if e.clickhouse == nil {
						e.clickhouse = storage.GetClickHouse()
						e.wsClickHouseTickers = make(chan []storage.Ticker, 1)
						e.wsClickHouseTrades = make(chan []storage.Trade, 1)
					}
				case "s3":
					val.s3Str = true
					if e.s3 == nil {
						e.s3 = storage.GetS3()
						e.wsS3Tickers = make(chan []storage.Ticker, 1)
						e.wsS3Trades = make(chan []storage.Trade, 1)
					}
				}
			}
			val.mktCommitName = mktCommitName
			e.cfgMap[key] = val
		}
	}
	return nil
}

func (e *bitfinex) connectWs(ctx context.Context) error {
	ws, err := connector.NewWebsocket(ctx, &e.connCfg.WS, config.BitfinexWebsocketURL)
	if err != nil {
		if !errors.Is(err, ctx.Err()) {
			logErrStack(err)
		}
		return err
	}
	e.ws = ws
	log.Info().Str("exchange", "bitfinex").Msg("websocket connected")
	return nil
}

// closeWsConnOnError closes websocket connection if there is any error in app context.
// This will unblock all read and writes on websocket.
func (e *bitfinex) closeWsConnOnError(ctx context.Context) error {
	<-ctx.Done()
	err := e.ws.Conn.Close()
	if err != nil {
		return err
	}
	return ctx.Err()
}

// subWsChannel sends channel subscription requests to the websocket server.
func (e *bitfinex) subWsChannel(market string, channel string) error {
	if channel == "trade" {
		channel = "trades"
	}
	market = "t" + strings.ToUpper(market)
	frame, err := jsoniter.Marshal(map[string]string{
		"event":   "subscribe",
		"channel": channel,
		"symbol":  market,
	})
	if err != nil {
		logErrStack(err)
		return err
	}
	err = e.ws.Write(frame)
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
func (e *bitfinex) readWs(ctx context.Context) error {
	channelMap := make(map[int]map[string]string)

	// To avoid data race, creating a new local lookup map.
	cfgLookup := make(map[cfgLookupKey]cfgLookupVal, len(e.cfgMap))
	for k, v := range e.cfgMap {
		cfgLookup[k] = v
	}

	// See influxTimeVal struct doc for details.
	itv := influxTimeVal{}
	if e.influx != nil {
		itv.TickerMap = make(map[string]int64)
		itv.TradeMap = make(map[string]int64)
	}

	cd := commitData{
		terTickers:        make([]storage.Ticker, 0, e.connCfg.Terminal.TickerCommitBuf),
		terTrades:         make([]storage.Trade, 0, e.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers:      make([]storage.Ticker, 0, e.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:       make([]storage.Trade, 0, e.connCfg.MySQL.TradeCommitBuf),
		esTickers:         make([]storage.Ticker, 0, e.connCfg.ES.TickerCommitBuf),
		esTrades:          make([]storage.Trade, 0, e.connCfg.ES.TradeCommitBuf),
		influxTickers:     make([]storage.Ticker, 0, e.connCfg.InfluxDB.TickerCommitBuf),
		influxTrades:      make([]storage.Trade, 0, e.connCfg.InfluxDB.TradeCommitBuf),
		natsTickers:       make([]storage.Ticker, 0, e.connCfg.NATS.TickerCommitBuf),
		natsTrades:        make([]storage.Trade, 0, e.connCfg.NATS.TradeCommitBuf),
		clickHouseTickers: make([]storage.Ticker, 0, e.connCfg.ClickHouse.TickerCommitBuf),
		clickHouseTrades:  make([]storage.Trade, 0, e.connCfg.ClickHouse.TradeCommitBuf),
		s3Tickers:         make([]storage.Ticker, 0, e.connCfg.S3.TickerCommitBuf),
		s3Trades:          make([]storage.Trade, 0, e.connCfg.S3.TradeCommitBuf),
	}

	for {
		select {
		default:
			frame, err := e.ws.Read()
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

			// Need to differentiate event and data responses.
			temp := bytes.TrimLeftFunc(frame, unicode.IsSpace)
			if bytes.HasPrefix(temp, []byte("{")) {
				wr := wsEventRespBitfinex{}
				err = jsoniter.Unmarshal(frame, &wr)
				if err != nil {
					logErrStack(err)
					return err
				}

				// Keep a map of id to subscribed market channel as subsequent data frames only contain
				// channel id, actual data and not the market info.
				switch wr.Event {
				case "hb":
				case "subscribed":
					channelInfo := make(map[string]string, 2)
					channelInfo["market"] = wr.Symbol[1:]
					if wr.Channel == "trades" {
						wr.Channel = "trade"
					}
					channelInfo["channel"] = wr.Channel
					channelMap[wr.ChannelID] = channelInfo
					log.Debug().Str("exchange", "bitfinex").Str("func", "readWs").Str("market", channelInfo["market"]).Str("channel", wr.Channel).Msg("channel subscribed")
				case "error":
					log.Error().Str("exchange", "bitfinex").Str("func", "readWs").Int("code", wr.Code).Str("msg", wr.Msg).Msg("")
					return errors.New("bitfinex websocket error")
				case "info":
					if wr.Code != 0 {
						log.Info().Str("exchange", "bitfinex").Str("func", "readWs").Int("code", wr.Code).Str("msg", wr.Msg).Msg("info received")
					} else if wr.Version != 0 {
						log.Info().Str("exchange", "bitfinex").Str("func", "readWs").Int("version", wr.Version).Int("platform-status", wr.Platform.Status).Msg("info received")
					}
				}
			} else if bytes.HasPrefix(temp, []byte("[")) {
				wr := respBitfinex{}
				err = jsoniter.Unmarshal(frame, &wr)
				if err != nil {
					logErrStack(err)
					return err
				}

				if chanID, ok := wr[0].(float64); ok {
					wri := wsRespInfoBitfinex{
						market:  channelMap[int(chanID)]["market"],
						channel: channelMap[int(chanID)]["channel"],
					}

					// Ignore trade snapshot and trade update.
					// Consider ticker and trade execute.
					switch data := wr[1].(type) {
					case string:
						if data != "te" {
							continue
						}
						if wsData, ok := wr[2].([]interface{}); ok {
							wri.respBitfinex = wsData
						} else {
							log.Error().Str("exchange", "bitfinex").Str("func", "readWs").Interface("data", wr[2]).Msg("")
							return errors.New("cannot convert frame data to []interface{}")
						}
					case []interface{}:
						if wri.channel != "ticker" {
							continue
						}
						wri.respBitfinex = data
					}

					// Consider frame only in configured interval, otherwise ignore it.
					key := cfgLookupKey{market: wri.market, channel: wri.channel}
					val := cfgLookup[key]
					if val.wsConsiderIntSec == 0 || time.Since(val.wsLastUpdated).Seconds() >= float64(val.wsConsiderIntSec) {
						val.wsLastUpdated = time.Now()
						wri.mktCommitName = val.mktCommitName
						cfgLookup[key] = val
					} else {
						continue
					}

					err := e.processWs(ctx, &wri, &cd, &itv)
					if err != nil {
						return err
					}
				} else {
					log.Error().Str("exchange", "bitfinex").Str("func", "readWs").Interface("channel id", wr[0]).Msg("")
					return errors.New("cannot convert frame data field channel id to float")
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
func (e *bitfinex) processWs(ctx context.Context, wr *wsRespInfoBitfinex, cd *commitData, itv *influxTimeVal) error {
	switch wr.channel {
	case "ticker":
		ticker := storage.Ticker{}
		ticker.Exchange = "bitfinex"
		ticker.MktID = wr.market
		ticker.MktCommitName = wr.mktCommitName

		// Price sent is an array value, needed to access it by it's position.
		// (Sent array has different data type values so the interface is used.)
		if price, ok := wr.respBitfinex[6].(float64); ok {
			ticker.Price = price
		} else {
			log.Error().Str("exchange", "bitfinex").Str("func", "processWs").Interface("price", wr.respBitfinex[6]).Msg("")
			return errors.New("cannot convert ticker data field price to float")
		}

		ticker.Timestamp = time.Now().UTC()

		key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
		val := e.cfgMap[key]
		if val.terStr {
			cd.terTickersCount++
			cd.terTickers = append(cd.terTickers, ticker)
			if cd.terTickersCount == e.connCfg.Terminal.TickerCommitBuf {
				select {
				case e.wsTerTickers <- cd.terTickers:
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
			if cd.mysqlTickersCount == e.connCfg.MySQL.TickerCommitBuf {
				select {
				case e.wsMysqlTickers <- cd.mysqlTickers:
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
			if cd.esTickersCount == e.connCfg.ES.TickerCommitBuf {
				select {
				case e.wsEsTickers <- cd.esTickers:
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
			if cd.influxTickersCount == e.connCfg.InfluxDB.TickerCommitBuf {
				select {
				case e.wsInfluxTickers <- cd.influxTickers:
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
			if cd.natsTickersCount == e.connCfg.NATS.TickerCommitBuf {
				select {
				case e.wsNatsTickers <- cd.natsTickers:
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
			if cd.clickHouseTickersCount == e.connCfg.ClickHouse.TickerCommitBuf {
				select {
				case e.wsClickHouseTickers <- cd.clickHouseTickers:
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
			if cd.s3TickersCount == e.connCfg.S3.TickerCommitBuf {
				select {
				case e.wsS3Tickers <- cd.s3Tickers:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.s3TickersCount = 0
				cd.s3Tickers = nil
			}
		}
	case "trade":
		trade := storage.Trade{}
		trade.Exchange = "bitfinex"
		trade.MktID = wr.market
		trade.MktCommitName = wr.mktCommitName

		// All the values sent are an array value, needed to access it by it's position.
		// (Sent array has different data type values so the interface is used.)

		if tradeID, ok := wr.respBitfinex[0].(float64); ok {
			trade.TradeID = strconv.FormatFloat(tradeID, 'f', 0, 64)
		} else {
			log.Error().Str("exchange", "bitfinex").Str("func", "processWs").Interface("trade id", wr.respBitfinex[0]).Msg("")
			return errors.New("cannot convert trade data field trade id to float")
		}

		if size, ok := wr.respBitfinex[2].(float64); ok {
			if size > 0 {
				trade.Side = "buy"
			} else {
				trade.Side = "sell"
			}
			trade.Size = math.Abs(size)
		} else {
			log.Error().Str("exchange", "bitfinex").Str("func", "processWs").Interface("size", wr.respBitfinex[2]).Msg("")
			return errors.New("cannot convert trade data field size to float")
		}

		if price, ok := wr.respBitfinex[3].(float64); ok {
			trade.Price = price
		} else {
			log.Error().Str("exchange", "bitfinex").Str("func", "processWs").Interface("price", wr.respBitfinex[3]).Msg("")
			return errors.New("cannot convert trade data field price to float")
		}

		if timestamp, ok := wr.respBitfinex[1].(float64); ok {
			trade.Timestamp = time.Unix(0, int64(timestamp)*int64(time.Millisecond)).UTC()
		} else {
			log.Error().Str("exchange", "bitfinex").Str("func", "processWs").Interface("timestamp", wr.respBitfinex[1]).Msg("")
			return errors.New("cannot convert trade data field timestamp to float")
		}

		key := cfgLookupKey{market: trade.MktID, channel: "trade"}
		val := e.cfgMap[key]
		if val.terStr {
			cd.terTradesCount++
			cd.terTrades = append(cd.terTrades, trade)
			if cd.terTradesCount == e.connCfg.Terminal.TradeCommitBuf {
				select {
				case e.wsTerTrades <- cd.terTrades:
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
			if cd.mysqlTradesCount == e.connCfg.MySQL.TradeCommitBuf {
				select {
				case e.wsMysqlTrades <- cd.mysqlTrades:
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
			if cd.esTradesCount == e.connCfg.ES.TradeCommitBuf {
				select {
				case e.wsEsTrades <- cd.esTrades:
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
			if cd.influxTradesCount == e.connCfg.InfluxDB.TradeCommitBuf {
				select {
				case e.wsInfluxTrades <- cd.influxTrades:
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
			if cd.natsTradesCount == e.connCfg.NATS.TradeCommitBuf {
				select {
				case e.wsNatsTrades <- cd.natsTrades:
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
			if cd.clickHouseTradesCount == e.connCfg.ClickHouse.TradeCommitBuf {
				select {
				case e.wsClickHouseTrades <- cd.clickHouseTrades:
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
			if cd.s3TradesCount == e.connCfg.S3.TradeCommitBuf {
				select {
				case e.wsS3Trades <- cd.s3Trades:
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

// processREST queries exchange for ticker / trade data through REST API in configured intervals,
// transforms it to a common ticker / trade store format,
// buffers the same in memory and
// then sends it to different storage systems for commit through go channels.
func (e *bitfinex) processREST(ctx context.Context, mktID string, mktCommitName string, channel string, interval int) error {
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
		terTickers:        make([]storage.Ticker, 0, e.connCfg.Terminal.TickerCommitBuf),
		terTrades:         make([]storage.Trade, 0, e.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers:      make([]storage.Ticker, 0, e.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:       make([]storage.Trade, 0, e.connCfg.MySQL.TradeCommitBuf),
		esTickers:         make([]storage.Ticker, 0, e.connCfg.ES.TickerCommitBuf),
		esTrades:          make([]storage.Trade, 0, e.connCfg.ES.TradeCommitBuf),
		influxTickers:     make([]storage.Ticker, 0, e.connCfg.InfluxDB.TickerCommitBuf),
		influxTrades:      make([]storage.Trade, 0, e.connCfg.InfluxDB.TradeCommitBuf),
		natsTickers:       make([]storage.Ticker, 0, e.connCfg.NATS.TickerCommitBuf),
		natsTrades:        make([]storage.Trade, 0, e.connCfg.NATS.TradeCommitBuf),
		clickHouseTickers: make([]storage.Ticker, 0, e.connCfg.ClickHouse.TickerCommitBuf),
		clickHouseTrades:  make([]storage.Trade, 0, e.connCfg.ClickHouse.TradeCommitBuf),
		s3Tickers:         make([]storage.Ticker, 0, e.connCfg.S3.TickerCommitBuf),
		s3Trades:          make([]storage.Trade, 0, e.connCfg.S3.TradeCommitBuf),
	}

	switch channel {
	case "ticker":
		req, err = e.rest.Request(ctx, "GET", config.BitfinexRESTBaseURL+"ticker/t"+mktID)
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
	case "trade":
		req, err = e.rest.Request(ctx, "GET", config.BitfinexRESTBaseURL+"trades/t"+mktID+"/hist")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
		q = req.URL.Query()

		// Querying for 100 trades, which is a max allowed for a request by the exchange.
		// If the configured interval gap is big, then maybe it will not return all the trades.
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
				resp, err := e.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := respBitfinex{}
				if err := jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				// Price sent is an array value, needed to access it by it's position.
				// (Sent array has different data type values so the interface is used.)
				price, ok := rr[6].(float64)
				if !ok {
					log.Error().Str("exchange", "bitfinex").Str("func", "processREST").Interface("price", rr[6]).Msg("")
					return errors.New("cannot convert ticker data field price to float")
				}

				ticker := storage.Ticker{
					Exchange:      "bitfinex",
					MktID:         mktID,
					MktCommitName: mktCommitName,
					Price:         price,
					Timestamp:     time.Now().UTC(),
				}

				key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
				val := e.cfgMap[key]
				if val.terStr {
					cd.terTickersCount++
					cd.terTickers = append(cd.terTickers, ticker)
					if cd.terTickersCount == e.connCfg.Terminal.TickerCommitBuf {
						e.ter.CommitTickers(ctx, cd.terTickers)
						cd.terTickersCount = 0
						cd.terTickers = nil
					}
				}
				if val.mysqlStr {
					cd.mysqlTickersCount++
					cd.mysqlTickers = append(cd.mysqlTickers, ticker)
					if cd.mysqlTickersCount == e.connCfg.MySQL.TickerCommitBuf {
						err := e.mysql.CommitTickers(ctx, cd.mysqlTickers)
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
					if cd.esTickersCount == e.connCfg.ES.TickerCommitBuf {
						err := e.es.CommitTickers(ctx, cd.esTickers)
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
					if cd.influxTickersCount == e.connCfg.InfluxDB.TickerCommitBuf {
						err := e.influx.CommitTickers(ctx, cd.influxTickers)
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
					if cd.natsTickersCount == e.connCfg.NATS.TickerCommitBuf {
						err := e.nats.CommitTickers(ctx, cd.natsTickers)
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
					if cd.clickHouseTickersCount == e.connCfg.ClickHouse.TickerCommitBuf {
						err := e.clickhouse.CommitTickers(ctx, cd.clickHouseTickers)
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
					if cd.s3TickersCount == e.connCfg.S3.TickerCommitBuf {
						err := e.s3.CommitTickers(ctx, cd.s3Tickers)
						if err != nil {
							return err
						}
						cd.s3TickersCount = 0
						cd.s3Tickers = nil
					}
				}
			case "trade":
				q.Del("start")
				req.URL.RawQuery = q.Encode()
				resp, err := e.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := []respBitfinex{}
				if err := jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				// All the values sent are an array value, needed to access it by it's position.
				// (Sent array has different data type values so the interface is used.)
				for i := range rr {
					r := rr[i]
					tradeID, ok := r[0].(float64)
					if !ok {
						log.Error().Str("exchange", "bitfinex").Str("func", "processREST").Interface("trade id", r[0]).Msg("")
						return errors.New("cannot convert trade data field trade id to float")
					}

					size, ok := r[2].(float64)
					if !ok {
						log.Error().Str("exchange", "bitfinex").Str("func", "processREST").Interface("size", r[2]).Msg("")
						return errors.New("cannot convert trade data field size to float")
					}
					if size > 0 {
						side = "buy"
					} else {
						side = "sell"
					}
					size = math.Abs(size)

					price, ok := r[3].(float64)
					if !ok {
						log.Error().Str("exchange", "bitfinex").Str("func", "processREST").Interface("price", r[3]).Msg("")
						return errors.New("cannot convert trade data field price to float")
					}

					timestamp, ok := r[1].(float64)
					if !ok {
						log.Error().Str("exchange", "bitfinex").Str("func", "processREST").Interface("timestamp", r[1]).Msg("")
						return errors.New("cannot convert trade data field timestamp to float")
					}

					trade := storage.Trade{
						Exchange:      "bitfinex",
						MktID:         mktID,
						MktCommitName: mktCommitName,
						TradeID:       strconv.FormatFloat(tradeID, 'f', 0, 64),
						Side:          side,
						Size:          size,
						Price:         price,
						Timestamp:     time.Unix(0, int64(timestamp)*int64(time.Millisecond)).UTC(),
					}

					key := cfgLookupKey{market: trade.MktID, channel: "trade"}
					val := e.cfgMap[key]
					if val.terStr {
						cd.terTradesCount++
						cd.terTrades = append(cd.terTrades, trade)
						if cd.terTradesCount == e.connCfg.Terminal.TradeCommitBuf {
							e.ter.CommitTrades(ctx, cd.terTrades)
							cd.terTradesCount = 0
							cd.terTrades = nil
						}
					}
					if val.mysqlStr {
						cd.mysqlTradesCount++
						cd.mysqlTrades = append(cd.mysqlTrades, trade)
						if cd.mysqlTradesCount == e.connCfg.MySQL.TradeCommitBuf {
							err := e.mysql.CommitTrades(ctx, cd.mysqlTrades)
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
						if cd.esTradesCount == e.connCfg.ES.TradeCommitBuf {
							err := e.es.CommitTrades(ctx, cd.esTrades)
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
						if cd.influxTradesCount == e.connCfg.InfluxDB.TradeCommitBuf {
							err := e.influx.CommitTrades(ctx, cd.influxTrades)
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
						if cd.natsTradesCount == e.connCfg.NATS.TradeCommitBuf {
							err := e.nats.CommitTrades(ctx, cd.natsTrades)
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
						if cd.clickHouseTradesCount == e.connCfg.ClickHouse.TradeCommitBuf {
							err := e.clickhouse.CommitTrades(ctx, cd.clickHouseTrades)
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
						if cd.s3TradesCount == e.connCfg.S3.TradeCommitBuf {
							err := e.s3.CommitTrades(ctx, cd.s3Trades)
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
