package exchange

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
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

// StartKraken is for starting kraken exchange functions.
func StartKraken(appCtx context.Context, markets []config.Market, retry *config.Retry, connCfg *config.Connection) error {

	// If any error occurs or connection is lost, retry the exchange functions with a time gap till it reaches
	// a configured number of retry.
	// Retry counter will be reset back to zero if the elapsed time since the last retry is greater than the configured one.
	var retryCount int
	lastRetryTime := time.Now()

	for {
		err := newKraken(appCtx, markets, connCfg)
		if err != nil {
			log.Error().Err(err).Str("exchange", "kraken").Msg("error occurred")
			if retry.Number == 0 {
				return errors.New("not able to connect kraken exchange. please check the log for details")
			}
			if retry.ResetSec == 0 || time.Since(lastRetryTime).Seconds() < float64(retry.ResetSec) {
				retryCount++
			} else {
				retryCount = 1
			}
			lastRetryTime = time.Now()
			if retryCount > retry.Number {
				err = fmt.Errorf("not able to connect kraken exchange even after %d retry", retry.Number)
				log.Error().Err(err).Str("exchange", "kraken").Msg("")
				return err
			}

			log.Error().Str("exchange", "kraken").Int("retry", retryCount).Msg(fmt.Sprintf("retrying functions in %d seconds", retry.GapSec))
			tick := time.NewTicker(time.Duration(retry.GapSec) * time.Second)
			select {
			case <-tick.C:
				tick.Stop()

			// Return, if there is any error from another exchange.
			case <-appCtx.Done():
				log.Error().Str("exchange", "kraken").Msg("ctx canceled, return from StartKraken")
				return appCtx.Err()
			}
		}
	}
}

type kraken struct {
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

type wsSubKraken struct {
	Event string         `json:"event"`
	Pair  [1]string      `json:"pair"`
	Sub   wsSubSubKraken `json:"subscription"`
}

type wsSubSubKraken struct {
	Name string `json:"name"`
}

type respKraken []interface{}

type wsRespInfoKraken struct {
	market        string
	channel       string
	tickerResp    map[string]interface{}
	tradeResp     []interface{}
	mktCommitName string
}

type wsEventRespKraken struct {
	ChannelName string `json:"channelName"`
	Event       string `json:"event"`
	Pair        string `json:"pair"`
	Status      string `json:"status"`
	ErrMsg      string `json:"errorMessage"`
	Sub         struct {
		Name string `json:"name"`
	} `json:"subscription"`
}

type restRespKraken struct {
	Result map[string]interface{} `json:"result"`
}

func newKraken(appCtx context.Context, markets []config.Market, connCfg *config.Connection) error {

	// If any exchange function fails, force all the other functions to stop and return.
	krakenErrGroup, ctx := errgroup.WithContext(appCtx)

	k := kraken{connCfg: connCfg}

	err := k.cfgLookup(markets)
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

					err = k.connectWs(ctx)
					if err != nil {
						return err
					}

					krakenErrGroup.Go(func() error {
						return k.closeWsConnOnError(ctx)
					})

					krakenErrGroup.Go(func() error {
						return k.readWs(ctx)
					})

					if k.ter != nil {
						krakenErrGroup.Go(func() error {
							return storage.TickersToStorage(ctx, k.ter, k.wsTerTickers)
						})
						krakenErrGroup.Go(func() error {
							return storage.TradesToStorage(ctx, k.ter, k.wsTerTrades)
						})
					}

					if k.mysql != nil {
						krakenErrGroup.Go(func() error {
							return storage.TickersToStorage(ctx, k.mysql, k.wsMysqlTickers)
						})
						krakenErrGroup.Go(func() error {
							return storage.TradesToStorage(ctx, k.mysql, k.wsMysqlTrades)
						})
					}

					if k.es != nil {
						krakenErrGroup.Go(func() error {
							return storage.TickersToStorage(ctx, k.es, k.wsEsTickers)
						})
						krakenErrGroup.Go(func() error {
							return storage.TradesToStorage(ctx, k.es, k.wsEsTrades)
						})
					}

					if k.influx != nil {
						krakenErrGroup.Go(func() error {
							return storage.TickersToStorage(ctx, k.influx, k.wsInfluxTickers)
						})
						krakenErrGroup.Go(func() error {
							return storage.TradesToStorage(ctx, k.influx, k.wsInfluxTrades)
						})
					}

					if k.nats != nil {
						krakenErrGroup.Go(func() error {
							return storage.TickersToStorage(ctx, k.nats, k.wsNatsTickers)
						})
						krakenErrGroup.Go(func() error {
							return storage.TradesToStorage(ctx, k.nats, k.wsNatsTrades)
						})
					}

					if k.clickhouse != nil {
						krakenErrGroup.Go(func() error {
							return storage.TickersToStorage(ctx, k.clickhouse, k.wsClickHouseTickers)
						})
						krakenErrGroup.Go(func() error {
							return storage.TradesToStorage(ctx, k.clickhouse, k.wsClickHouseTrades)
						})
					}

					if k.s3 != nil {
						krakenErrGroup.Go(func() error {
							return storage.TickersToStorage(ctx, k.s3, k.wsS3Tickers)
						})
						krakenErrGroup.Go(func() error {
							return storage.TradesToStorage(ctx, k.s3, k.wsS3Trades)
						})
					}
				}

				err = k.subWsChannel(market.ID, info.Channel)
				if err != nil {
					return err
				}
				wsCount++
			case "rest":
				if restCount == 0 {
					err = k.connectRest()
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
				krakenErrGroup.Go(func() error {
					return k.processREST(ctx, mktID, mktCommitName, channel, restPingIntSec)
				})

				restCount++
			}
		}
	}

	err = krakenErrGroup.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (k *kraken) cfgLookup(markets []config.Market) error {

	// Configurations flat map is prepared for easy lookup later in the app.
	k.cfgMap = make(map[cfgLookupKey]cfgLookupVal)
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
					if k.ter == nil {
						k.ter = storage.GetTerminal()
						k.wsTerTickers = make(chan []storage.Ticker, 1)
						k.wsTerTrades = make(chan []storage.Trade, 1)
					}
				case "mysql":
					val.mysqlStr = true
					if k.mysql == nil {
						k.mysql = storage.GetMySQL()
						k.wsMysqlTickers = make(chan []storage.Ticker, 1)
						k.wsMysqlTrades = make(chan []storage.Trade, 1)
					}
				case "elastic_search":
					val.esStr = true
					if k.es == nil {
						k.es = storage.GetElasticSearch()
						k.wsEsTickers = make(chan []storage.Ticker, 1)
						k.wsEsTrades = make(chan []storage.Trade, 1)
					}
				case "influxdb":
					val.influxStr = true
					if k.influx == nil {
						k.influx = storage.GetInfluxDB()
						k.wsInfluxTickers = make(chan []storage.Ticker, 1)
						k.wsInfluxTrades = make(chan []storage.Trade, 1)
					}
				case "nats":
					val.natsStr = true
					if k.nats == nil {
						k.nats = storage.GetNATS()
						k.wsNatsTickers = make(chan []storage.Ticker, 1)
						k.wsNatsTrades = make(chan []storage.Trade, 1)
					}
				case "clickhouse":
					val.clickHouseStr = true
					if k.clickhouse == nil {
						k.clickhouse = storage.GetClickHouse()
						k.wsClickHouseTickers = make(chan []storage.Ticker, 1)
						k.wsClickHouseTrades = make(chan []storage.Trade, 1)
					}
				case "s3":
					val.s3Str = true
					if k.s3 == nil {
						k.s3 = storage.GetS3()
						k.wsS3Tickers = make(chan []storage.Ticker, 1)
						k.wsS3Trades = make(chan []storage.Trade, 1)
					}
				}
			}
			val.mktCommitName = marketCommitName
			k.cfgMap[key] = val
		}
	}
	return nil
}

func (k *kraken) connectWs(ctx context.Context) error {
	ws, err := connector.NewWebsocket(ctx, &k.connCfg.WS, config.KrakenWebsocketURL)
	if err != nil {
		if !errors.Is(err, ctx.Err()) {
			logErrStack(err)
		}
		return err
	}
	k.ws = ws
	log.Info().Str("exchange", "kraken").Msg("websocket connected")
	return nil
}

// closeWsConnOnError closes websocket connection if there is any error in app context.
// This will unblock all read and writes on websocket.
func (k *kraken) closeWsConnOnError(ctx context.Context) error {
	<-ctx.Done()
	err := k.ws.Conn.Close()
	if err != nil {
		return err
	}
	return ctx.Err()
}

// subWsChannel sends channel subscription requests to the websocket server.
func (k *kraken) subWsChannel(market string, channel string) error {
	sub := wsSubKraken{
		Event: "subscribe",
		Pair:  [1]string{market},
		Sub: wsSubSubKraken{
			Name: channel,
		},
	}
	frame, err := jsoniter.Marshal(sub)
	if err != nil {
		logErrStack(err)
		return err
	}
	err = k.ws.Write(frame)
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
func (k *kraken) readWs(ctx context.Context) error {

	// To avoid data race, creating a new local lookup map.
	cfgLookup := make(map[cfgLookupKey]cfgLookupVal, len(k.cfgMap))
	for k, v := range k.cfgMap {
		cfgLookup[k] = v
	}

	// See influxTimeVal struct doc for details.
	itv := influxTimeVal{}
	if k.influx != nil {
		itv.TickerMap = make(map[string]int64)
		itv.TradeMap = make(map[string]int64)
	}

	cd := commitData{
		terTickers:        make([]storage.Ticker, 0, k.connCfg.Terminal.TickerCommitBuf),
		terTrades:         make([]storage.Trade, 0, k.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers:      make([]storage.Ticker, 0, k.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:       make([]storage.Trade, 0, k.connCfg.MySQL.TradeCommitBuf),
		esTickers:         make([]storage.Ticker, 0, k.connCfg.ES.TickerCommitBuf),
		esTrades:          make([]storage.Trade, 0, k.connCfg.ES.TradeCommitBuf),
		influxTickers:     make([]storage.Ticker, 0, k.connCfg.InfluxDB.TickerCommitBuf),
		influxTrades:      make([]storage.Trade, 0, k.connCfg.InfluxDB.TradeCommitBuf),
		natsTickers:       make([]storage.Ticker, 0, k.connCfg.NATS.TickerCommitBuf),
		natsTrades:        make([]storage.Trade, 0, k.connCfg.NATS.TradeCommitBuf),
		clickHouseTickers: make([]storage.Ticker, 0, k.connCfg.ClickHouse.TickerCommitBuf),
		clickHouseTrades:  make([]storage.Trade, 0, k.connCfg.ClickHouse.TradeCommitBuf),
		s3Tickers:         make([]storage.Ticker, 0, k.connCfg.S3.TickerCommitBuf),
		s3Trades:          make([]storage.Trade, 0, k.connCfg.S3.TradeCommitBuf),
	}

	for {
		select {
		default:
			frame, err := k.ws.Read()
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
				wr := wsEventRespKraken{}
				err = jsoniter.Unmarshal(frame, &wr)
				if err != nil {
					logErrStack(err)
					return err
				}
				if wr.ErrMsg != "" {
					log.Error().Str("exchange", "kraken").Str("func", "readWs").Str("msg", wr.ErrMsg).Msg("")
					return errors.New("kraken websocket error")
				}
				if wr.Status == "subscribed" {
					log.Debug().Str("exchange", "kraken").Str("func", "readWs").Str("market", wr.Pair).Str("channel", wr.Sub.Name).Msg("channel subscribed")
				}
			} else if bytes.HasPrefix(temp, []byte("[")) {
				wr := respKraken{}
				err = jsoniter.Unmarshal(frame, &wr)
				if err != nil {
					logErrStack(err)
					return err
				}

				if channel, ok := wr[2].(string); ok {
					if market, ok := wr[3].(string); ok {
						wri := wsRespInfoKraken{
							market:  market,
							channel: channel,
						}
						switch data := wr[1].(type) {
						case []interface{}:
							wri.tradeResp = data
						default:
							if tickerResp, ok := data.(map[string]interface{}); ok {
								wri.tickerResp = tickerResp
							} else {
								log.Error().Str("exchange", "kraken").Str("func", "readWs").Interface("ticker", wr[1]).Msg("")
								return errors.New("cannot convert frame ticker data to tickerRespKraken")
							}
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

						err := k.processWs(ctx, &wri, &cd, &itv)
						if err != nil {
							return err
						}
					} else {
						log.Error().Str("exchange", "kraken").Str("func", "readWs").Interface("market", wr[3]).Msg("")
						return errors.New("cannot convert frame data field market to string")
					}
				} else {
					log.Error().Str("exchange", "kraken").Str("func", "readWs").Interface("channel", wr[2]).Msg("")
					return errors.New("cannot convert frame data field channel to string")
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
func (k *kraken) processWs(ctx context.Context, wr *wsRespInfoKraken, cd *commitData, itv *influxTimeVal) error {
	switch wr.channel {
	case "ticker":
		ticker := storage.Ticker{}
		ticker.Exchange = "kraken"
		ticker.MktID = wr.market
		ticker.MktCommitName = wr.mktCommitName

		if data, ok := wr.tickerResp["c"].([]interface{}); ok {
			if str, ok := data[0].(string); ok {
				price, err := strconv.ParseFloat(str, 64)
				if err != nil {
					logErrStack(err)
					return err
				}
				ticker.Price = price
			} else {
				log.Error().Str("exchange", "kraken").Str("func", "processWs").Interface("price", data[0]).Msg("")
				return errors.New("cannot convert ticker price to string")
			}
		} else {
			log.Error().Str("exchange", "kraken").Str("func", "processWs").Interface("price", wr.tickerResp["c"]).Msg("")
			return errors.New("cannot convert ticker data to []interface")
		}
		ticker.Timestamp = time.Now().UTC()

		key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
		val := k.cfgMap[key]
		if val.terStr {
			cd.terTickersCount++
			cd.terTickers = append(cd.terTickers, ticker)
			if cd.terTickersCount == k.connCfg.Terminal.TickerCommitBuf {
				select {
				case k.wsTerTickers <- cd.terTickers:
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
			if cd.mysqlTickersCount == k.connCfg.MySQL.TickerCommitBuf {
				select {
				case k.wsMysqlTickers <- cd.mysqlTickers:
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
			if cd.esTickersCount == k.connCfg.ES.TickerCommitBuf {
				select {
				case k.wsEsTickers <- cd.esTickers:
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
			if cd.influxTickersCount == k.connCfg.InfluxDB.TickerCommitBuf {
				select {
				case k.wsInfluxTickers <- cd.influxTickers:
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
			if cd.natsTickersCount == k.connCfg.NATS.TickerCommitBuf {
				select {
				case k.wsNatsTickers <- cd.natsTickers:
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
			if cd.clickHouseTickersCount == k.connCfg.ClickHouse.TickerCommitBuf {
				select {
				case k.wsClickHouseTickers <- cd.clickHouseTickers:
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
			if cd.s3TickersCount == k.connCfg.S3.TickerCommitBuf {
				select {
				case k.wsS3Tickers <- cd.s3Tickers:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.s3TickersCount = 0
				cd.s3Tickers = nil
			}
		}
	case "trade":
		trade := storage.Trade{}
		trade.Exchange = "kraken"
		trade.MktID = wr.market
		trade.MktCommitName = wr.mktCommitName

		// All the values sent are an array value, needed to access it by it's position.
		// (Sent array has different data type values so the interface is used.)

		for _, trades := range wr.tradeResp {
			if data, ok := trades.([]interface{}); ok {
				if side, ok := data[3].(string); ok {
					if side == "b" {
						trade.Side = "buy"
					} else {
						trade.Side = "sell"
					}
				} else {
					log.Error().Str("exchange", "kraken").Str("func", "processWs").Interface("side", data[3]).Msg("")
					return errors.New("cannot convert trade data field side to string")
				}

				if str, ok := data[1].(string); ok {
					size, err := strconv.ParseFloat(str, 64)
					if err != nil {
						logErrStack(err)
						return err
					}
					trade.Size = size
				} else {
					log.Error().Str("exchange", "kraken").Str("func", "processWs").Interface("size", data[1]).Msg("")
					return errors.New("cannot convert trade data field size to string")
				}

				if str, ok := data[0].(string); ok {
					price, err := strconv.ParseFloat(str, 64)
					if err != nil {
						logErrStack(err)
						return err
					}
					trade.Price = price
				} else {
					log.Error().Str("exchange", "kraken").Str("func", "processWs").Interface("price", data[0]).Msg("")
					return errors.New("cannot convert trade data field price to string")
				}

				// Time sent is in fractional seconds string format.
				if str, ok := data[2].(string); ok {
					timeFloat, err := strconv.ParseFloat(str, 64)
					if err != nil {
						logErrStack(err)
						return err
					}
					intPart, fracPart := math.Modf(timeFloat)
					trade.Timestamp = time.Unix(int64(intPart), int64(fracPart*1e9)).UTC()
				} else {
					log.Error().Str("exchange", "kraken").Str("func", "processWs").Interface("timestamp", data[2]).Msg("")
					return errors.New("cannot convert trade data field timestamp to string")
				}

				key := cfgLookupKey{market: trade.MktID, channel: "trade"}
				val := k.cfgMap[key]
				if val.terStr {
					cd.terTradesCount++
					cd.terTrades = append(cd.terTrades, trade)
					if cd.terTradesCount == k.connCfg.Terminal.TradeCommitBuf {
						select {
						case k.wsTerTrades <- cd.terTrades:
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
					if cd.mysqlTradesCount == k.connCfg.MySQL.TradeCommitBuf {
						select {
						case k.wsMysqlTrades <- cd.mysqlTrades:
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
					if cd.esTradesCount == k.connCfg.ES.TradeCommitBuf {
						select {
						case k.wsEsTrades <- cd.esTrades:
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
					if cd.influxTradesCount == k.connCfg.InfluxDB.TradeCommitBuf {
						select {
						case k.wsInfluxTrades <- cd.influxTrades:
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
					if cd.natsTradesCount == k.connCfg.NATS.TradeCommitBuf {
						select {
						case k.wsNatsTrades <- cd.natsTrades:
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
					if cd.clickHouseTradesCount == k.connCfg.ClickHouse.TradeCommitBuf {
						select {
						case k.wsClickHouseTrades <- cd.clickHouseTrades:
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
					if cd.s3TradesCount == k.connCfg.S3.TradeCommitBuf {
						select {
						case k.wsS3Trades <- cd.s3Trades:
						case <-ctx.Done():
							return ctx.Err()
						}
						cd.s3TradesCount = 0
						cd.s3Trades = nil
					}
				}
			} else {
				log.Error().Str("exchange", "kraken").Str("func", "processWs").Interface("trades", trades).Msg("")
				return errors.New("cannot convert trades to []interface")
			}
		}
	}
	return nil
}

func (k *kraken) connectRest() error {
	rest, err := connector.GetREST()
	if err != nil {
		logErrStack(err)
		return err
	}
	k.rest = rest
	log.Info().Str("exchange", "kraken").Msg("REST connection setup is done")
	return nil
}

// processREST queries exchange for ticker / trade data through REST API in configured intervals,
// transforms it to a common ticker / trade store format,
// buffers the same in memory and
// then sends it to different storage systems for commit through go channels.
func (k *kraken) processREST(ctx context.Context, mktID string, mktCommitName string, channel string, interval int) error {
	var (
		req       *http.Request
		q         url.Values
		err       error
		side      string
		size      float64
		price     float64
		tradeTime time.Time

		// See influxTimeVal (exchange.go) struct doc for details.
		influxTickerTime int64
		influxTradeTime  int64
	)

	cd := commitData{
		terTickers:        make([]storage.Ticker, 0, k.connCfg.Terminal.TickerCommitBuf),
		terTrades:         make([]storage.Trade, 0, k.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers:      make([]storage.Ticker, 0, k.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:       make([]storage.Trade, 0, k.connCfg.MySQL.TradeCommitBuf),
		esTickers:         make([]storage.Ticker, 0, k.connCfg.ES.TickerCommitBuf),
		esTrades:          make([]storage.Trade, 0, k.connCfg.ES.TradeCommitBuf),
		influxTickers:     make([]storage.Ticker, 0, k.connCfg.InfluxDB.TickerCommitBuf),
		influxTrades:      make([]storage.Trade, 0, k.connCfg.InfluxDB.TradeCommitBuf),
		natsTickers:       make([]storage.Ticker, 0, k.connCfg.NATS.TickerCommitBuf),
		natsTrades:        make([]storage.Trade, 0, k.connCfg.NATS.TradeCommitBuf),
		clickHouseTickers: make([]storage.Ticker, 0, k.connCfg.ClickHouse.TickerCommitBuf),
		clickHouseTrades:  make([]storage.Trade, 0, k.connCfg.ClickHouse.TradeCommitBuf),
		s3Tickers:         make([]storage.Ticker, 0, k.connCfg.S3.TickerCommitBuf),
		s3Trades:          make([]storage.Trade, 0, k.connCfg.S3.TradeCommitBuf),
	}

	switch channel {
	case "ticker":
		req, err = k.rest.Request(ctx, "GET", config.KrakenRESTBaseURL+"Ticker")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
		q = req.URL.Query()
		q.Add("pair", mktID)
	case "trade":
		req, err = k.rest.Request(ctx, "GET", config.KrakenRESTBaseURL+"Trades")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
		q = req.URL.Query()
		q.Add("pair", mktID)

		// Returns 1000 trades.
		// If the configured interval gap is big, then maybe it will not return all the trades
		// and if the gap is too small, maybe it will return duplicate ones.
		// Better to use websocket.
	}

	tick := time.NewTicker(time.Duration(interval) * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:

			switch channel {
			case "ticker":
				req.URL.RawQuery = q.Encode()
				resp, err := k.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := restRespKraken{}
				if err = jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				for _, result := range rr.Result {
					if ticker, ok := result.(map[string]interface{}); ok {
						if data, ok := ticker["c"].([]interface{}); ok {
							if str, ok := data[0].(string); ok {
								p, err := strconv.ParseFloat(str, 64)
								if err != nil {
									logErrStack(err)
									return err
								}
								price = p
							} else {
								log.Error().Str("exchange", "kraken").Str("func", "processREST").Interface("price", data[0]).Msg("")
								return errors.New("cannot convert ticker price to string")
							}
						} else {
							log.Error().Str("exchange", "kraken").Str("func", "processREST").Interface("price", ticker["c"]).Msg("")
							return errors.New("cannot convert ticker price to []interface")
						}
					} else {
						log.Error().Str("exchange", "kraken").Str("func", "processREST").Interface("price", rr.Result[mktID]).Msg("")
						return errors.New("cannot convert ticker result to map[string]interface")
					}
				}

				ticker := storage.Ticker{
					Exchange:      "kraken",
					MktID:         mktID,
					MktCommitName: mktCommitName,
					Price:         price,
					Timestamp:     time.Now().UTC(),
				}

				key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
				val := k.cfgMap[key]
				if val.terStr {
					cd.terTickersCount++
					cd.terTickers = append(cd.terTickers, ticker)
					if cd.terTickersCount == k.connCfg.Terminal.TickerCommitBuf {
						err := k.ter.CommitTickers(ctx, cd.terTickers)
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
					if cd.mysqlTickersCount == k.connCfg.MySQL.TickerCommitBuf {
						err := k.mysql.CommitTickers(ctx, cd.mysqlTickers)
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
					if cd.esTickersCount == k.connCfg.ES.TickerCommitBuf {
						err := k.es.CommitTickers(ctx, cd.esTickers)
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
					if cd.influxTickersCount == k.connCfg.InfluxDB.TickerCommitBuf {
						err := k.influx.CommitTickers(ctx, cd.influxTickers)
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
					if cd.natsTickersCount == k.connCfg.NATS.TickerCommitBuf {
						err := k.nats.CommitTickers(ctx, cd.natsTickers)
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
					if cd.clickHouseTickersCount == k.connCfg.ClickHouse.TickerCommitBuf {
						err := k.clickhouse.CommitTickers(ctx, cd.clickHouseTickers)
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
					if cd.s3TickersCount == k.connCfg.S3.TickerCommitBuf {
						err := k.s3.CommitTickers(ctx, cd.s3Tickers)
						if err != nil {
							return err
						}
						cd.s3TickersCount = 0
						cd.s3Tickers = nil
					}
				}
			case "trade":
				req.URL.RawQuery = q.Encode()
				resp, err := k.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := restRespKraken{}
				if err = jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				for key, result := range rr.Result {
					if key == "last" {
						continue
					}
					if v, ok := result.([]interface{}); ok {
						for _, trades := range v {
							if data, ok := trades.([]interface{}); ok {
								if str, ok := data[3].(string); ok {
									if str == "b" {
										side = "buy"
									} else {
										side = "sell"
									}
								} else {
									log.Error().Str("exchange", "kraken").Str("func", "processREST").Interface("side", data[3]).Msg("")
									return errors.New("cannot convert trade data field side to string")
								}

								if str, ok := data[1].(string); ok {
									size, err = strconv.ParseFloat(str, 64)
									if err != nil {
										logErrStack(err)
										return err
									}
								} else {
									log.Error().Str("exchange", "kraken").Str("func", "processREST").Interface("size", data[1]).Msg("")
									return errors.New("cannot convert trade data field size to string")
								}

								if str, ok := data[0].(string); ok {
									price, err = strconv.ParseFloat(str, 64)
									if err != nil {
										logErrStack(err)
										return err
									}
								} else {
									log.Error().Str("exchange", "kraken").Str("func", "processREST").Interface("price", data[0]).Msg("")
									return errors.New("cannot convert trade data field price to string")
								}

								// Time sent is in fractional seconds.
								if timeFloat, ok := data[2].(float64); ok {
									intPart, fracPart := math.Modf(timeFloat)
									tradeTime = time.Unix(int64(intPart), int64(fracPart*1e9)).UTC()
								} else {
									log.Error().Str("exchange", "kraken").Str("func", "processREST").Interface("timestamp", data[2]).Msg("")
									return errors.New("cannot convert trade data field timestamp to float")
								}

								trade := storage.Trade{
									Exchange:      "kraken",
									MktID:         mktID,
									MktCommitName: mktCommitName,
									Side:          side,
									Size:          size,
									Price:         price,
									Timestamp:     tradeTime,
								}

								key := cfgLookupKey{market: trade.MktID, channel: "trade"}
								val := k.cfgMap[key]
								if val.terStr {
									cd.terTradesCount++
									cd.terTrades = append(cd.terTrades, trade)
									if cd.terTradesCount == k.connCfg.Terminal.TradeCommitBuf {
										err := k.ter.CommitTrades(ctx, cd.terTrades)
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
									if cd.mysqlTradesCount == k.connCfg.MySQL.TradeCommitBuf {
										err := k.mysql.CommitTrades(ctx, cd.mysqlTrades)
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
									if cd.esTradesCount == k.connCfg.ES.TradeCommitBuf {
										err := k.es.CommitTrades(ctx, cd.esTrades)
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
									if cd.influxTradesCount == k.connCfg.InfluxDB.TradeCommitBuf {
										err := k.influx.CommitTrades(ctx, cd.influxTrades)
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
									if cd.natsTradesCount == k.connCfg.NATS.TradeCommitBuf {
										err := k.nats.CommitTrades(ctx, cd.natsTrades)
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
									if cd.clickHouseTradesCount == k.connCfg.ClickHouse.TradeCommitBuf {
										err := k.clickhouse.CommitTrades(ctx, cd.clickHouseTrades)
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
									if cd.s3TradesCount == k.connCfg.S3.TradeCommitBuf {
										err := k.s3.CommitTrades(ctx, cd.s3Trades)
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
							} else {
								log.Error().Str("exchange", "kraken").Str("func", "processREST").Interface("trades", trades).Msg("")
								return errors.New("cannot convert trades to []interface")
							}
						}
					} else {
						log.Error().Str("exchange", "kraken").Str("func", "processREST").Interface("trades", result).Msg("")
						return errors.New("cannot convert trades result to []interface")
					}
				}
			}

		// Return, if there is any error from another function or exchange.
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
