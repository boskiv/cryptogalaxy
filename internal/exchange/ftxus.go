package exchange

import (
	"context"
	"fmt"
	"io"
	"math"
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

// StartFtxUS is for starting ftx-us exchange functions.
func StartFtxUS(appCtx context.Context, markets []config.Market, retry *config.Retry, connCfg *config.Connection) error {

	// If any error occurs or connection is lost, retry the exchange functions with a time gap till it reaches
	// a configured number of retry.
	// Retry counter will be reset back to zero if the elapsed time since the last retry is greater than the configured one.
	var retryCount int
	lastRetryTime := time.Now()

	for {
		err := newFtxUS(appCtx, markets, connCfg)
		if err != nil {
			log.Error().Err(err).Str("exchange", "ftx-us").Msg("error occurred")
			if retry.Number == 0 {
				return errors.New("not able to connect ftx-us exchange. please check the log for details")
			}
			if retry.ResetSec == 0 || time.Since(lastRetryTime).Seconds() < float64(retry.ResetSec) {
				retryCount++
			} else {
				retryCount = 1
			}
			lastRetryTime = time.Now()
			if retryCount > retry.Number {
				err = fmt.Errorf("not able to connect ftx-us exchange even after %d retry", retry.Number)
				log.Error().Err(err).Str("exchange", "ftx-us").Msg("")
				return err
			}

			log.Error().Str("exchange", "ftx-us").Int("retry", retryCount).Msg(fmt.Sprintf("retrying functions in %d seconds", retry.GapSec))
			tick := time.NewTicker(time.Duration(retry.GapSec) * time.Second)
			select {
			case <-tick.C:
				tick.Stop()

			// Return, if there is any error from another exchange.
			case <-appCtx.Done():
				log.Error().Str("exchange", "ftx-us").Msg("ctx canceled, return from StartFtxUS")
				return appCtx.Err()
			}
		}
	}
}

type ftxUS struct {
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

type wsRespFtxUS struct {
	Channel       string              `json:"channel"`
	Market        string              `json:"market"`
	Type          string              `json:"type"`
	Code          int                 `json:"code"`
	Msg           string              `json:"msg"`
	Data          jsoniter.RawMessage `json:"data"`
	mktCommitName string
}

type wsTickerRespDataFtxUS struct {
	Last float64 `json:"last"`
	Time float64 `json:"time"`
}

type wsTradeRespDataFtxUS struct {
	Side  string  `json:"side"`
	Size  float64 `json:"size"`
	Price float64 `json:"price"`
	Time  string  `json:"time"`
}

type restTickerRespFtxUS struct {
	Success bool                `json:"success"`
	Result  restRespResultFtxUS `json:"result"`
}

type restTradeRespFtxUS struct {
	Success bool                  `json:"success"`
	Result  []restRespResultFtxUS `json:"result"`
}

type restRespResultFtxUS struct {
	Last  float64 `json:"last"`
	Side  string  `json:"side"`
	Size  float64 `json:"size"`
	Price float64 `json:"price"`
	Time  string  `json:"time"`
}

func newFtxUS(appCtx context.Context, markets []config.Market, connCfg *config.Connection) error {

	// If any exchange function fails, force all the other functions to stop and return.
	ftxUSErrGroup, ctx := errgroup.WithContext(appCtx)

	f := ftxUS{connCfg: connCfg}

	err := f.cfgLookup(markets)
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

					err = f.connectWs(ctx)
					if err != nil {
						return err
					}

					ftxUSErrGroup.Go(func() error {
						return f.closeWsConnOnError(ctx)
					})

					ftxUSErrGroup.Go(func() error {
						return f.pingWs(ctx)
					})

					ftxUSErrGroup.Go(func() error {
						return f.readWs(ctx)
					})

					if f.ter != nil {
						ftxUSErrGroup.Go(func() error {
							return f.wsTickersToTerminal(ctx)
						})
						ftxUSErrGroup.Go(func() error {
							return f.wsTradesToTerminal(ctx)
						})
					}

					if f.mysql != nil {
						ftxUSErrGroup.Go(func() error {
							return f.wsTickersToMySQL(ctx)
						})
						ftxUSErrGroup.Go(func() error {
							return f.wsTradesToMySQL(ctx)
						})
					}

					if f.es != nil {
						ftxUSErrGroup.Go(func() error {
							return f.wsTickersToES(ctx)
						})
						ftxUSErrGroup.Go(func() error {
							return f.wsTradesToES(ctx)
						})
					}

					if f.influx != nil {
						ftxUSErrGroup.Go(func() error {
							return f.wsTickersToInflux(ctx)
						})
						ftxUSErrGroup.Go(func() error {
							return f.wsTradesToInflux(ctx)
						})
					}

					if f.nats != nil {
						ftxUSErrGroup.Go(func() error {
							return f.wsTickersToNats(ctx)
						})
						ftxUSErrGroup.Go(func() error {
							return f.wsTradesToNats(ctx)
						})
					}

					if f.clickhouse != nil {
						ftxUSErrGroup.Go(func() error {
							return f.wsTickersToClickHouse(ctx)
						})
						ftxUSErrGroup.Go(func() error {
							return f.wsTradesToClickHouse(ctx)
						})
					}

					if f.s3 != nil {
						ftxUSErrGroup.Go(func() error {
							return f.wsTickersToS3(ctx)
						})
						ftxUSErrGroup.Go(func() error {
							return f.wsTradesToS3(ctx)
						})
					}
				}

				err = f.subWsChannel(market.ID, info.Channel)
				if err != nil {
					return err
				}

				wsCount++
			case "rest":
				if restCount == 0 {
					err = f.connectRest()
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
				ftxUSErrGroup.Go(func() error {
					return f.processREST(ctx, mktID, mktCommitName, channel, restPingIntSec)
				})

				restCount++
			}
		}
	}

	err = ftxUSErrGroup.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (f *ftxUS) cfgLookup(markets []config.Market) error {

	// Configurations flat map is prepared for easy lookup later in the app.
	f.cfgMap = make(map[cfgLookupKey]cfgLookupVal)
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
					if f.ter == nil {
						f.ter = storage.GetTerminal()
						f.wsTerTickers = make(chan []storage.Ticker, 1)
						f.wsTerTrades = make(chan []storage.Trade, 1)
					}
				case "mysql":
					val.mysqlStr = true
					if f.mysql == nil {
						f.mysql = storage.GetMySQL()
						f.wsMysqlTickers = make(chan []storage.Ticker, 1)
						f.wsMysqlTrades = make(chan []storage.Trade, 1)
					}
				case "elastic_search":
					val.esStr = true
					if f.es == nil {
						f.es = storage.GetElasticSearch()
						f.wsEsTickers = make(chan []storage.Ticker, 1)
						f.wsEsTrades = make(chan []storage.Trade, 1)
					}
				case "influxdb":
					val.influxStr = true
					if f.influx == nil {
						f.influx = storage.GetInfluxDB()
						f.wsInfluxTickers = make(chan []storage.Ticker, 1)
						f.wsInfluxTrades = make(chan []storage.Trade, 1)
					}
				case "nats":
					val.natsStr = true
					if f.nats == nil {
						f.nats = storage.GetNATS()
						f.wsNatsTickers = make(chan []storage.Ticker, 1)
						f.wsNatsTrades = make(chan []storage.Trade, 1)
					}
				case "clickhouse":
					val.clickHouseStr = true
					if f.clickhouse == nil {
						f.clickhouse = storage.GetClickHouse()
						f.wsClickHouseTickers = make(chan []storage.Ticker, 1)
						f.wsClickHouseTrades = make(chan []storage.Trade, 1)
					}
				case "s3":
					val.s3Str = true
					if f.s3 == nil {
						f.s3 = storage.GetS3()
						f.wsS3Tickers = make(chan []storage.Ticker, 1)
						f.wsS3Trades = make(chan []storage.Trade, 1)
					}
				}
			}
			val.mktCommitName = marketCommitName
			f.cfgMap[key] = val
		}
	}
	return nil
}

func (f *ftxUS) connectWs(ctx context.Context) error {
	ws, err := connector.NewWebsocket(ctx, &f.connCfg.WS, config.FtxUSWebsocketURL)
	if err != nil {
		if !errors.Is(err, ctx.Err()) {
			logErrStack(err)
		}
		return err
	}
	f.ws = ws
	log.Info().Str("exchange", "ftx-us").Msg("websocket connected")
	return nil
}

// closeWsConnOnError closes websocket connection if there is any error in app context.
// This will unblock all read and writes on websocket.
func (f *ftxUS) closeWsConnOnError(ctx context.Context) error {
	<-ctx.Done()
	err := f.ws.Conn.Close()
	if err != nil {
		return err
	}
	return ctx.Err()
}

// pingWs sends ping request to websocket server for every 13 seconds (~10% earlier to required 15 seconds on a safer side).
func (f *ftxUS) pingWs(ctx context.Context) error {
	tick := time.NewTicker(13 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			err := f.ws.Write([]byte(`{"op":"ping"}`))
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					err = errors.New("context canceled")
				} else {
					logErrStack(err)
				}
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// subWsChannel sends channel subscription requests to the websocket server.
func (f *ftxUS) subWsChannel(market string, channel string) error {
	if channel == "trade" {
		channel = "trades"
	}
	frame, err := jsoniter.Marshal(map[string]string{
		"op":      "subscribe",
		"market":  market,
		"channel": channel,
	})
	if err != nil {
		logErrStack(err)
		return err
	}
	err = f.ws.Write(frame)
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
func (f *ftxUS) readWs(ctx context.Context) error {

	// To avoid data race, creating a new local lookup map.
	cfgLookup := make(map[cfgLookupKey]cfgLookupVal, len(f.cfgMap))
	for k, v := range f.cfgMap {
		cfgLookup[k] = v
	}

	// See influxTimeVal struct doc for details.
	itv := influxTimeVal{}
	if f.influx != nil {
		itv.TickerMap = make(map[string]int64)
		itv.TradeMap = make(map[string]int64)
	}

	cd := commitData{
		terTickers:        make([]storage.Ticker, 0, f.connCfg.Terminal.TickerCommitBuf),
		terTrades:         make([]storage.Trade, 0, f.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers:      make([]storage.Ticker, 0, f.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:       make([]storage.Trade, 0, f.connCfg.MySQL.TradeCommitBuf),
		esTickers:         make([]storage.Ticker, 0, f.connCfg.ES.TickerCommitBuf),
		esTrades:          make([]storage.Trade, 0, f.connCfg.ES.TradeCommitBuf),
		influxTickers:     make([]storage.Ticker, 0, f.connCfg.InfluxDB.TickerCommitBuf),
		influxTrades:      make([]storage.Trade, 0, f.connCfg.InfluxDB.TradeCommitBuf),
		natsTickers:       make([]storage.Ticker, 0, f.connCfg.NATS.TickerCommitBuf),
		natsTrades:        make([]storage.Trade, 0, f.connCfg.NATS.TradeCommitBuf),
		clickHouseTickers: make([]storage.Ticker, 0, f.connCfg.ClickHouse.TickerCommitBuf),
		clickHouseTrades:  make([]storage.Trade, 0, f.connCfg.ClickHouse.TradeCommitBuf),
		s3Tickers:         make([]storage.Ticker, 0, f.connCfg.S3.TickerCommitBuf),
		s3Trades:          make([]storage.Trade, 0, f.connCfg.S3.TradeCommitBuf),
	}

	for {
		select {
		default:
			frame, err := f.ws.Read()
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

			wr := wsRespFtxUS{}
			err = jsoniter.Unmarshal(frame, &wr)
			if err != nil {
				logErrStack(err)
				return err
			}

			if wr.Channel == "trades" {
				wr.Channel = "trade"
			}

			switch wr.Type {
			case "pong":
			case "error":
				log.Error().Str("exchange", "ftx-us").Str("func", "readWs").Int("code", wr.Code).Str("msg", wr.Msg).Msg("")
				return errors.New("ftx-us websocket error")
			case "subscribed":
				log.Debug().Str("exchange", "ftx-us").Str("func", "readWs").Str("market", wr.Market).Str("channel", wr.Channel).Msg("channel subscribed")
			case "info":
				log.Info().Str("exchange", "ftx-us").Str("func", "readWs").Int("code", wr.Code).Str("msg", wr.Msg).Msg("info received")
			case "update":

				// Consider frame only in configured interval, otherwise ignore it.
				switch wr.Channel {
				case "ticker", "trade":
					key := cfgLookupKey{market: wr.Market, channel: wr.Channel}
					val := cfgLookup[key]
					if val.wsConsiderIntSec == 0 || time.Since(val.wsLastUpdated).Seconds() >= float64(val.wsConsiderIntSec) {
						val.wsLastUpdated = time.Now()
						wr.mktCommitName = val.mktCommitName
						cfgLookup[key] = val
					} else {
						continue
					}

					err := f.processWs(ctx, &wr, &cd, &itv)
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
func (f *ftxUS) processWs(ctx context.Context, wr *wsRespFtxUS, cd *commitData, itv *influxTimeVal) error {
	switch wr.Channel {
	case "ticker":
		ticker := storage.Ticker{}
		ticker.Exchange = "ftx-us"
		ticker.MktID = wr.Market
		ticker.MktCommitName = wr.mktCommitName

		// Received data is an object for ticker and an array for trade.
		data := wsTickerRespDataFtxUS{}
		if err := jsoniter.Unmarshal(wr.Data, &data); err != nil {
			logErrStack(err)
			return err
		}

		ticker.Price = data.Last

		// Time sent is in fractional seconds.
		intPart, fracPart := math.Modf(data.Time)
		ticker.Timestamp = time.Unix(int64(intPart), int64(fracPart*1e9)).UTC()

		key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
		val := f.cfgMap[key]
		if val.terStr {
			cd.terTickersCount++
			cd.terTickers = append(cd.terTickers, ticker)
			if cd.terTickersCount == f.connCfg.Terminal.TickerCommitBuf {
				select {
				case f.wsTerTickers <- cd.terTickers:
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
			if cd.mysqlTickersCount == f.connCfg.MySQL.TickerCommitBuf {
				select {
				case f.wsMysqlTickers <- cd.mysqlTickers:
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
			if cd.esTickersCount == f.connCfg.ES.TickerCommitBuf {
				select {
				case f.wsEsTickers <- cd.esTickers:
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
			if cd.influxTickersCount == f.connCfg.InfluxDB.TickerCommitBuf {
				select {
				case f.wsInfluxTickers <- cd.influxTickers:
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
			if cd.natsTickersCount == f.connCfg.NATS.TickerCommitBuf {
				select {
				case f.wsNatsTickers <- cd.natsTickers:
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
			if cd.clickHouseTickersCount == f.connCfg.ClickHouse.TickerCommitBuf {
				select {
				case f.wsClickHouseTickers <- cd.clickHouseTickers:
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
			if cd.s3TickersCount == f.connCfg.S3.TickerCommitBuf {
				select {
				case f.wsS3Tickers <- cd.s3Tickers:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.s3TickersCount = 0
				cd.s3Tickers = nil
			}
		}
	case "trade":

		// Received data is an object for ticker and an array for trade.
		dataResp := []wsTradeRespDataFtxUS{}
		if err := jsoniter.Unmarshal(wr.Data, &dataResp); err != nil {
			logErrStack(err)
			return err
		}
		for _, data := range dataResp {
			trade := storage.Trade{}
			trade.Exchange = "ftx-us"
			trade.MktID = wr.Market
			trade.MktCommitName = wr.mktCommitName
			trade.Side = data.Side
			trade.Size = data.Size
			trade.Price = data.Price

			// Time sent is in string format.
			timestamp, err := time.Parse(time.RFC3339Nano, data.Time)
			if err != nil {
				logErrStack(err)
				return err
			}
			trade.Timestamp = timestamp

			key := cfgLookupKey{market: trade.MktID, channel: "trade"}
			val := f.cfgMap[key]
			if val.terStr {
				cd.terTradesCount++
				cd.terTrades = append(cd.terTrades, trade)
				if cd.terTradesCount == f.connCfg.Terminal.TradeCommitBuf {
					select {
					case f.wsTerTrades <- cd.terTrades:
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
				if cd.mysqlTradesCount == f.connCfg.MySQL.TradeCommitBuf {
					select {
					case f.wsMysqlTrades <- cd.mysqlTrades:
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
				if cd.esTradesCount == f.connCfg.ES.TradeCommitBuf {
					select {
					case f.wsEsTrades <- cd.esTrades:
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
				if cd.influxTradesCount == f.connCfg.InfluxDB.TradeCommitBuf {
					select {
					case f.wsInfluxTrades <- cd.influxTrades:
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
				if cd.natsTradesCount == f.connCfg.NATS.TradeCommitBuf {
					select {
					case f.wsNatsTrades <- cd.natsTrades:
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
				if cd.clickHouseTradesCount == f.connCfg.ClickHouse.TradeCommitBuf {
					select {
					case f.wsClickHouseTrades <- cd.clickHouseTrades:
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
				if cd.s3TradesCount == f.connCfg.S3.TradeCommitBuf {
					select {
					case f.wsS3Trades <- cd.s3Trades:
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

func (f *ftxUS) wsTickersToTerminal(ctx context.Context) error {
	for {
		select {
		case data := <-f.wsTerTickers:
			f.ter.CommitTickers(data)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (f *ftxUS) wsTradesToTerminal(ctx context.Context) error {
	for {
		select {
		case data := <-f.wsTerTrades:
			f.ter.CommitTrades(data)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (f *ftxUS) wsTickersToMySQL(ctx context.Context) error {
	for {
		select {
		case data := <-f.wsMysqlTickers:
			err := f.mysql.CommitTickers(ctx, data)
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

func (f *ftxUS) wsTradesToMySQL(ctx context.Context) error {
	for {
		select {
		case data := <-f.wsMysqlTrades:
			err := f.mysql.CommitTrades(ctx, data)
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

func (f *ftxUS) wsTickersToES(ctx context.Context) error {
	for {
		select {
		case data := <-f.wsEsTickers:
			err := f.es.CommitTickers(ctx, data)
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

func (f *ftxUS) wsTradesToES(ctx context.Context) error {
	for {
		select {
		case data := <-f.wsEsTrades:
			err := f.es.CommitTrades(ctx, data)
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

func (f *ftxUS) wsTickersToInflux(ctx context.Context) error {
	for {
		select {
		case data := <-f.wsInfluxTickers:
			err := f.influx.CommitTickers(ctx, data)
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

func (f *ftxUS) wsTradesToInflux(ctx context.Context) error {
	for {
		select {
		case data := <-f.wsInfluxTrades:
			err := f.influx.CommitTrades(ctx, data)
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

func (f *ftxUS) wsTickersToNats(ctx context.Context) error {
	for {
		select {
		case data := <-f.wsNatsTickers:
			err := f.nats.CommitTickers(data)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (f *ftxUS) wsTradesToNats(ctx context.Context) error {
	for {
		select {
		case data := <-f.wsNatsTrades:
			err := f.nats.CommitTrades(data)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (f *ftxUS) wsTickersToClickHouse(ctx context.Context) error {
	for {
		select {
		case data := <-f.wsClickHouseTickers:
			err := f.clickhouse.CommitTickers(ctx, data)
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

func (f *ftxUS) wsTradesToClickHouse(ctx context.Context) error {
	for {
		select {
		case data := <-f.wsClickHouseTrades:
			err := f.clickhouse.CommitTrades(ctx, data)
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

func (f *ftxUS) wsTickersToS3(ctx context.Context) error {
	for {
		select {
		case data := <-f.wsS3Tickers:
			err := f.s3.CommitTickers(ctx, data)
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

func (f *ftxUS) wsTradesToS3(ctx context.Context) error {
	for {
		select {
		case data := <-f.wsS3Trades:
			err := f.s3.CommitTrades(ctx, data)
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

func (f *ftxUS) connectRest() error {
	rest, err := connector.GetREST()
	if err != nil {
		logErrStack(err)
		return err
	}
	f.rest = rest
	log.Info().Str("exchange", "ftx-us").Msg("REST connection setup is done")
	return nil
}

// processREST queries exchange for ticker / trade data through REST API in configured intervals,
// transforms it to a common ticker / trade store format,
// buffers the same in memory and
// then sends it to different storage systems for commit through go channels.
func (f *ftxUS) processREST(ctx context.Context, mktID string, mktCommitName string, channel string, interval int) error {
	var (
		req *http.Request
		q   url.Values
		err error

		// See influxTimeVal (exchange.go) struct doc for details.
		influxTickerTime int64
		influxTradeTime  int64
	)

	cd := commitData{
		terTickers:        make([]storage.Ticker, 0, f.connCfg.Terminal.TickerCommitBuf),
		terTrades:         make([]storage.Trade, 0, f.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers:      make([]storage.Ticker, 0, f.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:       make([]storage.Trade, 0, f.connCfg.MySQL.TradeCommitBuf),
		esTickers:         make([]storage.Ticker, 0, f.connCfg.ES.TickerCommitBuf),
		esTrades:          make([]storage.Trade, 0, f.connCfg.ES.TradeCommitBuf),
		influxTickers:     make([]storage.Ticker, 0, f.connCfg.InfluxDB.TickerCommitBuf),
		influxTrades:      make([]storage.Trade, 0, f.connCfg.InfluxDB.TradeCommitBuf),
		natsTickers:       make([]storage.Ticker, 0, f.connCfg.NATS.TickerCommitBuf),
		natsTrades:        make([]storage.Trade, 0, f.connCfg.NATS.TradeCommitBuf),
		clickHouseTickers: make([]storage.Ticker, 0, f.connCfg.ClickHouse.TickerCommitBuf),
		clickHouseTrades:  make([]storage.Trade, 0, f.connCfg.ClickHouse.TradeCommitBuf),
		s3Tickers:         make([]storage.Ticker, 0, f.connCfg.S3.TickerCommitBuf),
		s3Trades:          make([]storage.Trade, 0, f.connCfg.S3.TradeCommitBuf),
	}

	switch channel {
	case "ticker":
		req, err = f.rest.Request(ctx, "GET", config.FtxUSRESTBaseURL+"markets/"+mktID)
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
	case "trade":
		req, err = f.rest.Request(ctx, "GET", config.FtxUSRESTBaseURL+"markets/"+mktID+"/trades")
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
				resp, err := f.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := restTickerRespFtxUS{}
				if err := jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				r := rr.Result
				ticker := storage.Ticker{
					Exchange:      "ftx-us",
					MktID:         mktID,
					MktCommitName: mktCommitName,
					Price:         r.Last,
					Timestamp:     time.Now().UTC(),
				}

				key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
				val := f.cfgMap[key]
				if val.terStr {
					cd.terTickersCount++
					cd.terTickers = append(cd.terTickers, ticker)
					if cd.terTickersCount == f.connCfg.Terminal.TickerCommitBuf {
						f.ter.CommitTickers(cd.terTickers)
						cd.terTickersCount = 0
						cd.terTickers = nil
					}
				}
				if val.mysqlStr {
					cd.mysqlTickersCount++
					cd.mysqlTickers = append(cd.mysqlTickers, ticker)
					if cd.mysqlTickersCount == f.connCfg.MySQL.TickerCommitBuf {
						err := f.mysql.CommitTickers(ctx, cd.mysqlTickers)
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
					if cd.esTickersCount == f.connCfg.ES.TickerCommitBuf {
						err := f.es.CommitTickers(ctx, cd.esTickers)
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
					if cd.influxTickersCount == f.connCfg.InfluxDB.TickerCommitBuf {
						err := f.influx.CommitTickers(ctx, cd.influxTickers)
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
					if cd.natsTickersCount == f.connCfg.NATS.TickerCommitBuf {
						err := f.nats.CommitTickers(cd.natsTickers)
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
					if cd.clickHouseTickersCount == f.connCfg.ClickHouse.TickerCommitBuf {
						err := f.clickhouse.CommitTickers(ctx, cd.clickHouseTickers)
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
					if cd.s3TickersCount == f.connCfg.S3.TickerCommitBuf {
						err := f.s3.CommitTickers(ctx, cd.s3Tickers)
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
				resp, err := f.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := restTradeRespFtxUS{}
				if err := jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				for i := range rr.Result {
					r := rr.Result[i]

					// Time sent is in string format.
					timestamp, err := time.Parse(time.RFC3339Nano, r.Time)
					if err != nil {
						logErrStack(err)
						return err
					}

					trade := storage.Trade{
						Exchange:      "ftx-us",
						MktID:         mktID,
						MktCommitName: mktCommitName,
						Side:          r.Side,
						Size:          r.Size,
						Price:         r.Price,
						Timestamp:     timestamp,
					}

					key := cfgLookupKey{market: trade.MktID, channel: "trade"}
					val := f.cfgMap[key]
					if val.terStr {
						cd.terTradesCount++
						cd.terTrades = append(cd.terTrades, trade)
						if cd.terTradesCount == f.connCfg.Terminal.TradeCommitBuf {
							f.ter.CommitTrades(cd.terTrades)
							cd.terTradesCount = 0
							cd.terTrades = nil
						}
					}
					if val.mysqlStr {
						cd.mysqlTradesCount++
						cd.mysqlTrades = append(cd.mysqlTrades, trade)
						if cd.mysqlTradesCount == f.connCfg.MySQL.TradeCommitBuf {
							err := f.mysql.CommitTrades(ctx, cd.mysqlTrades)
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
						if cd.esTradesCount == f.connCfg.ES.TradeCommitBuf {
							err := f.es.CommitTrades(ctx, cd.esTrades)
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
						if cd.influxTradesCount == f.connCfg.InfluxDB.TradeCommitBuf {
							err := f.influx.CommitTrades(ctx, cd.influxTrades)
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
						if cd.natsTradesCount == f.connCfg.NATS.TradeCommitBuf {
							err := f.nats.CommitTrades(cd.natsTrades)
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
						if cd.clickHouseTradesCount == f.connCfg.ClickHouse.TradeCommitBuf {
							err := f.clickhouse.CommitTrades(ctx, cd.clickHouseTrades)
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
						if cd.s3TradesCount == f.connCfg.S3.TradeCommitBuf {
							err := f.s3.CommitTrades(ctx, cd.s3Trades)
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
