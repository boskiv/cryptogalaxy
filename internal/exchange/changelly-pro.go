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

// StartChangellyPro is for starting changelly-pro exchange functions.
func StartChangellyPro(appCtx context.Context, markets []config.Market, retry *config.Retry, connCfg *config.Connection) error {

	// If any error occurs or connection is lost, retry the exchange functions with a time gap till it reaches
	// a configured number of retry.
	// Retry counter will be reset back to zero if the elapsed time since the last retry is greater than the configured one.
	var retryCount int
	lastRetryTime := time.Now()

	for {
		err := newChangellyPro(appCtx, markets, connCfg)
		if err != nil {
			log.Error().Err(err).Str("exchange", "changelly-pro").Msg("error occurred")
			if retry.Number == 0 {
				return errors.New("not able to connect changelly-pro exchange. please check the log for details")
			}
			if retry.ResetSec == 0 || time.Since(lastRetryTime).Seconds() < float64(retry.ResetSec) {
				retryCount++
			} else {
				retryCount = 1
			}
			lastRetryTime = time.Now()
			if retryCount > retry.Number {
				err = fmt.Errorf("not able to connect changelly-pro exchange even after %d retry", retry.Number)
				log.Error().Err(err).Str("exchange", "changelly-pro").Msg("")
				return err
			}

			log.Error().Str("exchange", "changelly-pro").Int("retry", retryCount).Msg(fmt.Sprintf("retrying functions in %d seconds", retry.GapSec))
			tick := time.NewTicker(time.Duration(retry.GapSec) * time.Second)
			select {
			case <-tick.C:
				tick.Stop()

			// Return, if there is any error from another exchange.
			case <-appCtx.Done():
				log.Error().Str("exchange", "changelly-pro").Msg("ctx canceled, return from StartChangellyPro")
				return appCtx.Err()
			}
		}
	}
}

type changellyPro struct {
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

type wsSubChangellyPro struct {
	Method  string                 `json:"method"`
	Channel string                 `json:"ch"`
	Params  wsSubParamChangellyPro `json:"params"`
	ID      int                    `json:"id"`
}

type wsSubParamChangellyPro struct {
	Symbols [1]string `json:"symbols"`
}

type wsRespChangellyPro struct {
	Channel       string                              `json:"ch"`
	Data          map[string]wsRespDataChangellyPro   `json:"data"`
	Update        map[string][]wsRespDataChangellyPro `json:"update"`
	ID            int                                 `json:"id"`
	market        string
	mktCommitName string
}

type wsRespDataChangellyPro struct {
	TradeID     uint64 `json:"i"`
	Side        string `json:"s"`
	Qty         string `json:"q"`
	TickerPrice string `json:"c"`
	TradePrice  string `json:"p"`
	Timestamp   int64  `json:"t"`
}

type restRespDataChangellyPro struct {
	TradeID     uint64 `json:"id"`
	Side        string `json:"side"`
	Qty         string `json:"qty"`
	TickerPrice string `json:"last"`
	TradePrice  string `json:"price"`
	Timestamp   string `json:"timestamp"`
}

func newChangellyPro(appCtx context.Context, markets []config.Market, connCfg *config.Connection) error {

	// If any exchange function fails, force all the other functions to stop and return.
	changellyProErrGroup, ctx := errgroup.WithContext(appCtx)

	c := changellyPro{connCfg: connCfg}

	err := c.cfgLookup(markets)
	if err != nil {
		return err
	}

	var (
		wsCount   int
		restCount int
		threshold int
	)

	for _, market := range markets {
		for _, info := range market.Info {
			switch info.Connector {
			case "websocket":
				if wsCount == 0 {

					err = c.connectWs(ctx)
					if err != nil {
						return err
					}

					changellyProErrGroup.Go(func() error {
						return c.closeWsConnOnError(ctx)
					})

					changellyProErrGroup.Go(func() error {
						return c.readWs(ctx)
					})

					if c.ter != nil {
						changellyProErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, c.ter, c.wsTerTickers)
						})
						changellyProErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, c.ter, c.wsTerTrades)
						})
					}

					if c.mysql != nil {
						changellyProErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, c.mysql, c.wsMysqlTickers)
						})
						changellyProErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, c.mysql, c.wsMysqlTrades)
						})
					}

					if c.es != nil {
						changellyProErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, c.es, c.wsEsTickers)
						})
						changellyProErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, c.es, c.wsEsTrades)
						})
					}

					if c.influx != nil {
						changellyProErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, c.influx, c.wsInfluxTickers)
						})
						changellyProErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, c.influx, c.wsInfluxTrades)
						})
					}

					if c.nats != nil {
						changellyProErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, c.nats, c.wsNatsTickers)
						})
						changellyProErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, c.nats, c.wsNatsTrades)
						})
					}

					if c.clickhouse != nil {
						changellyProErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, c.clickhouse, c.wsClickHouseTickers)
						})
						changellyProErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, c.clickhouse, c.wsClickHouseTrades)
						})
					}

					if c.s3 != nil {
						changellyProErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, c.s3, c.wsS3Tickers)
						})
						changellyProErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, c.s3, c.wsS3Trades)
						})
					}
				}

				key := cfgLookupKey{market: market.ID, channel: info.Channel}
				val := c.cfgMap[key]
				err = c.subWsChannel(market.ID, info.Channel, val.id)
				if err != nil {
					return err
				}
				wsCount++

				// Maximum messages sent to a websocket connection per sec is 20.
				// So on a safer side, this will wait for 2 sec before proceeding once it reaches ~90% of the limit.
				threshold++
				if threshold == 18 {
					log.Debug().Str("exchange", "changelly-pro").Int("count", threshold).Msg("subscribe threshold reached, waiting 2 sec")
					time.Sleep(2 * time.Second)
					threshold = 0
				}

			case "rest":
				if restCount == 0 {
					err = c.connectRest()
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
				changellyProErrGroup.Go(func() error {
					return c.processREST(ctx, mktID, mktCommitName, channel, restPingIntSec)
				})

				restCount++
			}
		}
	}

	err = changellyProErrGroup.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (c *changellyPro) cfgLookup(markets []config.Market) error {
	var id int

	// Configurations flat map is prepared for easy lookup later in the app.
	c.cfgMap = make(map[cfgLookupKey]cfgLookupVal)
	c.channelIds = make(map[int][2]string)
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
					if c.ter == nil {
						c.ter = storage.GetTerminal()
						c.wsTerTickers = make(chan []storage.Ticker, 1)
						c.wsTerTrades = make(chan []storage.Trade, 1)
					}
				case "mysql":
					val.mysqlStr = true
					if c.mysql == nil {
						c.mysql = storage.GetMySQL()
						c.wsMysqlTickers = make(chan []storage.Ticker, 1)
						c.wsMysqlTrades = make(chan []storage.Trade, 1)
					}
				case "elastic_search":
					val.esStr = true
					if c.es == nil {
						c.es = storage.GetElasticSearch()
						c.wsEsTickers = make(chan []storage.Ticker, 1)
						c.wsEsTrades = make(chan []storage.Trade, 1)
					}
				case "influxdb":
					val.influxStr = true
					if c.influx == nil {
						c.influx = storage.GetInfluxDB()
						c.wsInfluxTickers = make(chan []storage.Ticker, 1)
						c.wsInfluxTrades = make(chan []storage.Trade, 1)
					}
				case "nats":
					val.natsStr = true
					if c.nats == nil {
						c.nats = storage.GetNATS()
						c.wsNatsTickers = make(chan []storage.Ticker, 1)
						c.wsNatsTrades = make(chan []storage.Trade, 1)
					}
				case "clickhouse":
					val.clickHouseStr = true
					if c.clickhouse == nil {
						c.clickhouse = storage.GetClickHouse()
						c.wsClickHouseTickers = make(chan []storage.Ticker, 1)
						c.wsClickHouseTrades = make(chan []storage.Trade, 1)
					}
				case "s3":
					val.s3Str = true
					if c.s3 == nil {
						c.s3 = storage.GetS3()
						c.wsS3Tickers = make(chan []storage.Ticker, 1)
						c.wsS3Trades = make(chan []storage.Trade, 1)
					}
				}
			}

			// Channel id is used to identify channel in subscribe success message of websocket server.
			id++
			c.channelIds[id] = [2]string{market.ID, info.Channel}
			val.id = id

			val.mktCommitName = marketCommitName
			c.cfgMap[key] = val
		}
	}
	return nil
}

func (c *changellyPro) connectWs(ctx context.Context) error {
	ws, err := connector.NewWebsocket(ctx, &c.connCfg.WS, config.ChangellyProWebsocketURL)
	if err != nil {
		if !errors.Is(err, ctx.Err()) {
			logErrStack(err)
		}
		return err
	}
	c.ws = ws
	log.Info().Str("exchange", "changelly-pro").Msg("websocket connected")
	return nil
}

// closeWsConnOnError closes websocket connection if there is any error in app context.
// This will unblock all read and writes on websocket.
func (c *changellyPro) closeWsConnOnError(ctx context.Context) error {
	<-ctx.Done()
	err := c.ws.Conn.Close()
	if err != nil {
		return err
	}
	return ctx.Err()
}

// subWsChannel sends channel subscription requests to the websocket server.
func (c *changellyPro) subWsChannel(market string, channel string, id int) error {
	if channel == "ticker" {
		channel = "ticker/1s"
	} else {
		channel = "trades"
	}
	sub := wsSubChangellyPro{
		Method:  "subscribe",
		Channel: channel,
		ID:      id,
	}
	sub.Params.Symbols = [1]string{market}
	frame, err := jsoniter.Marshal(&sub)
	if err != nil {
		logErrStack(err)
		return err
	}
	err = c.ws.Write(frame)
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
func (c *changellyPro) readWs(ctx context.Context) error {

	// To avoid data race, creating a new local lookup map.
	cfgLookup := make(map[cfgLookupKey]cfgLookupVal, len(c.cfgMap))
	for k, v := range c.cfgMap {
		cfgLookup[k] = v
	}

	// See influxTimeVal struct doc for details.
	itv := influxTimeVal{}
	if c.influx != nil {
		itv.TickerMap = make(map[string]int64)
		itv.TradeMap = make(map[string]int64)
	}

	cd := commitData{
		terTickers:        make([]storage.Ticker, 0, c.connCfg.Terminal.TickerCommitBuf),
		terTrades:         make([]storage.Trade, 0, c.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers:      make([]storage.Ticker, 0, c.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:       make([]storage.Trade, 0, c.connCfg.MySQL.TradeCommitBuf),
		esTickers:         make([]storage.Ticker, 0, c.connCfg.ES.TickerCommitBuf),
		esTrades:          make([]storage.Trade, 0, c.connCfg.ES.TradeCommitBuf),
		influxTickers:     make([]storage.Ticker, 0, c.connCfg.InfluxDB.TickerCommitBuf),
		influxTrades:      make([]storage.Trade, 0, c.connCfg.InfluxDB.TradeCommitBuf),
		natsTickers:       make([]storage.Ticker, 0, c.connCfg.NATS.TickerCommitBuf),
		natsTrades:        make([]storage.Trade, 0, c.connCfg.NATS.TradeCommitBuf),
		clickHouseTickers: make([]storage.Ticker, 0, c.connCfg.ClickHouse.TickerCommitBuf),
		clickHouseTrades:  make([]storage.Trade, 0, c.connCfg.ClickHouse.TradeCommitBuf),
		s3Tickers:         make([]storage.Ticker, 0, c.connCfg.S3.TickerCommitBuf),
		s3Trades:          make([]storage.Trade, 0, c.connCfg.S3.TradeCommitBuf),
	}

	for {
		select {
		default:
			frame, err := c.ws.Read()
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

			wr := wsRespChangellyPro{}
			err = jsoniter.Unmarshal(frame, &wr)
			if err != nil {
				logErrStack(err)
				return err
			}

			if wr.ID != 0 {
				log.Debug().Str("exchange", "changelly-pro").Str("func", "readWs").Str("market", c.channelIds[wr.ID][0]).Str("channel", c.channelIds[wr.ID][1]).Msg("channel subscribed")
				continue
			}

			if wr.Channel == "ticker/1s" {
				wr.Channel = "ticker"
				for k := range wr.Data {
					wr.market = k
				}
			} else {
				wr.Channel = "trade"
				for k := range wr.Update {
					wr.market = k
				}
			}

			// Consider frame only in configured interval, otherwise ignore it.
			switch wr.Channel {
			case "ticker", "trade":
				key := cfgLookupKey{market: wr.market, channel: wr.Channel}
				val := cfgLookup[key]
				if val.wsConsiderIntSec == 0 || time.Since(val.wsLastUpdated).Seconds() >= float64(val.wsConsiderIntSec) {
					val.wsLastUpdated = time.Now()
					wr.mktCommitName = val.mktCommitName
					cfgLookup[key] = val
				} else {
					continue
				}

				err := c.processWs(ctx, &wr, &cd, &itv)
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
func (c *changellyPro) processWs(ctx context.Context, wr *wsRespChangellyPro, cd *commitData, itv *influxTimeVal) error {
	switch wr.Channel {
	case "ticker":
		ticker := storage.Ticker{}
		ticker.Exchange = "changelly-pro"
		ticker.MktID = wr.market
		ticker.MktCommitName = wr.mktCommitName

		price, err := strconv.ParseFloat(wr.Data[wr.market].TickerPrice, 64)
		if err != nil {
			logErrStack(err)
			return err
		}
		ticker.Price = price

		// Time sent is in milliseconds.
		ticker.Timestamp = time.Unix(0, wr.Data[wr.market].Timestamp*int64(time.Millisecond)).UTC()

		key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
		val := c.cfgMap[key]
		if val.terStr {
			cd.terTickersCount++
			cd.terTickers = append(cd.terTickers, ticker)
			if cd.terTickersCount == c.connCfg.Terminal.TickerCommitBuf {
				select {
				case c.wsTerTickers <- cd.terTickers:
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
			if cd.mysqlTickersCount == c.connCfg.MySQL.TickerCommitBuf {
				select {
				case c.wsMysqlTickers <- cd.mysqlTickers:
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
			if cd.esTickersCount == c.connCfg.ES.TickerCommitBuf {
				select {
				case c.wsEsTickers <- cd.esTickers:
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
			if cd.influxTickersCount == c.connCfg.InfluxDB.TickerCommitBuf {
				select {
				case c.wsInfluxTickers <- cd.influxTickers:
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
			if cd.natsTickersCount == c.connCfg.NATS.TickerCommitBuf {
				select {
				case c.wsNatsTickers <- cd.natsTickers:
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
			if cd.clickHouseTickersCount == c.connCfg.ClickHouse.TickerCommitBuf {
				select {
				case c.wsClickHouseTickers <- cd.clickHouseTickers:
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
			if cd.s3TickersCount == c.connCfg.S3.TickerCommitBuf {
				select {
				case c.wsS3Tickers <- cd.s3Tickers:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.s3TickersCount = 0
				cd.s3Tickers = nil
			}
		}
	case "trade":
		for _, data := range wr.Update[wr.market] {
			trade := storage.Trade{}
			trade.Exchange = "changelly-pro"
			trade.MktID = wr.market
			trade.MktCommitName = wr.mktCommitName
			trade.TradeID = strconv.FormatUint(data.TradeID, 10)
			trade.Side = data.Side

			size, err := strconv.ParseFloat(data.Qty, 64)
			if err != nil {
				logErrStack(err)
				return err
			}
			trade.Size = size

			price, err := strconv.ParseFloat(data.TradePrice, 64)
			if err != nil {
				logErrStack(err)
				return err
			}
			trade.Price = price

			// Time sent is in milliseconds.
			trade.Timestamp = time.Unix(0, data.Timestamp*int64(time.Millisecond)).UTC()

			key := cfgLookupKey{market: trade.MktID, channel: "trade"}
			val := c.cfgMap[key]
			if val.terStr {
				cd.terTradesCount++
				cd.terTrades = append(cd.terTrades, trade)
				if cd.terTradesCount == c.connCfg.Terminal.TradeCommitBuf {
					select {
					case c.wsTerTrades <- cd.terTrades:
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
				if cd.mysqlTradesCount == c.connCfg.MySQL.TradeCommitBuf {
					select {
					case c.wsMysqlTrades <- cd.mysqlTrades:
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
				if cd.esTradesCount == c.connCfg.ES.TradeCommitBuf {
					select {
					case c.wsEsTrades <- cd.esTrades:
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
				if cd.influxTradesCount == c.connCfg.InfluxDB.TradeCommitBuf {
					select {
					case c.wsInfluxTrades <- cd.influxTrades:
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
				if cd.natsTradesCount == c.connCfg.NATS.TradeCommitBuf {
					select {
					case c.wsNatsTrades <- cd.natsTrades:
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
				if cd.clickHouseTradesCount == c.connCfg.ClickHouse.TradeCommitBuf {
					select {
					case c.wsClickHouseTrades <- cd.clickHouseTrades:
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
				if cd.s3TradesCount == c.connCfg.S3.TradeCommitBuf {
					select {
					case c.wsS3Trades <- cd.s3Trades:
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

func (c *changellyPro) connectRest() error {
	rest, err := connector.GetREST()
	if err != nil {
		logErrStack(err)
		return err
	}
	c.rest = rest
	log.Info().Str("exchange", "changelly-pro").Msg("REST connection setup is done")
	return nil
}

// processREST queries exchange for ticker / trade data through REST API in configured intervals,
// transforms it to a common ticker / trade store format,
// buffers the same in memory and
// then sends it to different storage systems for commit through go channels.
func (c *changellyPro) processREST(ctx context.Context, mktID string, mktCommitName string, channel string, interval int) error {
	var (
		req *http.Request
		q   url.Values
		err error

		// See influxTimeVal (exchange.go) struct doc for details.
		influxTickerTime int64
		influxTradeTime  int64
	)

	cd := commitData{
		terTickers:        make([]storage.Ticker, 0, c.connCfg.Terminal.TickerCommitBuf),
		terTrades:         make([]storage.Trade, 0, c.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers:      make([]storage.Ticker, 0, c.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:       make([]storage.Trade, 0, c.connCfg.MySQL.TradeCommitBuf),
		esTickers:         make([]storage.Ticker, 0, c.connCfg.ES.TickerCommitBuf),
		esTrades:          make([]storage.Trade, 0, c.connCfg.ES.TradeCommitBuf),
		influxTickers:     make([]storage.Ticker, 0, c.connCfg.InfluxDB.TickerCommitBuf),
		influxTrades:      make([]storage.Trade, 0, c.connCfg.InfluxDB.TradeCommitBuf),
		natsTickers:       make([]storage.Ticker, 0, c.connCfg.NATS.TickerCommitBuf),
		natsTrades:        make([]storage.Trade, 0, c.connCfg.NATS.TradeCommitBuf),
		clickHouseTickers: make([]storage.Ticker, 0, c.connCfg.ClickHouse.TickerCommitBuf),
		clickHouseTrades:  make([]storage.Trade, 0, c.connCfg.ClickHouse.TradeCommitBuf),
		s3Tickers:         make([]storage.Ticker, 0, c.connCfg.S3.TickerCommitBuf),
		s3Trades:          make([]storage.Trade, 0, c.connCfg.S3.TradeCommitBuf),
	}

	switch channel {
	case "ticker":
		req, err = c.rest.Request(ctx, "GET", config.ChangellyProRESTBaseURL+"ticker")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
		q = req.URL.Query()
		q.Add("symbols", mktID)
	case "trade":
		req, err = c.rest.Request(ctx, "GET", config.ChangellyProRESTBaseURL+"trades")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
		q = req.URL.Query()
		q.Add("symbols", mktID)

		// Querying for 100 trades.
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
				req.URL.RawQuery = q.Encode()
				resp, err := c.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := make(map[string]restRespDataChangellyPro, 1)
				if err = jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				price, err := strconv.ParseFloat(rr[mktID].TickerPrice, 64)
				if err != nil {
					logErrStack(err)
					return err
				}

				ticker := storage.Ticker{
					Exchange:      "changelly-pro",
					MktID:         mktID,
					MktCommitName: mktCommitName,
					Price:         price,
					Timestamp:     time.Now().UTC(),
				}

				key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
				val := c.cfgMap[key]
				if val.terStr {
					cd.terTickersCount++
					cd.terTickers = append(cd.terTickers, ticker)
					if cd.terTickersCount == c.connCfg.Terminal.TickerCommitBuf {
						err := c.ter.CommitTickers(ctx, cd.terTickers)
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
					if cd.mysqlTickersCount == c.connCfg.MySQL.TickerCommitBuf {
						err := c.mysql.CommitTickers(ctx, cd.mysqlTickers)
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
					if cd.esTickersCount == c.connCfg.ES.TickerCommitBuf {
						err := c.es.CommitTickers(ctx, cd.esTickers)
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
					if cd.influxTickersCount == c.connCfg.InfluxDB.TickerCommitBuf {
						err := c.influx.CommitTickers(ctx, cd.influxTickers)
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
					if cd.natsTickersCount == c.connCfg.NATS.TickerCommitBuf {
						err := c.nats.CommitTickers(ctx, cd.natsTickers)
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
					if cd.clickHouseTickersCount == c.connCfg.ClickHouse.TickerCommitBuf {
						err := c.clickhouse.CommitTickers(ctx, cd.clickHouseTickers)
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
					if cd.s3TickersCount == c.connCfg.S3.TickerCommitBuf {
						err := c.s3.CommitTickers(ctx, cd.s3Tickers)
						if err != nil {
							return err
						}
						cd.s3TickersCount = 0
						cd.s3Tickers = nil
					}
				}
			case "trade":
				req.URL.RawQuery = q.Encode()
				resp, err := c.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := make(map[string][]restRespDataChangellyPro, 1)
				if err := jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				for i := range rr[mktID] {
					r := rr[mktID][i]

					size, err := strconv.ParseFloat(r.Qty, 64)
					if err != nil {
						logErrStack(err)
						return err
					}

					price, err := strconv.ParseFloat(r.TradePrice, 64)
					if err != nil {
						logErrStack(err)
						return err
					}

					// Time sent is in string format.
					timestamp, err := time.Parse(time.RFC3339Nano, r.Timestamp)
					if err != nil {
						logErrStack(err)
						return err
					}

					trade := storage.Trade{
						Exchange:      "changelly-pro",
						MktID:         mktID,
						MktCommitName: mktCommitName,
						TradeID:       strconv.FormatUint(r.TradeID, 10),
						Side:          r.Side,
						Size:          size,
						Price:         price,
						Timestamp:     timestamp,
					}

					key := cfgLookupKey{market: trade.MktID, channel: "trade"}
					val := c.cfgMap[key]
					if val.terStr {
						cd.terTradesCount++
						cd.terTrades = append(cd.terTrades, trade)
						if cd.terTradesCount == c.connCfg.Terminal.TradeCommitBuf {
							err := c.ter.CommitTrades(ctx, cd.terTrades)
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
						if cd.mysqlTradesCount == c.connCfg.MySQL.TradeCommitBuf {
							err := c.mysql.CommitTrades(ctx, cd.mysqlTrades)
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
						if cd.esTradesCount == c.connCfg.ES.TradeCommitBuf {
							err := c.es.CommitTrades(ctx, cd.esTrades)
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
						if cd.influxTradesCount == c.connCfg.InfluxDB.TradeCommitBuf {
							err := c.influx.CommitTrades(ctx, cd.influxTrades)
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
						if cd.natsTradesCount == c.connCfg.NATS.TradeCommitBuf {
							err := c.nats.CommitTrades(ctx, cd.natsTrades)
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
						if cd.clickHouseTradesCount == c.connCfg.ClickHouse.TradeCommitBuf {
							err := c.clickhouse.CommitTrades(ctx, cd.clickHouseTrades)
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
						if cd.s3TradesCount == c.connCfg.S3.TradeCommitBuf {
							err := c.s3.CommitTrades(ctx, cd.s3Trades)
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
