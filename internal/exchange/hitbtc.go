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

// StartHitBTC is for starting hitbtc exchange functions.
func StartHitBTC(appCtx context.Context, markets []config.Market, retry *config.Retry, connCfg *config.Connection) error {

	// If any error occurs or connection is lost, retry the exchange functions with a time gap till it reaches
	// a configured number of retry.
	// Retry counter will be reset back to zero if the elapsed time since the last retry is greater than the configured one.
	var retryCount int
	lastRetryTime := time.Now()

	for {
		err := newHitBTC(appCtx, markets, connCfg)
		if err != nil {
			log.Error().Err(err).Str("exchange", "hitbtc").Msg("error occurred")
			if retry.Number == 0 {
				return errors.New("not able to connect hitbtc exchange. please check the log for details")
			}
			if retry.ResetSec == 0 || time.Since(lastRetryTime).Seconds() < float64(retry.ResetSec) {
				retryCount++
			} else {
				retryCount = 1
			}
			lastRetryTime = time.Now()
			if retryCount > retry.Number {
				err = fmt.Errorf("not able to connect hitbtc exchange even after %d retry", retry.Number)
				log.Error().Err(err).Str("exchange", "hitbtc").Msg("")
				return err
			}

			log.Error().Str("exchange", "hitbtc").Int("retry", retryCount).Msg(fmt.Sprintf("retrying functions in %d seconds", retry.GapSec))
			tick := time.NewTicker(time.Duration(retry.GapSec) * time.Second)
			select {
			case <-tick.C:
				tick.Stop()

			// Return, if there is any error from another exchange.
			case <-appCtx.Done():
				log.Error().Str("exchange", "hitbtc").Msg("ctx canceled, return from StartHitBTC")
				return appCtx.Err()
			}
		}
	}
}

type hitBTC struct {
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

type wsSubHitBTC struct {
	Method  string            `json:"method"`
	Channel string            `json:"ch"`
	Params  wsSubParamsHitBTC `json:"params"`
	ID      int               `json:"id"`
}

type wsSubParamsHitBTC struct {
	Symbols [1]string `json:"symbols"`
}

type wsRespHitBTC struct {
	Result        wsRespSubHitBTC                 `json:"result"`
	Channel       string                          `json:"ch"`
	Data          map[string]wsRespDetailHitBTC   `json:"data"`
	Update        map[string][]wsRespDetailHitBTC `json:"update"`
	mktCommitName string
}

type wsRespSubHitBTC struct {
	Channel string   `json:"ch"`
	Subs    []string `json:"subscriptions"`
}

type wsRespDetailHitBTC struct {
	TradeID     uint64 `json:"i"`
	Side        string `json:"s"`
	Qty         string `json:"q"`
	TickerPrice string `json:"c"`
	TradePrice  string `json:"p"`
	Timestamp   int64  `json:"t"`
}

type restRespHitBTC struct {
	TradeID     uint64    `json:"id"`
	Side        string    `json:"side"`
	Qty         string    `json:"qty"`
	TickerPrice string    `json:"last"`
	TradePrice  string    `json:"price"`
	Timestamp   time.Time `json:"timestamp"`
}

func newHitBTC(appCtx context.Context, markets []config.Market, connCfg *config.Connection) error {

	// If any exchange function fails, force all the other functions to stop and return.
	hitBTCErrGroup, ctx := errgroup.WithContext(appCtx)

	h := hitBTC{connCfg: connCfg}

	err := h.cfgLookup(markets)
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

					err = h.connectWs(ctx)
					if err != nil {
						return err
					}

					hitBTCErrGroup.Go(func() error {
						return h.closeWsConnOnError(ctx)
					})

					hitBTCErrGroup.Go(func() error {
						return h.readWs(ctx)
					})

					if h.ter != nil {
						hitBTCErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, h.ter, h.wsTerTickers)
						})
						hitBTCErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, h.ter, h.wsTerTrades)
						})
					}

					if h.mysql != nil {
						hitBTCErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, h.mysql, h.wsMysqlTickers)
						})
						hitBTCErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, h.mysql, h.wsMysqlTrades)
						})
					}

					if h.es != nil {
						hitBTCErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, h.es, h.wsEsTickers)
						})
						hitBTCErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, h.es, h.wsEsTrades)
						})
					}

					if h.influx != nil {
						hitBTCErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, h.influx, h.wsInfluxTickers)
						})
						hitBTCErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, h.influx, h.wsInfluxTrades)
						})
					}

					if h.nats != nil {
						hitBTCErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, h.nats, h.wsNatsTickers)
						})
						hitBTCErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, h.nats, h.wsNatsTrades)
						})
					}

					if h.clickhouse != nil {
						hitBTCErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, h.clickhouse, h.wsClickHouseTickers)
						})
						hitBTCErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, h.clickhouse, h.wsClickHouseTrades)
						})
					}

					if h.s3 != nil {
						hitBTCErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, h.s3, h.wsS3Tickers)
						})
						hitBTCErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, h.s3, h.wsS3Trades)
						})
					}
				}

				key := cfgLookupKey{market: market.ID, channel: info.Channel}
				val := h.cfgMap[key]
				err = h.subWsChannel(market.ID, info.Channel, val.id)
				if err != nil {
					return err
				}
				wsCount++

				// Maximum messages sent to a websocket connection per sec is 20.
				// So on a safer side, this will wait for 2 sec before proceeding once it reaches ~90% of the limit.
				threshold++
				if threshold == 18 {
					log.Debug().Str("exchange", "hitbtc").Int("count", threshold).Msg("subscribe threshold reached, waiting 2 sec")
					time.Sleep(2 * time.Second)
					threshold = 0
				}

			case "rest":
				if restCount == 0 {
					err = h.connectRest()
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
				hitBTCErrGroup.Go(func() error {
					return h.processREST(ctx, mktID, mktCommitName, channel, restPingIntSec)
				})

				restCount++
			}
		}
	}

	err = hitBTCErrGroup.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (h *hitBTC) cfgLookup(markets []config.Market) error {
	var id int

	// Configurations flat map is prepared for easy lookup later in the app.
	h.cfgMap = make(map[cfgLookupKey]cfgLookupVal)
	h.channelIds = make(map[int][2]string)
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
					if h.ter == nil {
						h.ter = storage.GetTerminal()
						h.wsTerTickers = make(chan []storage.Ticker, 1)
						h.wsTerTrades = make(chan []storage.Trade, 1)
					}
				case "mysql":
					val.mysqlStr = true
					if h.mysql == nil {
						h.mysql = storage.GetMySQL()
						h.wsMysqlTickers = make(chan []storage.Ticker, 1)
						h.wsMysqlTrades = make(chan []storage.Trade, 1)
					}
				case "elastic_search":
					val.esStr = true
					if h.es == nil {
						h.es = storage.GetElasticSearch()
						h.wsEsTickers = make(chan []storage.Ticker, 1)
						h.wsEsTrades = make(chan []storage.Trade, 1)
					}
				case "influxdb":
					val.influxStr = true
					if h.influx == nil {
						h.influx = storage.GetInfluxDB()
						h.wsInfluxTickers = make(chan []storage.Ticker, 1)
						h.wsInfluxTrades = make(chan []storage.Trade, 1)
					}
				case "nats":
					val.natsStr = true
					if h.nats == nil {
						h.nats = storage.GetNATS()
						h.wsNatsTickers = make(chan []storage.Ticker, 1)
						h.wsNatsTrades = make(chan []storage.Trade, 1)
					}
				case "clickhouse":
					val.clickHouseStr = true
					if h.clickhouse == nil {
						h.clickhouse = storage.GetClickHouse()
						h.wsClickHouseTickers = make(chan []storage.Ticker, 1)
						h.wsClickHouseTrades = make(chan []storage.Trade, 1)
					}
				case "s3":
					val.s3Str = true
					if h.s3 == nil {
						h.s3 = storage.GetS3()
						h.wsS3Tickers = make(chan []storage.Ticker, 1)
						h.wsS3Trades = make(chan []storage.Trade, 1)
					}
				}
			}

			// Channel id is used to identify channel in subscribe success message of websocket server.
			id++
			h.channelIds[id] = [2]string{market.ID, info.Channel}
			val.id = id

			val.mktCommitName = mktCommitName
			h.cfgMap[key] = val
		}
	}
	return nil
}

func (h *hitBTC) connectWs(ctx context.Context) error {
	ws, err := connector.NewWebsocket(ctx, &h.connCfg.WS, config.HitBTCWebsocketURL)
	if err != nil {
		if !errors.Is(err, ctx.Err()) {
			logErrStack(err)
		}
		return err
	}
	h.ws = ws
	log.Info().Str("exchange", "hitbtc").Msg("websocket connected")
	return nil
}

// closeWsConnOnError closes websocket connection if there is any error in app context.
// This will unblock all read and writes on websocket.
func (h *hitBTC) closeWsConnOnError(ctx context.Context) error {
	<-ctx.Done()
	err := h.ws.Conn.Close()
	if err != nil {
		return err
	}
	return ctx.Err()
}

// subWsChannel sends channel subscription requests to the websocket server.
func (h *hitBTC) subWsChannel(market string, channel string, id int) error {
	switch channel {
	case "ticker":
		channel = "ticker/price/1s"
	case "trade":
		channel = "trades"
	}
	sub := wsSubHitBTC{
		Method:  "subscribe",
		Channel: channel,
		ID:      id,
	}
	sub.Params.Symbols = [1]string{market}
	frame, err := jsoniter.Marshal(sub)
	if err != nil {
		logErrStack(err)
		return err
	}
	err = h.ws.Write(frame)
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
func (h *hitBTC) readWs(ctx context.Context) error {

	// To avoid data race, creating a new local lookup map.
	cfgLookup := make(map[cfgLookupKey]cfgLookupVal, len(h.cfgMap))
	for k, v := range h.cfgMap {
		cfgLookup[k] = v
	}

	// See influxTimeVal struct doc for details.
	itv := influxTimeVal{}
	if h.influx != nil {
		itv.TickerMap = make(map[string]int64)
		itv.TradeMap = make(map[string]int64)
	}

	cd := commitData{
		terTickers:        make([]storage.Ticker, 0, h.connCfg.Terminal.TickerCommitBuf),
		terTrades:         make([]storage.Trade, 0, h.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers:      make([]storage.Ticker, 0, h.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:       make([]storage.Trade, 0, h.connCfg.MySQL.TradeCommitBuf),
		esTickers:         make([]storage.Ticker, 0, h.connCfg.ES.TickerCommitBuf),
		esTrades:          make([]storage.Trade, 0, h.connCfg.ES.TradeCommitBuf),
		influxTickers:     make([]storage.Ticker, 0, h.connCfg.InfluxDB.TickerCommitBuf),
		influxTrades:      make([]storage.Trade, 0, h.connCfg.InfluxDB.TradeCommitBuf),
		natsTickers:       make([]storage.Ticker, 0, h.connCfg.NATS.TickerCommitBuf),
		natsTrades:        make([]storage.Trade, 0, h.connCfg.NATS.TradeCommitBuf),
		clickHouseTickers: make([]storage.Ticker, 0, h.connCfg.ClickHouse.TickerCommitBuf),
		clickHouseTrades:  make([]storage.Trade, 0, h.connCfg.ClickHouse.TradeCommitBuf),
		s3Tickers:         make([]storage.Ticker, 0, h.connCfg.S3.TickerCommitBuf),
		s3Trades:          make([]storage.Trade, 0, h.connCfg.S3.TradeCommitBuf),
	}

	var market string

	for {
		select {
		default:
			frame, err := h.ws.Read()
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

			wr := wsRespHitBTC{}
			err = jsoniter.Unmarshal(frame, &wr)
			if err != nil {
				logErrStack(err)
				return err
			}

			if wr.Result.Channel != "" {
				switch wr.Result.Channel {
				case "ticker/price/1s":
					wr.Result.Channel = "ticker"
				case "trades":
					wr.Result.Channel = "trade"
				}
				for _, sub := range wr.Result.Subs {
					log.Debug().Str("exchange", "hitbtc").Str("func", "readWs").Str("market", sub).Str("channel", wr.Result.Channel).Msg("channel subscribed (this message may be duplicate as server sends list of all subscriptions on each channel subscribe)")
				}
				continue
			}

			switch wr.Channel {
			case "ticker/price/1s":
				wr.Channel = "ticker"
				for k := range wr.Data {
					market = k
					break
				}
			case "trades":
				wr.Channel = "trade"
				for k := range wr.Update {
					market = k
					break
				}
			}

			// Consider frame only in configured interval, otherwise ignore it.
			switch wr.Channel {
			case "ticker", "trade":
				key := cfgLookupKey{market: market, channel: wr.Channel}
				val := cfgLookup[key]
				if val.wsConsiderIntSec == 0 || time.Since(val.wsLastUpdated).Seconds() >= float64(val.wsConsiderIntSec) {
					val.wsLastUpdated = time.Now()
					wr.mktCommitName = val.mktCommitName
					cfgLookup[key] = val
				} else {
					continue
				}

				err := h.processWs(ctx, &wr, &cd, &itv)
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
func (h *hitBTC) processWs(ctx context.Context, wr *wsRespHitBTC, cd *commitData, itv *influxTimeVal) error {
	switch wr.Channel {
	case "ticker":
		ticker := storage.Ticker{}
		ticker.Exchange = "hitbtc"
		ticker.MktCommitName = wr.mktCommitName

		for market, value := range wr.Data {
			ticker.MktID = market

			price, err := strconv.ParseFloat(value.TickerPrice, 64)
			if err != nil {
				logErrStack(err)
				return err
			}
			ticker.Price = price

			// Time sent is in milliseconds.
			ticker.Timestamp = time.Unix(0, value.Timestamp*int64(time.Millisecond)).UTC()
			break
		}

		key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
		val := h.cfgMap[key]
		if val.terStr {
			cd.terTickersCount++
			cd.terTickers = append(cd.terTickers, ticker)
			if cd.terTickersCount == h.connCfg.Terminal.TickerCommitBuf {
				select {
				case h.wsTerTickers <- cd.terTickers:
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
			if cd.mysqlTickersCount == h.connCfg.MySQL.TickerCommitBuf {
				select {
				case h.wsMysqlTickers <- cd.mysqlTickers:
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
			if cd.esTickersCount == h.connCfg.ES.TickerCommitBuf {
				select {
				case h.wsEsTickers <- cd.esTickers:
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
			if cd.influxTickersCount == h.connCfg.InfluxDB.TickerCommitBuf {
				select {
				case h.wsInfluxTickers <- cd.influxTickers:
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
			if cd.natsTickersCount == h.connCfg.NATS.TickerCommitBuf {
				select {
				case h.wsNatsTickers <- cd.natsTickers:
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
			if cd.clickHouseTickersCount == h.connCfg.ClickHouse.TickerCommitBuf {
				select {
				case h.wsClickHouseTickers <- cd.clickHouseTickers:
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
			if cd.s3TickersCount == h.connCfg.S3.TickerCommitBuf {
				select {
				case h.wsS3Tickers <- cd.s3Tickers:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.s3TickersCount = 0
				cd.s3Tickers = nil
			}
		}
	case "trade":
		for market, data := range wr.Update {
			for _, value := range data {
				trade := storage.Trade{}
				trade.Exchange = "hitbtc"
				trade.MktID = market
				trade.MktCommitName = wr.mktCommitName
				trade.TradeID = strconv.FormatUint(value.TradeID, 10)
				trade.Side = value.Side

				size, err := strconv.ParseFloat(value.Qty, 64)
				if err != nil {
					logErrStack(err)
					return err
				}
				trade.Size = size

				price, err := strconv.ParseFloat(value.TradePrice, 64)
				if err != nil {
					logErrStack(err)
					return err
				}
				trade.Price = price

				// Time sent is in milliseconds.
				trade.Timestamp = time.Unix(0, value.Timestamp*int64(time.Millisecond)).UTC()

				key := cfgLookupKey{market: trade.MktID, channel: "trade"}
				val := h.cfgMap[key]
				if val.terStr {
					cd.terTradesCount++
					cd.terTrades = append(cd.terTrades, trade)
					if cd.terTradesCount == h.connCfg.Terminal.TradeCommitBuf {
						select {
						case h.wsTerTrades <- cd.terTrades:
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
					if cd.mysqlTradesCount == h.connCfg.MySQL.TradeCommitBuf {
						select {
						case h.wsMysqlTrades <- cd.mysqlTrades:
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
					if cd.esTradesCount == h.connCfg.ES.TradeCommitBuf {
						select {
						case h.wsEsTrades <- cd.esTrades:
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
					if cd.influxTradesCount == h.connCfg.InfluxDB.TradeCommitBuf {
						select {
						case h.wsInfluxTrades <- cd.influxTrades:
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
					if cd.natsTradesCount == h.connCfg.NATS.TradeCommitBuf {
						select {
						case h.wsNatsTrades <- cd.natsTrades:
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
					if cd.clickHouseTradesCount == h.connCfg.ClickHouse.TradeCommitBuf {
						select {
						case h.wsClickHouseTrades <- cd.clickHouseTrades:
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
					if cd.s3TradesCount == h.connCfg.S3.TradeCommitBuf {
						select {
						case h.wsS3Trades <- cd.s3Trades:
						case <-ctx.Done():
							return ctx.Err()
						}
						cd.s3TradesCount = 0
						cd.s3Trades = nil
					}
				}
			}
			break
		}
	}
	return nil
}

func (h *hitBTC) connectRest() error {
	rest, err := connector.GetREST()
	if err != nil {
		logErrStack(err)
		return err
	}
	h.rest = rest
	log.Info().Str("exchange", "hitbtc").Msg("REST connection setup is done")
	return nil
}

// processREST queries exchange for ticker / trade data through REST API in configured intervals,
// transforms it to a common ticker / trade store format,
// buffers the same in memory and
// then sends it to different storage systems for commit through go channels.
func (h *hitBTC) processREST(ctx context.Context, mktID string, mktCommitName string, channel string, interval int) error {
	var (
		req *http.Request
		q   url.Values
		err error

		// See influxTimeVal (exchange.go) struct doc for details.
		influxTickerTime int64
		influxTradeTime  int64
	)

	cd := commitData{
		terTickers:        make([]storage.Ticker, 0, h.connCfg.Terminal.TickerCommitBuf),
		terTrades:         make([]storage.Trade, 0, h.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers:      make([]storage.Ticker, 0, h.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:       make([]storage.Trade, 0, h.connCfg.MySQL.TradeCommitBuf),
		esTickers:         make([]storage.Ticker, 0, h.connCfg.ES.TickerCommitBuf),
		esTrades:          make([]storage.Trade, 0, h.connCfg.ES.TradeCommitBuf),
		influxTickers:     make([]storage.Ticker, 0, h.connCfg.InfluxDB.TickerCommitBuf),
		influxTrades:      make([]storage.Trade, 0, h.connCfg.InfluxDB.TradeCommitBuf),
		natsTickers:       make([]storage.Ticker, 0, h.connCfg.NATS.TickerCommitBuf),
		natsTrades:        make([]storage.Trade, 0, h.connCfg.NATS.TradeCommitBuf),
		clickHouseTickers: make([]storage.Ticker, 0, h.connCfg.ClickHouse.TickerCommitBuf),
		clickHouseTrades:  make([]storage.Trade, 0, h.connCfg.ClickHouse.TradeCommitBuf),
		s3Tickers:         make([]storage.Ticker, 0, h.connCfg.S3.TickerCommitBuf),
		s3Trades:          make([]storage.Trade, 0, h.connCfg.S3.TradeCommitBuf),
	}

	switch channel {
	case "ticker":
		req, err = h.rest.Request(ctx, "GET", config.HitBTCRESTBaseURL+"ticker")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
		q = req.URL.Query()
		q.Add("symbols", mktID)
	case "trade":
		req, err = h.rest.Request(ctx, "GET", config.HitBTCRESTBaseURL+"trades")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
		q = req.URL.Query()
		q.Add("symbols", mktID)

		// Querying for 100 trades.
		// If the configured interval gap is big, then maybe it will not return all the trades
		// and if the gap is too small, maybe it will return duplicate ones.
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
				resp, err := h.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := make(map[string]restRespHitBTC)
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
					Exchange:      "hitbtc",
					MktID:         mktID,
					MktCommitName: mktCommitName,
					Price:         price,
					Timestamp:     time.Now().UTC(),
				}

				key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
				val := h.cfgMap[key]
				if val.terStr {
					cd.terTickersCount++
					cd.terTickers = append(cd.terTickers, ticker)
					if cd.terTickersCount == h.connCfg.Terminal.TickerCommitBuf {
						err := h.ter.CommitTickers(ctx, cd.terTickers)
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
					if cd.mysqlTickersCount == h.connCfg.MySQL.TickerCommitBuf {
						err := h.mysql.CommitTickers(ctx, cd.mysqlTickers)
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
					if cd.esTickersCount == h.connCfg.ES.TickerCommitBuf {
						err := h.es.CommitTickers(ctx, cd.esTickers)
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
					if cd.influxTickersCount == h.connCfg.InfluxDB.TickerCommitBuf {
						err := h.influx.CommitTickers(ctx, cd.influxTickers)
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
					if cd.natsTickersCount == h.connCfg.NATS.TickerCommitBuf {
						err := h.nats.CommitTickers(ctx, cd.natsTickers)
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
					if cd.clickHouseTickersCount == h.connCfg.ClickHouse.TickerCommitBuf {
						err := h.clickhouse.CommitTickers(ctx, cd.clickHouseTickers)
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
					if cd.s3TickersCount == h.connCfg.S3.TickerCommitBuf {
						err := h.s3.CommitTickers(ctx, cd.s3Tickers)
						if err != nil {
							return err
						}
						cd.s3TickersCount = 0
						cd.s3Tickers = nil
					}
				}
			case "trade":
				req.URL.RawQuery = q.Encode()
				resp, err := h.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := make(map[string][]restRespHitBTC)
				if err := jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				data := rr[mktID]
				for i := range data {
					r := data[i]

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

					trade := storage.Trade{
						Exchange:      "hitbtc",
						MktID:         mktID,
						MktCommitName: mktCommitName,
						TradeID:       strconv.FormatUint(r.TradeID, 10),
						Side:          r.Side,
						Size:          size,
						Price:         price,
						Timestamp:     r.Timestamp,
					}

					key := cfgLookupKey{market: trade.MktID, channel: "trade"}
					val := h.cfgMap[key]
					if val.terStr {
						cd.terTradesCount++
						cd.terTrades = append(cd.terTrades, trade)
						if cd.terTradesCount == h.connCfg.Terminal.TradeCommitBuf {
							err := h.ter.CommitTrades(ctx, cd.terTrades)
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
						if cd.mysqlTradesCount == h.connCfg.MySQL.TradeCommitBuf {
							err := h.mysql.CommitTrades(ctx, cd.mysqlTrades)
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
						if cd.esTradesCount == h.connCfg.ES.TradeCommitBuf {
							err := h.es.CommitTrades(ctx, cd.esTrades)
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
						if cd.influxTradesCount == h.connCfg.InfluxDB.TradeCommitBuf {
							err := h.influx.CommitTrades(ctx, cd.influxTrades)
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
						if cd.natsTradesCount == h.connCfg.NATS.TradeCommitBuf {
							err := h.nats.CommitTrades(ctx, cd.natsTrades)
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
						if cd.clickHouseTradesCount == h.connCfg.ClickHouse.TradeCommitBuf {
							err := h.clickhouse.CommitTrades(ctx, cd.clickHouseTrades)
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
						if cd.s3TradesCount == h.connCfg.S3.TradeCommitBuf {
							err := h.s3.CommitTrades(ctx, cd.s3Trades)
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
