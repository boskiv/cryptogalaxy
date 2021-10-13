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

// StartOKEx is for starting okex exchange functions.
func StartOKEx(appCtx context.Context, markets []config.Market, retry *config.Retry, connCfg *config.Connection) error {

	// If any error occurs or connection is lost, retry the exchange functions with a time gap till it reaches
	// a configured number of retry.
	// Retry counter will be reset back to zero if the elapsed time since the last retry is greater than the configured one.
	var retryCount int
	lastRetryTime := time.Now()

	for {
		err := newOKEx(appCtx, markets, connCfg)
		if err != nil {
			log.Error().Err(err).Str("exchange", "okex").Msg("error occurred")
			if retry.Number == 0 {
				return errors.New("not able to connect okex exchange. please check the log for details")
			}
			if retry.ResetSec == 0 || time.Since(lastRetryTime).Seconds() < float64(retry.ResetSec) {
				retryCount++
			} else {
				retryCount = 1
			}
			lastRetryTime = time.Now()
			if retryCount > retry.Number {
				err = fmt.Errorf("not able to connect okex exchange even after %d retry", retry.Number)
				log.Error().Err(err).Str("exchange", "okex").Msg("")
				return err
			}

			log.Error().Str("exchange", "okex").Int("retry", retryCount).Msg(fmt.Sprintf("retrying functions in %d seconds", retry.GapSec))
			tick := time.NewTicker(time.Duration(retry.GapSec) * time.Second)
			select {
			case <-tick.C:
				tick.Stop()

			// Return, if there is any error from another exchange.
			case <-appCtx.Done():
				log.Error().Str("exchange", "okex").Msg("ctx canceled, return from StartOKEx")
				return appCtx.Err()
			}
		}
	}
}

type okex struct {
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

type wsSubOkex struct {
	Op   string          `json:"op"`
	Args []wsSubChanOkex `json:"args"`
}

type wsSubChanOkex struct {
	Channel string `json:"channel"`
	InstID  string `json:"instId"`
}

type respOkex struct {
	Event         string         `json:"event"`
	Arg           wsSubChanOkex  `json:"arg"`
	Data          []respDataOkex `json:"data"`
	Code          string         `json:"code"`
	Msg           string         `json:"msg"`
	mktCommitName string
}

type respDataOkex struct {
	TradeID     string `json:"tradeId"`
	Side        string `json:"side"`
	Size        string `json:"sz"`
	TickerPrice string `json:"last"`
	TradePrice  string `json:"px"`
	Timestamp   string `json:"ts"`
}

func newOKEx(appCtx context.Context, markets []config.Market, connCfg *config.Connection) error {

	// If any exchange function fails, force all the other functions to stop and return.
	okexErrGroup, ctx := errgroup.WithContext(appCtx)

	o := okex{connCfg: connCfg}

	err := o.cfgLookup(markets)
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

					err = o.connectWs(ctx)
					if err != nil {
						return err
					}

					okexErrGroup.Go(func() error {
						return o.closeWsConnOnError(ctx)
					})

					okexErrGroup.Go(func() error {
						return o.pingWs(ctx)
					})

					okexErrGroup.Go(func() error {
						return o.readWs(ctx)
					})

					if o.ter != nil {
						okexErrGroup.Go(func() error {
							return storage.TickersToStorage(ctx, o.ter, o.wsTerTickers)
						})
						okexErrGroup.Go(func() error {
							return storage.TradesToStorage(ctx, o.ter, o.wsTerTrades)
						})
					}

					if o.mysql != nil {
						okexErrGroup.Go(func() error {
							return storage.TickersToStorage(ctx, o.mysql, o.wsMysqlTickers)
						})
						okexErrGroup.Go(func() error {
							return storage.TradesToStorage(ctx, o.mysql, o.wsMysqlTrades)
						})
					}

					if o.es != nil {
						okexErrGroup.Go(func() error {
							return storage.TickersToStorage(ctx, o.es, o.wsEsTickers)
						})
						okexErrGroup.Go(func() error {
							return storage.TradesToStorage(ctx, o.es, o.wsEsTrades)
						})
					}

					if o.influx != nil {
						okexErrGroup.Go(func() error {
							return storage.TickersToStorage(ctx, o.influx, o.wsInfluxTickers)
						})
						okexErrGroup.Go(func() error {
							return storage.TradesToStorage(ctx, o.influx, o.wsInfluxTrades)
						})
					}

					if o.nats != nil {
						okexErrGroup.Go(func() error {
							return storage.TickersToStorage(ctx, o.nats, o.wsNatsTickers)
						})
						okexErrGroup.Go(func() error {
							return storage.TradesToStorage(ctx, o.nats, o.wsNatsTrades)
						})
					}

					if o.clickhouse != nil {
						okexErrGroup.Go(func() error {
							return storage.TickersToStorage(ctx, o.clickhouse, o.wsClickHouseTickers)
						})
						okexErrGroup.Go(func() error {
							return storage.TradesToStorage(ctx, o.clickhouse, o.wsClickHouseTrades)
						})
					}

					if o.s3 != nil {
						okexErrGroup.Go(func() error {
							return storage.TickersToStorage(ctx, o.s3, o.wsS3Tickers)
						})
						okexErrGroup.Go(func() error {
							return storage.TradesToStorage(ctx, o.s3, o.wsS3Trades)
						})
					}
				}

				err = o.subWsChannel(market.ID, info.Channel)
				if err != nil {
					return err
				}
				wsCount++

				// Maximum messages sent to a websocket connection per hour is 240.
				// So on a safer side, this will wait for 1 hr 2 min before proceeding once it reaches ~90% of the limit.
				threshold++
				if threshold == 216 {
					log.Debug().Str("exchange", "okex").Int("count", threshold).Msg("subscribe threshold reached, waiting 1 hr 2 min")
					time.Sleep(62 * time.Minute)
					threshold = 0
				}

			case "rest":
				if restCount == 0 {
					err = o.connectRest()
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
				okexErrGroup.Go(func() error {
					return o.processREST(ctx, mktID, mktCommitName, channel, restPingIntSec)
				})

				restCount++
			}
		}
	}

	err = okexErrGroup.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (o *okex) cfgLookup(markets []config.Market) error {

	// Configurations flat map is prepared for easy lookup later in the app.
	o.cfgMap = make(map[cfgLookupKey]cfgLookupVal)
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
					if o.ter == nil {
						o.ter = storage.GetTerminal()
						o.wsTerTickers = make(chan []storage.Ticker, 1)
						o.wsTerTrades = make(chan []storage.Trade, 1)
					}
				case "mysql":
					val.mysqlStr = true
					if o.mysql == nil {
						o.mysql = storage.GetMySQL()
						o.wsMysqlTickers = make(chan []storage.Ticker, 1)
						o.wsMysqlTrades = make(chan []storage.Trade, 1)
					}
				case "elastic_search":
					val.esStr = true
					if o.es == nil {
						o.es = storage.GetElasticSearch()
						o.wsEsTickers = make(chan []storage.Ticker, 1)
						o.wsEsTrades = make(chan []storage.Trade, 1)
					}
				case "influxdb":
					val.influxStr = true
					if o.influx == nil {
						o.influx = storage.GetInfluxDB()
						o.wsInfluxTickers = make(chan []storage.Ticker, 1)
						o.wsInfluxTrades = make(chan []storage.Trade, 1)
					}
				case "nats":
					val.natsStr = true
					if o.nats == nil {
						o.nats = storage.GetNATS()
						o.wsNatsTickers = make(chan []storage.Ticker, 1)
						o.wsNatsTrades = make(chan []storage.Trade, 1)
					}
				case "clickhouse":
					val.clickHouseStr = true
					if o.clickhouse == nil {
						o.clickhouse = storage.GetClickHouse()
						o.wsClickHouseTickers = make(chan []storage.Ticker, 1)
						o.wsClickHouseTrades = make(chan []storage.Trade, 1)
					}
				case "s3":
					val.s3Str = true
					if o.s3 == nil {
						o.s3 = storage.GetS3()
						o.wsS3Tickers = make(chan []storage.Ticker, 1)
						o.wsS3Trades = make(chan []storage.Trade, 1)
					}
				}
			}
			val.mktCommitName = marketCommitName
			o.cfgMap[key] = val
		}
	}
	return nil
}

func (o *okex) connectWs(ctx context.Context) error {
	ws, err := connector.NewWebsocket(ctx, &o.connCfg.WS, config.OKExWebsocketURL)
	if err != nil {
		if !errors.Is(err, ctx.Err()) {
			logErrStack(err)
		}
		return err
	}
	o.ws = ws
	log.Info().Str("exchange", "okex").Msg("websocket connected")
	return nil
}

// closeWsConnOnError closes websocket connection if there is any error in app context.
// This will unblock all read and writes on websocket.
func (o *okex) closeWsConnOnError(ctx context.Context) error {
	<-ctx.Done()
	err := o.ws.Conn.Close()
	if err != nil {
		return err
	}
	return ctx.Err()
}

// pingWs sends ping request to websocket server for every 27 seconds (~10% earlier to required 30 seconds on a safer side).
func (o *okex) pingWs(ctx context.Context) error {
	tick := time.NewTicker(27 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			err := o.ws.Write([]byte(`ping`))
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
func (o *okex) subWsChannel(market string, channel string) error {
	switch channel {
	case "ticker":
		channel = "tickers"
	case "trade":
		channel = "trades"
	}
	channels := make([]wsSubChanOkex, 1)
	channels[0].Channel = channel
	channels[0].InstID = market
	sub := wsSubOkex{
		Op:   "subscribe",
		Args: channels,
	}
	frame, err := jsoniter.Marshal(sub)
	if err != nil {
		logErrStack(err)
		return err
	}
	err = o.ws.Write(frame)
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
func (o *okex) readWs(ctx context.Context) error {

	// To avoid data race, creating a new local lookup map.
	cfgLookup := make(map[cfgLookupKey]cfgLookupVal, len(o.cfgMap))
	for k, v := range o.cfgMap {
		cfgLookup[k] = v
	}

	// See influxTimeVal struct doc for details.
	itv := influxTimeVal{}
	if o.influx != nil {
		itv.TickerMap = make(map[string]int64)
		itv.TradeMap = make(map[string]int64)
	}

	cd := commitData{
		terTickers:        make([]storage.Ticker, 0, o.connCfg.Terminal.TickerCommitBuf),
		terTrades:         make([]storage.Trade, 0, o.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers:      make([]storage.Ticker, 0, o.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:       make([]storage.Trade, 0, o.connCfg.MySQL.TradeCommitBuf),
		esTickers:         make([]storage.Ticker, 0, o.connCfg.ES.TickerCommitBuf),
		esTrades:          make([]storage.Trade, 0, o.connCfg.ES.TradeCommitBuf),
		influxTickers:     make([]storage.Ticker, 0, o.connCfg.InfluxDB.TickerCommitBuf),
		influxTrades:      make([]storage.Trade, 0, o.connCfg.InfluxDB.TradeCommitBuf),
		natsTickers:       make([]storage.Ticker, 0, o.connCfg.NATS.TickerCommitBuf),
		natsTrades:        make([]storage.Trade, 0, o.connCfg.NATS.TradeCommitBuf),
		clickHouseTickers: make([]storage.Ticker, 0, o.connCfg.ClickHouse.TickerCommitBuf),
		clickHouseTrades:  make([]storage.Trade, 0, o.connCfg.ClickHouse.TradeCommitBuf),
		s3Tickers:         make([]storage.Ticker, 0, o.connCfg.S3.TickerCommitBuf),
		s3Trades:          make([]storage.Trade, 0, o.connCfg.S3.TradeCommitBuf),
	}

	for {
		select {
		default:
			frame, err := o.ws.Read()
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

			wr := respOkex{}
			err = jsoniter.Unmarshal(frame, &wr)
			if err != nil {
				if string(frame) == "pong" {
					continue
				}
				logErrStack(err)
				return err
			}

			switch wr.Event {
			case "error":
				log.Error().Str("exchange", "okex").Str("func", "readWs").Str("code", wr.Code).Str("msg", wr.Msg).Msg("")
				return errors.New("okex websocket error")
			case "subscribe":
				if wr.Arg.Channel == "tickers" {
					log.Debug().Str("exchange", "okex").Str("func", "readWs").Str("market", wr.Arg.InstID).Str("channel", "ticker").Msg("channel subscribed")
				} else {
					log.Debug().Str("exchange", "okex").Str("func", "readWs").Str("market", wr.Arg.InstID).Str("channel", "trade").Msg("channel subscribed")
				}
			}

			// Consider frame only in configured interval, otherwise ignore it.
			if len(wr.Data) > 0 {
				if wr.Arg.Channel == "tickers" {
					wr.Arg.Channel = "ticker"
				} else {
					wr.Arg.Channel = "trade"
				}

				switch wr.Arg.Channel {
				case "ticker", "trade":
					key := cfgLookupKey{market: wr.Arg.InstID, channel: wr.Arg.Channel}
					val := cfgLookup[key]
					if val.wsConsiderIntSec == 0 || time.Since(val.wsLastUpdated).Seconds() >= float64(val.wsConsiderIntSec) {
						val.wsLastUpdated = time.Now()
						wr.mktCommitName = val.mktCommitName
						cfgLookup[key] = val
					} else {
						continue
					}

					err := o.processWs(ctx, &wr, &cd, &itv)
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
func (o *okex) processWs(ctx context.Context, wr *respOkex, cd *commitData, itv *influxTimeVal) error {
	switch wr.Arg.Channel {
	case "ticker":
		ticker := storage.Ticker{}
		ticker.Exchange = "okex"
		ticker.MktID = wr.Arg.InstID
		ticker.MktCommitName = wr.mktCommitName

		price, err := strconv.ParseFloat(wr.Data[0].TickerPrice, 64)
		if err != nil {
			logErrStack(err)
			return err
		}
		ticker.Price = price

		// Time sent is in milliseconds string format.
		t, err := strconv.ParseInt(wr.Data[0].Timestamp, 10, 0)
		if err != nil {
			logErrStack(err)
			return err
		}
		ticker.Timestamp = time.Unix(0, t*int64(time.Millisecond)).UTC()

		key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
		val := o.cfgMap[key]
		if val.terStr {
			cd.terTickersCount++
			cd.terTickers = append(cd.terTickers, ticker)
			if cd.terTickersCount == o.connCfg.Terminal.TickerCommitBuf {
				select {
				case o.wsTerTickers <- cd.terTickers:
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
			if cd.mysqlTickersCount == o.connCfg.MySQL.TickerCommitBuf {
				select {
				case o.wsMysqlTickers <- cd.mysqlTickers:
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
			if cd.esTickersCount == o.connCfg.ES.TickerCommitBuf {
				select {
				case o.wsEsTickers <- cd.esTickers:
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
			if cd.influxTickersCount == o.connCfg.InfluxDB.TickerCommitBuf {
				select {
				case o.wsInfluxTickers <- cd.influxTickers:
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
			if cd.natsTickersCount == o.connCfg.NATS.TickerCommitBuf {
				select {
				case o.wsNatsTickers <- cd.natsTickers:
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
			if cd.clickHouseTickersCount == o.connCfg.ClickHouse.TickerCommitBuf {
				select {
				case o.wsClickHouseTickers <- cd.clickHouseTickers:
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
			if cd.s3TickersCount == o.connCfg.S3.TickerCommitBuf {
				select {
				case o.wsS3Tickers <- cd.s3Tickers:
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
			trade.Exchange = "okex"
			trade.MktID = wr.Arg.InstID
			trade.MktCommitName = wr.mktCommitName
			trade.TradeID = data.TradeID
			trade.Side = data.Side

			size, err := strconv.ParseFloat(data.Size, 64)
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

			// Time sent is in milliseconds string format.
			t, err := strconv.ParseInt(data.Timestamp, 10, 0)
			if err != nil {
				logErrStack(err)
				return err
			}
			trade.Timestamp = time.Unix(0, t*int64(time.Millisecond)).UTC()

			key := cfgLookupKey{market: trade.MktID, channel: "trade"}
			val := o.cfgMap[key]
			if val.terStr {
				cd.terTradesCount++
				cd.terTrades = append(cd.terTrades, trade)
				if cd.terTradesCount == o.connCfg.Terminal.TradeCommitBuf {
					select {
					case o.wsTerTrades <- cd.terTrades:
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
				if cd.mysqlTradesCount == o.connCfg.MySQL.TradeCommitBuf {
					select {
					case o.wsMysqlTrades <- cd.mysqlTrades:
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
				if cd.esTradesCount == o.connCfg.ES.TradeCommitBuf {
					select {
					case o.wsEsTrades <- cd.esTrades:
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
				if cd.influxTradesCount == o.connCfg.InfluxDB.TradeCommitBuf {
					select {
					case o.wsInfluxTrades <- cd.influxTrades:
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
				if cd.natsTradesCount == o.connCfg.NATS.TradeCommitBuf {
					select {
					case o.wsNatsTrades <- cd.natsTrades:
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
				if cd.clickHouseTradesCount == o.connCfg.ClickHouse.TradeCommitBuf {
					select {
					case o.wsClickHouseTrades <- cd.clickHouseTrades:
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
				if cd.s3TradesCount == o.connCfg.S3.TradeCommitBuf {
					select {
					case o.wsS3Trades <- cd.s3Trades:
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

func (o *okex) connectRest() error {
	rest, err := connector.GetREST()
	if err != nil {
		logErrStack(err)
		return err
	}
	o.rest = rest
	log.Info().Str("exchange", "okex").Msg("REST connection setup is done")
	return nil
}

// processREST queries exchange for ticker / trade data through REST API in configured intervals,
// transforms it to a common ticker / trade store format,
// buffers the same in memory and
// then sends it to different storage systems for commit through go channels.
func (o *okex) processREST(ctx context.Context, mktID string, mktCommitName string, channel string, interval int) error {
	var (
		req *http.Request
		q   url.Values
		err error

		// See influxTimeVal (exchange.go) struct doc for details.
		influxTickerTime int64
		influxTradeTime  int64
	)

	cd := commitData{
		terTickers:        make([]storage.Ticker, 0, o.connCfg.Terminal.TickerCommitBuf),
		terTrades:         make([]storage.Trade, 0, o.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers:      make([]storage.Ticker, 0, o.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:       make([]storage.Trade, 0, o.connCfg.MySQL.TradeCommitBuf),
		esTickers:         make([]storage.Ticker, 0, o.connCfg.ES.TickerCommitBuf),
		esTrades:          make([]storage.Trade, 0, o.connCfg.ES.TradeCommitBuf),
		influxTickers:     make([]storage.Ticker, 0, o.connCfg.InfluxDB.TickerCommitBuf),
		influxTrades:      make([]storage.Trade, 0, o.connCfg.InfluxDB.TradeCommitBuf),
		natsTickers:       make([]storage.Ticker, 0, o.connCfg.NATS.TickerCommitBuf),
		natsTrades:        make([]storage.Trade, 0, o.connCfg.NATS.TradeCommitBuf),
		clickHouseTickers: make([]storage.Ticker, 0, o.connCfg.ClickHouse.TickerCommitBuf),
		clickHouseTrades:  make([]storage.Trade, 0, o.connCfg.ClickHouse.TradeCommitBuf),
		s3Tickers:         make([]storage.Ticker, 0, o.connCfg.S3.TickerCommitBuf),
		s3Trades:          make([]storage.Trade, 0, o.connCfg.S3.TradeCommitBuf),
	}

	switch channel {
	case "ticker":
		req, err = o.rest.Request(ctx, "GET", config.OKExRESTBaseURL+"market/ticker")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
		q = req.URL.Query()
		q.Add("instId", mktID)
	case "trade":
		req, err = o.rest.Request(ctx, "GET", config.OKExRESTBaseURL+"market/trades")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
		q = req.URL.Query()
		q.Add("instId", mktID)

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
				resp, err := o.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := respOkex{}
				if err = jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				price, err := strconv.ParseFloat(rr.Data[0].TickerPrice, 64)
				if err != nil {
					logErrStack(err)
					return err
				}

				ticker := storage.Ticker{
					Exchange:      "okex",
					MktID:         mktID,
					MktCommitName: mktCommitName,
					Price:         price,
					Timestamp:     time.Now().UTC(),
				}

				key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
				val := o.cfgMap[key]
				if val.terStr {
					cd.terTickersCount++
					cd.terTickers = append(cd.terTickers, ticker)
					if cd.terTickersCount == o.connCfg.Terminal.TickerCommitBuf {
						err := o.ter.CommitTickers(ctx, cd.terTickers)
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
					if cd.mysqlTickersCount == o.connCfg.MySQL.TickerCommitBuf {
						err := o.mysql.CommitTickers(ctx, cd.mysqlTickers)
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
					if cd.esTickersCount == o.connCfg.ES.TickerCommitBuf {
						err := o.es.CommitTickers(ctx, cd.esTickers)
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
					if cd.influxTickersCount == o.connCfg.InfluxDB.TickerCommitBuf {
						err := o.influx.CommitTickers(ctx, cd.influxTickers)
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
					if cd.natsTickersCount == o.connCfg.NATS.TickerCommitBuf {
						err := o.nats.CommitTickers(ctx, cd.natsTickers)
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
					if cd.clickHouseTickersCount == o.connCfg.ClickHouse.TickerCommitBuf {
						err := o.clickhouse.CommitTickers(ctx, cd.clickHouseTickers)
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
					if cd.s3TickersCount == o.connCfg.S3.TickerCommitBuf {
						err := o.s3.CommitTickers(ctx, cd.s3Tickers)
						if err != nil {
							return err
						}
						cd.s3TickersCount = 0
						cd.s3Tickers = nil
					}
				}
			case "trade":
				req.URL.RawQuery = q.Encode()
				resp, err := o.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := respOkex{}
				if err := jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				for i := range rr.Data {
					r := rr.Data[i]

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

					// Time sent is in milliseconds string format.
					t, err := strconv.ParseInt(r.Timestamp, 10, 0)
					if err != nil {
						logErrStack(err)
						return err
					}
					timestamp := time.Unix(0, t*int64(time.Millisecond)).UTC()

					trade := storage.Trade{
						Exchange:      "okex",
						MktID:         mktID,
						MktCommitName: mktCommitName,
						TradeID:       r.TradeID,
						Side:          r.Side,
						Size:          size,
						Price:         price,
						Timestamp:     timestamp,
					}

					key := cfgLookupKey{market: trade.MktID, channel: "trade"}
					val := o.cfgMap[key]
					if val.terStr {
						cd.terTradesCount++
						cd.terTrades = append(cd.terTrades, trade)
						if cd.terTradesCount == o.connCfg.Terminal.TradeCommitBuf {
							err := o.ter.CommitTrades(ctx, cd.terTrades)
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
						if cd.mysqlTradesCount == o.connCfg.MySQL.TradeCommitBuf {
							err := o.mysql.CommitTrades(ctx, cd.mysqlTrades)
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
						if cd.esTradesCount == o.connCfg.ES.TradeCommitBuf {
							err := o.es.CommitTrades(ctx, cd.esTrades)
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
						if cd.influxTradesCount == o.connCfg.InfluxDB.TradeCommitBuf {
							err := o.influx.CommitTrades(ctx, cd.influxTrades)
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
						if cd.natsTradesCount == o.connCfg.NATS.TradeCommitBuf {
							err := o.nats.CommitTrades(ctx, cd.natsTrades)
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
						if cd.clickHouseTradesCount == o.connCfg.ClickHouse.TradeCommitBuf {
							err := o.clickhouse.CommitTrades(ctx, cd.clickHouseTrades)
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
						if cd.s3TradesCount == o.connCfg.S3.TradeCommitBuf {
							err := o.s3.CommitTrades(ctx, cd.s3Trades)
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
