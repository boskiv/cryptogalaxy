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

// StartMexo is for starting mexo exchange functions.
func StartMexo(appCtx context.Context, markets []config.Market, retry *config.Retry, connCfg *config.Connection) error {

	// If any error occurs or connection is lost, retry the exchange functions with a time gap till it reaches
	// a configured number of retry.
	// Retry counter will be reset back to zero if the elapsed time since the last retry is greater than the configured one.
	var retryCount int
	lastRetryTime := time.Now()

	for {
		err := newMexo(appCtx, markets, connCfg)
		if err != nil {
			log.Error().Err(err).Str("exchange", "mexo").Msg("error occurred")
			if retry.Number == 0 {
				return errors.New("not able to connect mexo exchange. please check the log for details")
			}
			if retry.ResetSec == 0 || time.Since(lastRetryTime).Seconds() < float64(retry.ResetSec) {
				retryCount++
			} else {
				retryCount = 1
			}
			lastRetryTime = time.Now()
			if retryCount > retry.Number {
				err = fmt.Errorf("not able to connect mexo exchange even after %d retry", retry.Number)
				log.Error().Err(err).Str("exchange", "mexo").Msg("")
				return err
			}

			log.Error().Str("exchange", "mexo").Int("retry", retryCount).Msg(fmt.Sprintf("retrying functions in %d seconds", retry.GapSec))
			tick := time.NewTicker(time.Duration(retry.GapSec) * time.Second)
			select {
			case <-tick.C:
				tick.Stop()

			// Return, if there is any error from another exchange.
			case <-appCtx.Done():
				log.Error().Str("exchange", "mexo").Msg("ctx canceled, return from StartMexo")
				return appCtx.Err()
			}
		}
	}
}

type mexo struct {
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

type wsSubMexo struct {
	Symbol string          `json:"symbol"`
	Topic  string          `json:"topic"`
	Event  string          `json:"event"`
	Params wsSubParamsMexo `json:"params"`
}

type wsSubParamsMexo struct {
	Binary bool `json:"binary"`
}

type wsRespMexo struct {
	Symbol        string           `json:"symbol"`
	Topic         string           `json:"topic"`
	First         bool             `json:"f"`
	Data          []wsRespDataMexo `json:"data"`
	Code          string           `json:"code"`
	Desc          string           `json:"desc"`
	mktCommitName string
}

type wsRespDataMexo struct {
	TradeID     string      `json:"v"`
	Maker       interface{} `json:"m"`
	Size        string      `json:"q"`
	TickerPrice string      `json:"c"`
	TradePrice  string      `json:"p"`
	Timestamp   interface{} `json:"t"`
}

type restRespMexo struct {
	Maker bool   `json:"isBuyerMaker"`
	Qty   string `json:"qty"`
	Price string `json:"price"`
	Time  int64  `json:"time"`
}

func newMexo(appCtx context.Context, markets []config.Market, connCfg *config.Connection) error {

	// If any exchange function fails, force all the other functions to stop and return.
	mexoErrGroup, ctx := errgroup.WithContext(appCtx)

	m := mexo{connCfg: connCfg}

	err := m.cfgLookup(markets)
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

					err = m.connectWs(ctx)
					if err != nil {
						return err
					}

					mexoErrGroup.Go(func() error {
						return m.closeWsConnOnError(ctx)
					})

					mexoErrGroup.Go(func() error {
						return m.pingWs(ctx)
					})

					mexoErrGroup.Go(func() error {
						return m.readWs(ctx)
					})

					if m.ter != nil {
						mexoErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, m.ter, m.wsTerTickers)
						})
						mexoErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, m.ter, m.wsTerTrades)
						})
					}

					if m.mysql != nil {
						mexoErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, m.mysql, m.wsMysqlTickers)
						})
						mexoErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, m.mysql, m.wsMysqlTrades)
						})
					}

					if m.es != nil {
						mexoErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, m.es, m.wsEsTickers)
						})
						mexoErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, m.es, m.wsEsTrades)
						})
					}

					if m.influx != nil {
						mexoErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, m.influx, m.wsInfluxTickers)
						})
						mexoErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, m.influx, m.wsInfluxTrades)
						})
					}

					if m.nats != nil {
						mexoErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, m.nats, m.wsNatsTickers)
						})
						mexoErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, m.nats, m.wsNatsTrades)
						})
					}

					if m.clickhouse != nil {
						mexoErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, m.clickhouse, m.wsClickHouseTickers)
						})
						mexoErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, m.clickhouse, m.wsClickHouseTrades)
						})
					}

					if m.s3 != nil {
						mexoErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, m.s3, m.wsS3Tickers)
						})
						mexoErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, m.s3, m.wsS3Trades)
						})
					}
				}

				err = m.subWsChannel(market.ID, info.Channel)
				if err != nil {
					return err
				}

				wsCount++
			case "rest":
				if restCount == 0 {
					err = m.connectRest()
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
				mexoErrGroup.Go(func() error {
					return m.processREST(ctx, mktID, mktCommitName, channel, restPingIntSec)
				})

				restCount++
			}
		}
	}

	err = mexoErrGroup.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (m *mexo) cfgLookup(markets []config.Market) error {
	var id int

	// Configurations flat map is prepared for easy lookup later in the app.
	m.cfgMap = make(map[cfgLookupKey]cfgLookupVal)
	m.channelIds = make(map[int][2]string)
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
					if m.ter == nil {
						m.ter = storage.GetTerminal()
						m.wsTerTickers = make(chan []storage.Ticker, 1)
						m.wsTerTrades = make(chan []storage.Trade, 1)
					}
				case "mysql":
					val.mysqlStr = true
					if m.mysql == nil {
						m.mysql = storage.GetMySQL()
						m.wsMysqlTickers = make(chan []storage.Ticker, 1)
						m.wsMysqlTrades = make(chan []storage.Trade, 1)
					}
				case "elastic_search":
					val.esStr = true
					if m.es == nil {
						m.es = storage.GetElasticSearch()
						m.wsEsTickers = make(chan []storage.Ticker, 1)
						m.wsEsTrades = make(chan []storage.Trade, 1)
					}
				case "influxdb":
					val.influxStr = true
					if m.influx == nil {
						m.influx = storage.GetInfluxDB()
						m.wsInfluxTickers = make(chan []storage.Ticker, 1)
						m.wsInfluxTrades = make(chan []storage.Trade, 1)
					}
				case "nats":
					val.natsStr = true
					if m.nats == nil {
						m.nats = storage.GetNATS()
						m.wsNatsTickers = make(chan []storage.Ticker, 1)
						m.wsNatsTrades = make(chan []storage.Trade, 1)
					}
				case "clickhouse":
					val.clickHouseStr = true
					if m.clickhouse == nil {
						m.clickhouse = storage.GetClickHouse()
						m.wsClickHouseTickers = make(chan []storage.Ticker, 1)
						m.wsClickHouseTrades = make(chan []storage.Trade, 1)
					}
				case "s3":
					val.s3Str = true
					if m.s3 == nil {
						m.s3 = storage.GetS3()
						m.wsS3Tickers = make(chan []storage.Ticker, 1)
						m.wsS3Trades = make(chan []storage.Trade, 1)
					}
				}
			}

			// Channel id is used to identify channel in subscribe success message of websocket server.
			id++
			m.channelIds[id] = [2]string{market.ID, info.Channel}
			val.id = id

			val.mktCommitName = mktCommitName
			m.cfgMap[key] = val
		}
	}
	return nil
}

func (m *mexo) connectWs(ctx context.Context) error {
	ws, err := connector.NewWebsocket(ctx, &m.connCfg.WS, config.MexoWebsocketURL)
	if err != nil {
		if !errors.Is(err, ctx.Err()) {
			logErrStack(err)
		}
		return err
	}
	m.ws = ws
	log.Info().Str("exchange", "mexo").Msg("websocket connected")
	return nil
}

// closeWsConnOnError closes websocket connection if there is any error in app context.
// This will unblock all read and writes on websocket.
func (m *mexo) closeWsConnOnError(ctx context.Context) error {
	<-ctx.Done()
	err := m.ws.Conn.Close()
	if err != nil {
		return err
	}
	return ctx.Err()
}

// pingWs sends ping request to websocket server for every 1 minute.
func (m *mexo) pingWs(ctx context.Context) error {
	tick := time.NewTicker(1 * time.Minute)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			ping := make(map[string]int64, 1)
			ping["ping"] = time.Now().UTC().Unix()
			frame, err := jsoniter.Marshal(ping)
			if err != nil {
				logErrStack(err)
				return err
			}
			err = m.ws.Write(frame)
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
func (m *mexo) subWsChannel(market string, channel string) error {
	if channel == "ticker" {
		channel = "realtimes"
	}
	sub := wsSubMexo{
		Symbol: market,
		Topic:  channel,
		Event:  "sub",
	}
	sub.Params.Binary = false
	frame, err := jsoniter.Marshal(&sub)
	if err != nil {
		logErrStack(err)
		return err
	}
	err = m.ws.Write(frame)
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
func (m *mexo) readWs(ctx context.Context) error {

	// To avoid data race, creating a new local lookup map.
	cfgLookup := make(map[cfgLookupKey]cfgLookupVal, len(m.cfgMap))
	for k, v := range m.cfgMap {
		cfgLookup[k] = v
	}

	// See influxTimeVal struct doc for details.
	itv := influxTimeVal{}
	if m.influx != nil {
		itv.TickerMap = make(map[string]int64)
		itv.TradeMap = make(map[string]int64)
	}

	cd := commitData{
		terTickers:        make([]storage.Ticker, 0, m.connCfg.Terminal.TickerCommitBuf),
		terTrades:         make([]storage.Trade, 0, m.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers:      make([]storage.Ticker, 0, m.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:       make([]storage.Trade, 0, m.connCfg.MySQL.TradeCommitBuf),
		esTickers:         make([]storage.Ticker, 0, m.connCfg.ES.TickerCommitBuf),
		esTrades:          make([]storage.Trade, 0, m.connCfg.ES.TradeCommitBuf),
		influxTickers:     make([]storage.Ticker, 0, m.connCfg.InfluxDB.TickerCommitBuf),
		influxTrades:      make([]storage.Trade, 0, m.connCfg.InfluxDB.TradeCommitBuf),
		natsTickers:       make([]storage.Ticker, 0, m.connCfg.NATS.TickerCommitBuf),
		natsTrades:        make([]storage.Trade, 0, m.connCfg.NATS.TradeCommitBuf),
		clickHouseTickers: make([]storage.Ticker, 0, m.connCfg.ClickHouse.TickerCommitBuf),
		clickHouseTrades:  make([]storage.Trade, 0, m.connCfg.ClickHouse.TradeCommitBuf),
		s3Tickers:         make([]storage.Ticker, 0, m.connCfg.S3.TickerCommitBuf),
		s3Trades:          make([]storage.Trade, 0, m.connCfg.S3.TradeCommitBuf),
	}

	log.Debug().Str("exchange", "mexo").Str("func", "readWs").Msg("unlike other exchanges mexo does not send channel subscribed success message")

	for {
		select {
		default:
			frame, err := m.ws.Read()
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

			wr := wsRespMexo{}
			err = jsoniter.Unmarshal(frame, &wr)
			if err != nil {
				logErrStack(err)
				return err
			}

			if wr.Code != "" {
				log.Error().Str("exchange", "mexo").Str("func", "readWs").Str("code", wr.Code).Str("msg", wr.Desc).Msg("")
				return errors.New("mexo websocket error")
			}

			if wr.Topic == "realtimes" {
				wr.Topic = "ticker"
			}

			// Consider frame only in configured interval, otherwise ignore it.
			switch wr.Topic {
			case "ticker", "trade":
				key := cfgLookupKey{market: wr.Symbol, channel: wr.Topic}
				val := cfgLookup[key]
				if val.wsConsiderIntSec == 0 || time.Since(val.wsLastUpdated).Seconds() >= float64(val.wsConsiderIntSec) {
					val.wsLastUpdated = time.Now()
					wr.mktCommitName = val.mktCommitName
					cfgLookup[key] = val
				} else {
					continue
				}

				err := m.processWs(ctx, &wr, &cd, &itv)
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
func (m *mexo) processWs(ctx context.Context, wr *wsRespMexo, cd *commitData, itv *influxTimeVal) error {
	switch wr.Topic {
	case "ticker":
		ticker := storage.Ticker{}
		ticker.Exchange = "mexo"
		ticker.MktID = wr.Symbol
		ticker.MktCommitName = wr.mktCommitName

		price, err := strconv.ParseFloat(wr.Data[0].TickerPrice, 64)
		if err != nil {
			logErrStack(err)
			return err
		}
		ticker.Price = price
		ticker.Timestamp = time.Now().UTC()

		key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
		val := m.cfgMap[key]
		if val.terStr {
			cd.terTickersCount++
			cd.terTickers = append(cd.terTickers, ticker)
			if cd.terTickersCount == m.connCfg.Terminal.TickerCommitBuf {
				select {
				case m.wsTerTickers <- cd.terTickers:
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
			if cd.mysqlTickersCount == m.connCfg.MySQL.TickerCommitBuf {
				select {
				case m.wsMysqlTickers <- cd.mysqlTickers:
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
			if cd.esTickersCount == m.connCfg.ES.TickerCommitBuf {
				select {
				case m.wsEsTickers <- cd.esTickers:
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
			if cd.influxTickersCount == m.connCfg.InfluxDB.TickerCommitBuf {
				select {
				case m.wsInfluxTickers <- cd.influxTickers:
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
			if cd.natsTickersCount == m.connCfg.NATS.TickerCommitBuf {
				select {
				case m.wsNatsTickers <- cd.natsTickers:
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
			if cd.clickHouseTickersCount == m.connCfg.ClickHouse.TickerCommitBuf {
				select {
				case m.wsClickHouseTickers <- cd.clickHouseTickers:
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
			if cd.s3TickersCount == m.connCfg.S3.TickerCommitBuf {
				select {
				case m.wsS3Tickers <- cd.s3Tickers:
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
			trade.Exchange = "mexo"
			trade.MktID = wr.Symbol
			trade.MktCommitName = wr.mktCommitName
			trade.TradeID = data.TradeID

			if maker, ok := data.Maker.(bool); ok {
				if maker {
					trade.Side = "buy"
				} else {
					trade.Side = "sell"
				}
			} else {
				log.Error().Str("exchange", "mexo").Str("func", "processWs").Interface("maker", data.Maker).Msg("")
				return errors.New("cannot convert trade data field maker to bool")
			}

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

			// Time sent is in milliseconds int format for trade and string format for ticker.
			if timestamp, ok := data.Timestamp.(float64); ok {
				trade.Timestamp = time.Unix(0, int64(timestamp)*int64(time.Millisecond)).UTC()
			} else {
				log.Error().Str("exchange", "mexo").Str("func", "processWs").Interface("timestamp", data.Timestamp).Msg("")
				return errors.New("cannot convert trade data field timestamp to float")
			}

			key := cfgLookupKey{market: trade.MktID, channel: "trade"}
			val := m.cfgMap[key]
			if val.terStr {
				cd.terTradesCount++
				cd.terTrades = append(cd.terTrades, trade)
				if cd.terTradesCount == m.connCfg.Terminal.TradeCommitBuf {
					select {
					case m.wsTerTrades <- cd.terTrades:
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
				if cd.mysqlTradesCount == m.connCfg.MySQL.TradeCommitBuf {
					select {
					case m.wsMysqlTrades <- cd.mysqlTrades:
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
				if cd.esTradesCount == m.connCfg.ES.TradeCommitBuf {
					select {
					case m.wsEsTrades <- cd.esTrades:
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
				if cd.influxTradesCount == m.connCfg.InfluxDB.TradeCommitBuf {
					select {
					case m.wsInfluxTrades <- cd.influxTrades:
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
				if cd.natsTradesCount == m.connCfg.NATS.TradeCommitBuf {
					select {
					case m.wsNatsTrades <- cd.natsTrades:
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
				if cd.clickHouseTradesCount == m.connCfg.ClickHouse.TradeCommitBuf {
					select {
					case m.wsClickHouseTrades <- cd.clickHouseTrades:
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
				if cd.s3TradesCount == m.connCfg.S3.TradeCommitBuf {
					select {
					case m.wsS3Trades <- cd.s3Trades:
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

func (m *mexo) connectRest() error {
	rest, err := connector.GetREST()
	if err != nil {
		logErrStack(err)
		return err
	}
	m.rest = rest
	log.Info().Str("exchange", "mexo").Msg("REST connection setup is done")
	return nil
}

// processREST queries exchange for ticker / trade data through REST API in configured intervals,
// transforms it to a common ticker / trade store format,
// buffers the same in memory and
// then sends it to different storage systems for commit through go channels.
func (m *mexo) processREST(ctx context.Context, mktID string, mktCommitName string, channel string, interval int) error {
	var (
		req *http.Request
		q   url.Values
		err error

		// See influxTimeVal (exchange.go) struct doc for details.
		influxTickerTime int64
		influxTradeTime  int64
	)

	cd := commitData{
		terTickers:        make([]storage.Ticker, 0, m.connCfg.Terminal.TickerCommitBuf),
		terTrades:         make([]storage.Trade, 0, m.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers:      make([]storage.Ticker, 0, m.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:       make([]storage.Trade, 0, m.connCfg.MySQL.TradeCommitBuf),
		esTickers:         make([]storage.Ticker, 0, m.connCfg.ES.TickerCommitBuf),
		esTrades:          make([]storage.Trade, 0, m.connCfg.ES.TradeCommitBuf),
		influxTickers:     make([]storage.Ticker, 0, m.connCfg.InfluxDB.TickerCommitBuf),
		influxTrades:      make([]storage.Trade, 0, m.connCfg.InfluxDB.TradeCommitBuf),
		natsTickers:       make([]storage.Ticker, 0, m.connCfg.NATS.TickerCommitBuf),
		natsTrades:        make([]storage.Trade, 0, m.connCfg.NATS.TradeCommitBuf),
		clickHouseTickers: make([]storage.Ticker, 0, m.connCfg.ClickHouse.TickerCommitBuf),
		clickHouseTrades:  make([]storage.Trade, 0, m.connCfg.ClickHouse.TradeCommitBuf),
		s3Tickers:         make([]storage.Ticker, 0, m.connCfg.S3.TickerCommitBuf),
		s3Trades:          make([]storage.Trade, 0, m.connCfg.S3.TradeCommitBuf),
	}

	switch channel {
	case "ticker":
		req, err = m.rest.Request(ctx, "GET", config.MexoRESTBaseURL+"quote/v1/ticker/price")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)
	case "trade":
		req, err = m.rest.Request(ctx, "GET", config.MexoRESTBaseURL+"quote/v1/trades")
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
				resp, err := m.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := restRespMexo{}
				if err = jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				price, err := strconv.ParseFloat(rr.Price, 64)
				if err != nil {
					logErrStack(err)
					return err
				}

				ticker := storage.Ticker{
					Exchange:      "mexo",
					MktID:         mktID,
					MktCommitName: mktCommitName,
					Price:         price,
					Timestamp:     time.Now().UTC(),
				}

				key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
				val := m.cfgMap[key]
				if val.terStr {
					cd.terTickersCount++
					cd.terTickers = append(cd.terTickers, ticker)
					if cd.terTickersCount == m.connCfg.Terminal.TickerCommitBuf {
						err := m.ter.CommitTickers(ctx, cd.terTickers)
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
					if cd.mysqlTickersCount == m.connCfg.MySQL.TickerCommitBuf {
						err := m.mysql.CommitTickers(ctx, cd.mysqlTickers)
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
					if cd.esTickersCount == m.connCfg.ES.TickerCommitBuf {
						err := m.es.CommitTickers(ctx, cd.esTickers)
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
					if cd.influxTickersCount == m.connCfg.InfluxDB.TickerCommitBuf {
						err := m.influx.CommitTickers(ctx, cd.influxTickers)
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
					if cd.natsTickersCount == m.connCfg.NATS.TickerCommitBuf {
						err := m.nats.CommitTickers(ctx, cd.natsTickers)
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
					if cd.clickHouseTickersCount == m.connCfg.ClickHouse.TickerCommitBuf {
						err := m.clickhouse.CommitTickers(ctx, cd.clickHouseTickers)
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
					if cd.s3TickersCount == m.connCfg.S3.TickerCommitBuf {
						err := m.s3.CommitTickers(ctx, cd.s3Tickers)
						if err != nil {
							return err
						}
						cd.s3TickersCount = 0
						cd.s3Tickers = nil
					}
				}
			case "trade":
				req.URL.RawQuery = q.Encode()
				resp, err := m.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := []restRespMexo{}
				if err := jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				for i := range rr {
					r := rr[i]
					var side string
					if r.Maker {
						side = "buy"
					} else {
						side = "sell"
					}

					size, err := strconv.ParseFloat(r.Qty, 64)
					if err != nil {
						logErrStack(err)
						return err
					}

					price, err := strconv.ParseFloat(r.Price, 64)
					if err != nil {
						logErrStack(err)
						return err
					}

					// Time sent is in milliseconds.
					timestamp := time.Unix(0, r.Time*int64(time.Millisecond)).UTC()

					trade := storage.Trade{
						Exchange:      "mexo",
						MktID:         mktID,
						MktCommitName: mktCommitName,
						Side:          side,
						Size:          size,
						Price:         price,
						Timestamp:     timestamp,
					}

					key := cfgLookupKey{market: trade.MktID, channel: "trade"}
					val := m.cfgMap[key]
					if val.terStr {
						cd.terTradesCount++
						cd.terTrades = append(cd.terTrades, trade)
						if cd.terTradesCount == m.connCfg.Terminal.TradeCommitBuf {
							err := m.ter.CommitTrades(ctx, cd.terTrades)
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
						if cd.mysqlTradesCount == m.connCfg.MySQL.TradeCommitBuf {
							err := m.mysql.CommitTrades(ctx, cd.mysqlTrades)
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
						if cd.esTradesCount == m.connCfg.ES.TradeCommitBuf {
							err := m.es.CommitTrades(ctx, cd.esTrades)
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
						if cd.influxTradesCount == m.connCfg.InfluxDB.TradeCommitBuf {
							err := m.influx.CommitTrades(ctx, cd.influxTrades)
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
						if cd.natsTradesCount == m.connCfg.NATS.TradeCommitBuf {
							err := m.nats.CommitTrades(ctx, cd.natsTrades)
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
						if cd.clickHouseTradesCount == m.connCfg.ClickHouse.TradeCommitBuf {
							err := m.clickhouse.CommitTrades(ctx, cd.clickHouseTrades)
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
						if cd.s3TradesCount == m.connCfg.S3.TradeCommitBuf {
							err := m.s3.CommitTrades(ctx, cd.s3Trades)
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
