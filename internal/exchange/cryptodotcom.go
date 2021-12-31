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

// StartCryptodotCom is for starting cryptodot-com exchange functions.
func StartCryptodotCom(appCtx context.Context, markets []config.Market, retry *config.Retry, connCfg *config.Connection) error {

	// If any error occurs or connection is lost, retry the exchange functions with a time gap till it reaches
	// a configured number of retry.
	// Retry counter will be reset back to zero if the elapsed time since the last retry is greater than the configured one.
	var retryCount int
	lastRetryTime := time.Now()

	for {
		err := newCryptodotCom(appCtx, markets, connCfg)
		if err != nil {
			log.Error().Err(err).Str("exchange", "cryptodot-com").Msg("error occurred")
			if retry.Number == 0 {
				return errors.New("not able to connect cryptodot-com exchange. please check the log for details")
			}
			if retry.ResetSec == 0 || time.Since(lastRetryTime).Seconds() < float64(retry.ResetSec) {
				retryCount++
			} else {
				retryCount = 1
			}
			lastRetryTime = time.Now()
			if retryCount > retry.Number {
				err = fmt.Errorf("not able to connect cryptodot-com exchange even after %d retry", retry.Number)
				log.Error().Err(err).Str("exchange", "cryptodot-com").Msg("")
				return err
			}

			log.Error().Str("exchange", "cryptodot-com").Int("retry", retryCount).Msg(fmt.Sprintf("retrying functions in %d seconds", retry.GapSec))
			tick := time.NewTicker(time.Duration(retry.GapSec) * time.Second)
			select {
			case <-tick.C:
				tick.Stop()

			// Return, if there is any error from another exchange.
			case <-appCtx.Done():
				log.Error().Str("exchange", "cryptodot-com").Msg("ctx canceled, return from StartCryptodotCom")
				return appCtx.Err()
			}
		}
	}
}

type cryptodotCom struct {
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

type wsSubCryptoCom struct {
	ID     int                 `json:"id"`
	Method string              `json:"method"`
	Params wsSubParamCryptoDot `json:"params"`
	Nonce  int64               `json:"nonce"`
}

type wsSubParamCryptoDot struct {
	Channels [1]string `json:"channels"`
}

type restTickerCryptoCom struct {
	Result restTickerResultCryptoCom `json:"result"`
}

type respCryptoCom struct {
	Result        respResultCryptoCom `json:"result"`
	ID            int                 `json:"id"`
	Method        string              `json:"method"`
	Code          int                 `json:"code"`
	Message       string              `json:"message"`
	mktCommitName string
}

type restTickerResultCryptoCom struct {
	Data respDataCryptoCom `json:"data"`
}

type respResultCryptoCom struct {
	InstrumentName string              `json:"instrument_name"`
	Channel        string              `json:"channel"`
	Data           []respDataCryptoCom `json:"data"`
}

type respDataCryptoCom struct {
	TradeID     uint64  `json:"d"`
	Side        string  `json:"s"`
	Qty         float64 `json:"q"`
	TickerPrice float64 `json:"a"`
	TradePrice  float64 `json:"p"`
	Time        int64   `json:"t"`
}

func newCryptodotCom(appCtx context.Context, markets []config.Market, connCfg *config.Connection) error {

	// If any exchange function fails, force all the other functions to stop and return.
	cryptodotComErrGroup, ctx := errgroup.WithContext(appCtx)

	c := cryptodotCom{connCfg: connCfg}

	err := c.cfgLookup(markets)
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

					err = c.connectWs(ctx)
					if err != nil {
						return err
					}

					cryptodotComErrGroup.Go(func() error {
						return c.closeWsConnOnError(ctx)
					})

					cryptodotComErrGroup.Go(func() error {
						return c.readWs(ctx)
					})

					if c.ter != nil {
						cryptodotComErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, c.ter, c.wsTerTickers)
						})
						cryptodotComErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, c.ter, c.wsTerTrades)
						})
					}

					if c.mysql != nil {
						cryptodotComErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, c.mysql, c.wsMysqlTickers)
						})
						cryptodotComErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, c.mysql, c.wsMysqlTrades)
						})
					}

					if c.es != nil {
						cryptodotComErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, c.es, c.wsEsTickers)
						})
						cryptodotComErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, c.es, c.wsEsTrades)
						})
					}

					if c.influx != nil {
						cryptodotComErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, c.influx, c.wsInfluxTickers)
						})
						cryptodotComErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, c.influx, c.wsInfluxTrades)
						})
					}

					if c.nats != nil {
						cryptodotComErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, c.nats, c.wsNatsTickers)
						})
						cryptodotComErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, c.nats, c.wsNatsTrades)
						})
					}

					if c.clickhouse != nil {
						cryptodotComErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, c.clickhouse, c.wsClickHouseTickers)
						})
						cryptodotComErrGroup.Go(func() error {
							return WsTradesToStorage(ctx, c.clickhouse, c.wsClickHouseTrades)
						})
					}

					if c.s3 != nil {
						cryptodotComErrGroup.Go(func() error {
							return WsTickersToStorage(ctx, c.s3, c.wsS3Tickers)
						})
						cryptodotComErrGroup.Go(func() error {
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

				// As suggested by the exchange.
				time.Sleep(1 * time.Second)

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
				cryptodotComErrGroup.Go(func() error {
					return c.processREST(ctx, mktID, mktCommitName, channel, restPingIntSec)
				})

				restCount++
			}
		}
	}

	err = cryptodotComErrGroup.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (c *cryptodotCom) cfgLookup(markets []config.Market) error {
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

func (c *cryptodotCom) connectWs(ctx context.Context) error {
	ws, err := connector.NewWebsocket(ctx, &c.connCfg.WS, config.CryptodotComWebsocketURL)
	if err != nil {
		if !errors.Is(err, ctx.Err()) {
			logErrStack(err)
		}
		return err
	}
	c.ws = ws
	log.Info().Str("exchange", "cryptodot-com").Msg("websocket connected")
	return nil
}

// closeWsConnOnError closes websocket connection if there is any error in app context.
// This will unblock all read and writes on websocket.
func (c *cryptodotCom) closeWsConnOnError(ctx context.Context) error {
	<-ctx.Done()
	err := c.ws.Conn.Close()
	if err != nil {
		return err
	}
	return ctx.Err()
}

// heartBeatWs sends heartbeat response to websocket server upon receiving it.
func (c *cryptodotCom) heartBeatWs(heartBeatID int) error {
	sub := wsSubCryptoCom{
		ID:     heartBeatID,
		Method: "public/respond-heartbeat",
	}
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

// subWsChannel sends channel subscription requests to the websocket server.
func (c *cryptodotCom) subWsChannel(market string, channel string, id int) error {
	sub := wsSubCryptoCom{
		ID:     id,
		Method: "subscribe",
		Nonce:  time.Now().UTC().UnixMilli(),
	}
	sub.Params.Channels = [1]string{channel + "." + market}
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
func (c *cryptodotCom) readWs(ctx context.Context) error {

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

			wr := respCryptoCom{}
			err = jsoniter.Unmarshal(frame, &wr)
			if err != nil {
				logErrStack(err)
				return err
			}

			if wr.Code == 0 && wr.Method == "public/heartbeat" {
				err = c.heartBeatWs(wr.ID)
				if err != nil {
					return err
				}
				continue
			}

			if wr.Code == 0 && wr.ID != 0 {
				log.Debug().Str("exchange", "cryptodot-com").Str("func", "readWs").Str("market", c.channelIds[wr.ID][0]).Str("channel", c.channelIds[wr.ID][1]).Msg("channel subscribed")
				continue
			}

			if wr.Code != 0 && wr.Message != "" {
				log.Error().Str("exchange", "cryptodot-com").Str("func", "readWs").Int("code", wr.Code).Str("msg", wr.Message).Msg("")
				return errors.New("cryptodot-com websocket error")
			}

			if wr.Result.Channel == "ticker" || wr.Result.Channel == "trade" {
				key := cfgLookupKey{market: wr.Result.InstrumentName, channel: wr.Result.Channel}
				val := cfgLookup[key]
				if val.wsConsiderIntSec == 0 || time.Since(val.wsLastUpdated).Seconds() >= float64(val.wsConsiderIntSec) {
					val.wsLastUpdated = time.Now()
					wr.mktCommitName = val.mktCommitName
					cfgLookup[key] = val
				} else {
					continue
				}

				err = c.processWs(ctx, &wr, &cd, &itv)
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
func (c *cryptodotCom) processWs(ctx context.Context, wr *respCryptoCom, cd *commitData, itv *influxTimeVal) error {
	switch wr.Result.Channel {
	case "ticker":
		ticker := storage.Ticker{}
		ticker.Exchange = "cryptodot-com"
		ticker.MktID = wr.Result.InstrumentName
		ticker.MktCommitName = wr.mktCommitName
		ticker.Price = wr.Result.Data[0].TickerPrice

		// Time sent is in milliseconds.
		timestamp := time.Unix(0, wr.Result.Data[0].Time*int64(time.Millisecond)).UTC()
		ticker.Timestamp = timestamp

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
		for _, data := range wr.Result.Data {
			trade := storage.Trade{}
			trade.Exchange = "cryptodot-com"
			trade.MktID = wr.Result.InstrumentName
			trade.MktCommitName = wr.mktCommitName
			trade.TradeID = strconv.FormatUint(data.TradeID, 10)
			trade.Side = strings.ToLower(data.Side)
			trade.Size = data.Qty
			trade.Price = data.TradePrice

			// Time sent is in milliseconds.
			timestamp := time.Unix(0, wr.Result.Data[0].Time*int64(time.Millisecond)).UTC()
			trade.Timestamp = timestamp

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

func (c *cryptodotCom) connectRest() error {
	rest, err := connector.GetREST()
	if err != nil {
		logErrStack(err)
		return err
	}
	c.rest = rest
	log.Info().Str("exchange", "cryptodot-com").Msg("REST connection setup is done")
	return nil
}

// processREST queries exchange for ticker / trade data through REST API in configured intervals,
// transforms it to a common ticker / trade store format,
// buffers the same in memory and
// then sends it to different storage systems for commit through go channels.
func (c *cryptodotCom) processREST(ctx context.Context, mktID string, mktCommitName string, channel string, interval int) error {
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
		req, err = c.rest.Request(ctx, "GET", config.CryptodotComRESTBaseURL+"get-ticker")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
		q = req.URL.Query()
		q.Add("instrument_name", mktID)
	case "trade":
		req, err = c.rest.Request(ctx, "GET", config.CryptodotComRESTBaseURL+"get-trades")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
		q = req.URL.Query()
		q.Add("instrument_name", mktID)

		// Querying for trades in random fashion.
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
				resp, err := c.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := restTickerCryptoCom{}
				if err = jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				ticker := storage.Ticker{
					Exchange:      "cryptodot-com",
					MktID:         mktID,
					MktCommitName: mktCommitName,
					Price:         rr.Result.Data.TickerPrice,
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

				rr := respCryptoCom{}
				if err := jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				for i := range rr.Result.Data {
					r := rr.Result.Data[i]

					// Time sent is in milliseconds.
					timestamp := time.Unix(0, r.Time*int64(time.Millisecond)).UTC()

					trade := storage.Trade{
						Exchange:      "cryptodot-com",
						MktID:         mktID,
						MktCommitName: mktCommitName,
						TradeID:       strconv.FormatUint(r.TradeID, 10),
						Side:          strings.ToLower(r.Side),
						Size:          r.Qty,
						Price:         r.TradePrice,
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
