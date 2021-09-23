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

// StartGemini is for starting gemini exchange functions.
func StartGemini(appCtx context.Context, markets []config.Market, retry *config.Retry, connCfg *config.Connection) error {

	// If any error occurs or connection is lost, retry the exchange functions with a time gap till it reaches
	// a configured number of retry.
	// Retry counter will be reset back to zero if the elapsed time since the last retry is greater than the configured one.
	var retryCount int
	lastRetryTime := time.Now()

	for {
		err := newGemini(appCtx, markets, connCfg)
		if err != nil {
			log.Error().Err(err).Str("exchange", "gemini").Msg("error occurred")
			if retry.Number == 0 {
				return errors.New("not able to connect gemini exchange. please check the log for details")
			}
			if retry.ResetSec == 0 || time.Since(lastRetryTime).Seconds() < float64(retry.ResetSec) {
				retryCount++
			} else {
				retryCount = 1
			}
			lastRetryTime = time.Now()
			if retryCount > retry.Number {
				err = fmt.Errorf("not able to connect gemini exchange even after %d retry", retry.Number)
				log.Error().Err(err).Str("exchange", "gemini").Msg("")
				return err
			}

			log.Error().Str("exchange", "gemini").Int("retry", retryCount).Msg(fmt.Sprintf("retrying functions in %d seconds", retry.GapSec))
			tick := time.NewTicker(time.Duration(retry.GapSec) * time.Second)
			select {
			case <-tick.C:
				tick.Stop()

			// Return, if there is any error from another exchange.
			case <-appCtx.Done():
				log.Error().Str("exchange", "gemini").Msg("ctx canceled, return from StartGemini")
				return appCtx.Err()
			}
		}
	}
}

type gemini struct {
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

type wsSubGemini struct {
	Type          string           `json:"type"`
	Subscriptions []wsSubSubGemini `json:"subscriptions"`
}

type wsSubSubGemini struct {
	Name    string    `json:"name"`
	Symbols [1]string `json:"symbols"`
}

type wsRespGemini struct {
	Type          string `json:"type"`
	Symbol        string `json:"symbol"`
	EventID       uint64 `json:"event_id"`
	Side          string `json:"side"`
	Quantity      string `json:"quantity"`
	Price         string `json:"price"`
	Timestamp     int64  `json:"timestamp"`
	mktCommitName string
}

type restRespGemini struct {
	Symbol      string `json:"symbol"`
	TradeID     uint64 `json:"tid"`
	Type        string `json:"type"`
	Amount      string `json:"amount"`
	Price       string `json:"price"`
	Timestamp   int64  `json:"timestampms"`
	TickerPrice string `json:"last"`
}

func newGemini(appCtx context.Context, markets []config.Market, connCfg *config.Connection) error {

	// If any exchange function fails, force all the other functions to stop and return.
	geminiErrGroup, ctx := errgroup.WithContext(appCtx)

	g := gemini{connCfg: connCfg}

	err := g.cfgLookup(markets)
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

					err = g.connectWs(ctx)
					if err != nil {
						return err
					}

					geminiErrGroup.Go(func() error {
						return g.closeWsConnOnError(ctx)
					})

					geminiErrGroup.Go(func() error {
						return g.readWs(ctx)
					})

					if g.ter != nil {
						geminiErrGroup.Go(func() error {
							return g.wsTickersToTerminal(ctx)
						})
						geminiErrGroup.Go(func() error {
							return g.wsTradesToTerminal(ctx)
						})
					}

					if g.mysql != nil {
						geminiErrGroup.Go(func() error {
							return g.wsTickersToMySQL(ctx)
						})
						geminiErrGroup.Go(func() error {
							return g.wsTradesToMySQL(ctx)
						})
					}

					if g.es != nil {
						geminiErrGroup.Go(func() error {
							return g.wsTickersToES(ctx)
						})
						geminiErrGroup.Go(func() error {
							return g.wsTradesToES(ctx)
						})
					}

					if g.influx != nil {
						geminiErrGroup.Go(func() error {
							return g.wsTickersToInflux(ctx)
						})
						geminiErrGroup.Go(func() error {
							return g.wsTradesToInflux(ctx)
						})
					}

					if g.nats != nil {
						geminiErrGroup.Go(func() error {
							return g.wsTickersToNats(ctx)
						})
						geminiErrGroup.Go(func() error {
							return g.wsTradesToNats(ctx)
						})
					}

					if g.clickhouse != nil {
						geminiErrGroup.Go(func() error {
							return g.wsTickersToClickHouse(ctx)
						})
						geminiErrGroup.Go(func() error {
							return g.wsTradesToClickHouse(ctx)
						})
					}

					if g.s3 != nil {
						geminiErrGroup.Go(func() error {
							return g.wsTickersToS3(ctx)
						})
						geminiErrGroup.Go(func() error {
							return g.wsTradesToS3(ctx)
						})
					}
				}

				// There is only one channel provided for both ticker and trade data,
				// so need to subscribe only once.
				_, pres := subChannels[market.ID]
				if !pres {
					err = g.subWsChannel(market.ID)
					if err != nil {
						return err
					}
					subChannels[market.ID] = true
				}

				wsCount++
			case "rest":
				if restCount == 0 {
					err = g.connectRest()
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
				geminiErrGroup.Go(func() error {
					return g.processREST(ctx, mktID, mktCommitName, channel, restPingIntSec)
				})

				restCount++
			}
		}
	}

	err = geminiErrGroup.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (g *gemini) cfgLookup(markets []config.Market) error {

	// Configurations flat map is prepared for easy lookup later in the app.
	g.cfgMap = make(map[cfgLookupKey]cfgLookupVal)
	for _, market := range markets {
		var marketCommitName string
		if market.CommitName != "" {
			marketCommitName = market.CommitName
		} else {
			marketCommitName = market.ID
		}

		// Capitalizing market ID as there is no proper nomenclature mentioned in the exchange API doc for symbols.
		marketID := strings.ToUpper(market.ID)

		for _, info := range market.Info {
			key := cfgLookupKey{market: marketID, channel: info.Channel}
			val := cfgLookupVal{}
			val.connector = info.Connector
			val.wsConsiderIntSec = info.WsConsiderIntSec
			for _, str := range info.Storages {
				switch str {
				case "terminal":
					val.terStr = true
					if g.ter == nil {
						g.ter = storage.GetTerminal()
						g.wsTerTickers = make(chan []storage.Ticker, 1)
						g.wsTerTrades = make(chan []storage.Trade, 1)
					}
				case "mysql":
					val.mysqlStr = true
					if g.mysql == nil {
						g.mysql = storage.GetMySQL()
						g.wsMysqlTickers = make(chan []storage.Ticker, 1)
						g.wsMysqlTrades = make(chan []storage.Trade, 1)
					}
				case "elastic_search":
					val.esStr = true
					if g.es == nil {
						g.es = storage.GetElasticSearch()
						g.wsEsTickers = make(chan []storage.Ticker, 1)
						g.wsEsTrades = make(chan []storage.Trade, 1)
					}
				case "influxdb":
					val.influxStr = true
					if g.influx == nil {
						g.influx = storage.GetInfluxDB()
						g.wsInfluxTickers = make(chan []storage.Ticker, 1)
						g.wsInfluxTrades = make(chan []storage.Trade, 1)
					}
				case "nats":
					val.natsStr = true
					if g.nats == nil {
						g.nats = storage.GetNATS()
						g.wsNatsTickers = make(chan []storage.Ticker, 1)
						g.wsNatsTrades = make(chan []storage.Trade, 1)
					}
				case "clickhouse":
					val.clickHouseStr = true
					if g.clickhouse == nil {
						g.clickhouse = storage.GetClickHouse()
						g.wsClickHouseTickers = make(chan []storage.Ticker, 1)
						g.wsClickHouseTrades = make(chan []storage.Trade, 1)
					}
				case "s3":
					val.s3Str = true
					if g.s3 == nil {
						g.s3 = storage.GetS3()
						g.wsS3Tickers = make(chan []storage.Ticker, 1)
						g.wsS3Trades = make(chan []storage.Trade, 1)
					}
				}
			}
			val.mktCommitName = marketCommitName
			g.cfgMap[key] = val
		}
	}
	return nil
}

func (g *gemini) connectWs(ctx context.Context) error {
	ws, err := connector.NewWebsocket(ctx, &g.connCfg.WS, config.GeminiWebsocketURL)
	if err != nil {
		if !errors.Is(err, ctx.Err()) {
			logErrStack(err)
		}
		return err
	}
	g.ws = ws
	log.Info().Str("exchange", "gemini").Msg("websocket connected")
	return nil
}

// closeWsConnOnError closes websocket connection if there is any error in app context.
// This will unblock all read and writes on websocket.
func (g *gemini) closeWsConnOnError(ctx context.Context) error {
	<-ctx.Done()
	err := g.ws.Conn.Close()
	if err != nil {
		return err
	}
	return ctx.Err()
}

// subWsChannel sends channel subscription requests to the websocket server.
func (g *gemini) subWsChannel(market string) error {
	channels := make([]wsSubSubGemini, 1)
	channels[0].Name = "l2"
	channels[0].Symbols = [1]string{market}
	sub := wsSubGemini{
		Type:          "subscribe",
		Subscriptions: channels,
	}
	frame, err := jsoniter.Marshal(sub)
	if err != nil {
		logErrStack(err)
		return err
	}
	err = g.ws.Write(frame)
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
func (g *gemini) readWs(ctx context.Context) error {

	// To avoid data race, creating a new local lookup map.
	cfgLookup := make(map[cfgLookupKey]cfgLookupVal, len(g.cfgMap))
	for k, v := range g.cfgMap {
		cfgLookup[k] = v
	}

	// See influxTimeVal struct doc for details.
	itv := influxTimeVal{}
	if g.influx != nil {
		itv.TickerMap = make(map[string]int64)
		itv.TradeMap = make(map[string]int64)
	}

	cd := commitData{
		terTickers:        make([]storage.Ticker, 0, g.connCfg.Terminal.TickerCommitBuf),
		terTrades:         make([]storage.Trade, 0, g.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers:      make([]storage.Ticker, 0, g.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:       make([]storage.Trade, 0, g.connCfg.MySQL.TradeCommitBuf),
		esTickers:         make([]storage.Ticker, 0, g.connCfg.ES.TickerCommitBuf),
		esTrades:          make([]storage.Trade, 0, g.connCfg.ES.TradeCommitBuf),
		influxTickers:     make([]storage.Ticker, 0, g.connCfg.InfluxDB.TickerCommitBuf),
		influxTrades:      make([]storage.Trade, 0, g.connCfg.InfluxDB.TradeCommitBuf),
		natsTickers:       make([]storage.Ticker, 0, g.connCfg.NATS.TickerCommitBuf),
		natsTrades:        make([]storage.Trade, 0, g.connCfg.NATS.TradeCommitBuf),
		clickHouseTickers: make([]storage.Ticker, 0, g.connCfg.ClickHouse.TickerCommitBuf),
		clickHouseTrades:  make([]storage.Trade, 0, g.connCfg.ClickHouse.TradeCommitBuf),
		s3Tickers:         make([]storage.Ticker, 0, g.connCfg.S3.TickerCommitBuf),
		s3Trades:          make([]storage.Trade, 0, g.connCfg.S3.TradeCommitBuf),
	}

	log.Debug().Str("exchange", "gemini").Str("func", "readWs").Msg("unlike other exchanges gemini does not send channel subscribed success message")

	for {
		select {
		default:
			frame, err := g.ws.Read()
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

			wr := wsRespGemini{}
			err = jsoniter.Unmarshal(frame, &wr)
			if err != nil {
				logErrStack(err)
				return err
			}

			if wr.Type == "trade" {

				// There is only one channel provided for both ticker and trade data,
				// so need to duplicate it manually if there is a subscription to both the channels by user.

				// Ticker.
				key := cfgLookupKey{market: strings.ToUpper(wr.Symbol), channel: "ticker"}
				val, pres := cfgLookup[key]
				if pres && val.connector == "websocket" && (val.wsConsiderIntSec == 0 || time.Since(val.wsLastUpdated).Seconds() >= float64(val.wsConsiderIntSec)) {

					// Consider frame only in configured interval, otherwise ignore it.
					val.wsLastUpdated = time.Now()
					wr.mktCommitName = val.mktCommitName
					wr.Type = "ticker"
					cfgLookup[key] = val

					err := g.processWs(ctx, &wr, &cd, &itv)
					if err != nil {
						return err
					}
				}

				// Trade.
				key = cfgLookupKey{market: strings.ToUpper(wr.Symbol), channel: "trade"}
				val, pres = cfgLookup[key]
				if pres && val.connector == "websocket" && (val.wsConsiderIntSec == 0 || time.Since(val.wsLastUpdated).Seconds() >= float64(val.wsConsiderIntSec)) {

					// Consider frame only in configured interval, otherwise ignore it.
					val.wsLastUpdated = time.Now()
					wr.mktCommitName = val.mktCommitName
					wr.Type = "trade"
					cfgLookup[key] = val

					err := g.processWs(ctx, &wr, &cd, &itv)
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
func (g *gemini) processWs(ctx context.Context, wr *wsRespGemini, cd *commitData, itv *influxTimeVal) error {
	switch wr.Type {
	case "ticker":
		ticker := storage.Ticker{}
		ticker.Exchange = "gemini"
		ticker.MktID = wr.Symbol
		ticker.MktCommitName = wr.mktCommitName

		price, err := strconv.ParseFloat(wr.Price, 64)
		if err != nil {
			logErrStack(err)
			return err
		}
		ticker.Price = price

		ticker.Timestamp = time.Now().UTC()

		key := cfgLookupKey{market: strings.ToUpper(ticker.MktID), channel: "ticker"}
		val := g.cfgMap[key]
		if val.terStr {
			cd.terTickersCount++
			cd.terTickers = append(cd.terTickers, ticker)
			if cd.terTickersCount == g.connCfg.Terminal.TickerCommitBuf {
				select {
				case g.wsTerTickers <- cd.terTickers:
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
			if cd.mysqlTickersCount == g.connCfg.MySQL.TickerCommitBuf {
				select {
				case g.wsMysqlTickers <- cd.mysqlTickers:
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
			if cd.esTickersCount == g.connCfg.ES.TickerCommitBuf {
				select {
				case g.wsEsTickers <- cd.esTickers:
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
			if cd.influxTickersCount == g.connCfg.InfluxDB.TickerCommitBuf {
				select {
				case g.wsInfluxTickers <- cd.influxTickers:
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
			if cd.natsTickersCount == g.connCfg.NATS.TickerCommitBuf {
				select {
				case g.wsNatsTickers <- cd.natsTickers:
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
			if cd.clickHouseTickersCount == g.connCfg.ClickHouse.TickerCommitBuf {
				select {
				case g.wsClickHouseTickers <- cd.clickHouseTickers:
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
			if cd.s3TickersCount == g.connCfg.S3.TickerCommitBuf {
				select {
				case g.wsS3Tickers <- cd.s3Tickers:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.s3TickersCount = 0
				cd.s3Tickers = nil
			}
		}
	case "trade":
		trade := storage.Trade{}
		trade.Exchange = "gemini"
		trade.MktID = wr.Symbol
		trade.MktCommitName = wr.mktCommitName
		trade.TradeID = strconv.FormatUint(wr.EventID, 10)
		trade.Side = wr.Side

		size, err := strconv.ParseFloat(wr.Quantity, 64)
		if err != nil {
			logErrStack(err)
			return err
		}
		trade.Size = size

		price, err := strconv.ParseFloat(wr.Price, 64)
		if err != nil {
			logErrStack(err)
			return err
		}
		trade.Price = price

		// Time sent is in milliseconds.
		trade.Timestamp = time.Unix(0, wr.Timestamp*int64(time.Millisecond)).UTC()

		key := cfgLookupKey{market: strings.ToUpper(trade.MktID), channel: "trade"}
		val := g.cfgMap[key]
		if val.terStr {
			cd.terTradesCount++
			cd.terTrades = append(cd.terTrades, trade)
			if cd.terTradesCount == g.connCfg.Terminal.TradeCommitBuf {
				select {
				case g.wsTerTrades <- cd.terTrades:
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
			if cd.mysqlTradesCount == g.connCfg.MySQL.TradeCommitBuf {
				select {
				case g.wsMysqlTrades <- cd.mysqlTrades:
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
			if cd.esTradesCount == g.connCfg.ES.TradeCommitBuf {
				select {
				case g.wsEsTrades <- cd.esTrades:
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
			if cd.influxTradesCount == g.connCfg.InfluxDB.TradeCommitBuf {
				select {
				case g.wsInfluxTrades <- cd.influxTrades:
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
			if cd.natsTradesCount == g.connCfg.NATS.TradeCommitBuf {
				select {
				case g.wsNatsTrades <- cd.natsTrades:
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
			if cd.clickHouseTradesCount == g.connCfg.ClickHouse.TradeCommitBuf {
				select {
				case g.wsClickHouseTrades <- cd.clickHouseTrades:
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
			if cd.s3TradesCount == g.connCfg.S3.TradeCommitBuf {
				select {
				case g.wsS3Trades <- cd.s3Trades:
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

func (g *gemini) wsTickersToTerminal(ctx context.Context) error {
	for {
		select {
		case data := <-g.wsTerTickers:
			g.ter.CommitTickers(data)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (g *gemini) wsTradesToTerminal(ctx context.Context) error {
	for {
		select {
		case data := <-g.wsTerTrades:
			g.ter.CommitTrades(data)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (g *gemini) wsTickersToMySQL(ctx context.Context) error {
	for {
		select {
		case data := <-g.wsMysqlTickers:
			err := g.mysql.CommitTickers(ctx, data)
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

func (g *gemini) wsTradesToMySQL(ctx context.Context) error {
	for {
		select {
		case data := <-g.wsMysqlTrades:
			err := g.mysql.CommitTrades(ctx, data)
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

func (g *gemini) wsTickersToES(ctx context.Context) error {
	for {
		select {
		case data := <-g.wsEsTickers:
			err := g.es.CommitTickers(ctx, data)
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

func (g *gemini) wsTradesToES(ctx context.Context) error {
	for {
		select {
		case data := <-g.wsEsTrades:
			err := g.es.CommitTrades(ctx, data)
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

func (g *gemini) wsTickersToInflux(ctx context.Context) error {
	for {
		select {
		case data := <-g.wsInfluxTickers:
			err := g.influx.CommitTickers(ctx, data)
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

func (g *gemini) wsTradesToInflux(ctx context.Context) error {
	for {
		select {
		case data := <-g.wsInfluxTrades:
			err := g.influx.CommitTrades(ctx, data)
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

func (g *gemini) wsTickersToNats(ctx context.Context) error {
	for {
		select {
		case data := <-g.wsNatsTickers:
			err := g.nats.CommitTickers(data)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (g *gemini) wsTradesToNats(ctx context.Context) error {
	for {
		select {
		case data := <-g.wsNatsTrades:
			err := g.nats.CommitTrades(data)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (g *gemini) wsTickersToClickHouse(ctx context.Context) error {
	for {
		select {
		case data := <-g.wsClickHouseTickers:
			err := g.clickhouse.CommitTickers(ctx, data)
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

func (g *gemini) wsTradesToClickHouse(ctx context.Context) error {
	for {
		select {
		case data := <-g.wsClickHouseTrades:
			err := g.clickhouse.CommitTrades(ctx, data)
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

func (g *gemini) wsTickersToS3(ctx context.Context) error {
	for {
		select {
		case data := <-g.wsS3Tickers:
			err := g.s3.CommitTickers(ctx, data)
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

func (g *gemini) wsTradesToS3(ctx context.Context) error {
	for {
		select {
		case data := <-g.wsS3Trades:
			err := g.s3.CommitTrades(ctx, data)
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

func (g *gemini) connectRest() error {
	rest, err := connector.GetREST()
	if err != nil {
		logErrStack(err)
		return err
	}
	g.rest = rest
	log.Info().Str("exchange", "gemini").Msg("REST connection setup is done")
	return nil
}

// processREST queries exchange for ticker / trade data through REST API in configured intervals,
// transforms it to a common ticker / trade store format,
// buffers the same in memory and
// then sends it to different storage systems for commit through go channels.
func (g *gemini) processREST(ctx context.Context, mktID string, mktCommitName string, channel string, interval int) error {
	var (
		req *http.Request
		q   url.Values
		err error

		// See influxTimeVal (exchange.go) struct doc for details.
		influxTickerTime int64
		influxTradeTime  int64
	)

	cd := commitData{
		terTickers:        make([]storage.Ticker, 0, g.connCfg.Terminal.TickerCommitBuf),
		terTrades:         make([]storage.Trade, 0, g.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers:      make([]storage.Ticker, 0, g.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:       make([]storage.Trade, 0, g.connCfg.MySQL.TradeCommitBuf),
		esTickers:         make([]storage.Ticker, 0, g.connCfg.ES.TickerCommitBuf),
		esTrades:          make([]storage.Trade, 0, g.connCfg.ES.TradeCommitBuf),
		influxTickers:     make([]storage.Ticker, 0, g.connCfg.InfluxDB.TickerCommitBuf),
		influxTrades:      make([]storage.Trade, 0, g.connCfg.InfluxDB.TradeCommitBuf),
		natsTickers:       make([]storage.Ticker, 0, g.connCfg.NATS.TickerCommitBuf),
		natsTrades:        make([]storage.Trade, 0, g.connCfg.NATS.TradeCommitBuf),
		clickHouseTickers: make([]storage.Ticker, 0, g.connCfg.ClickHouse.TickerCommitBuf),
		clickHouseTrades:  make([]storage.Trade, 0, g.connCfg.ClickHouse.TradeCommitBuf),
		s3Tickers:         make([]storage.Ticker, 0, g.connCfg.S3.TickerCommitBuf),
		s3Trades:          make([]storage.Trade, 0, g.connCfg.S3.TradeCommitBuf),
	}

	switch channel {
	case "ticker":
		req, err = g.rest.Request(ctx, "GET", config.GeminiRESTBaseURL+"pubticker/"+mktID)
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
	case "trade":
		req, err = g.rest.Request(ctx, "GET", config.GeminiRESTBaseURL+"trades/"+mktID)
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
		q = req.URL.Query()

		// Querying for 100 trades.
		// If the configured interval gap is big, then maybe it will not return all the trades
		// and if the gap is too small, maybe it will return duplicate ones.
		// Cursor pagination is not implemented.
		// Better to use websocket.
		q.Add("limit_trades", strconv.Itoa(100))
	}

	tick := time.NewTicker(time.Duration(interval) * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:

			switch channel {
			case "ticker":
				resp, err := g.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := restRespGemini{}
				if err = jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				price, err := strconv.ParseFloat(rr.TickerPrice, 64)
				if err != nil {
					logErrStack(err)
					return err
				}

				ticker := storage.Ticker{
					Exchange:      "gemini",
					MktID:         mktID,
					MktCommitName: mktCommitName,
					Price:         price,
					Timestamp:     time.Now().UTC(),
				}

				key := cfgLookupKey{market: strings.ToUpper(ticker.MktID), channel: "ticker"}
				val := g.cfgMap[key]
				if val.terStr {
					cd.terTickersCount++
					cd.terTickers = append(cd.terTickers, ticker)
					if cd.terTickersCount == g.connCfg.Terminal.TickerCommitBuf {
						g.ter.CommitTickers(cd.terTickers)
						cd.terTickersCount = 0
						cd.terTickers = nil
					}
				}
				if val.mysqlStr {
					cd.mysqlTickersCount++
					cd.mysqlTickers = append(cd.mysqlTickers, ticker)
					if cd.mysqlTickersCount == g.connCfg.MySQL.TickerCommitBuf {
						err := g.mysql.CommitTickers(ctx, cd.mysqlTickers)
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
					if cd.esTickersCount == g.connCfg.ES.TickerCommitBuf {
						err := g.es.CommitTickers(ctx, cd.esTickers)
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
					if cd.influxTickersCount == g.connCfg.InfluxDB.TickerCommitBuf {
						err := g.influx.CommitTickers(ctx, cd.influxTickers)
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
					if cd.natsTickersCount == g.connCfg.NATS.TickerCommitBuf {
						err := g.nats.CommitTickers(cd.natsTickers)
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
					if cd.clickHouseTickersCount == g.connCfg.ClickHouse.TickerCommitBuf {
						err := g.clickhouse.CommitTickers(ctx, cd.clickHouseTickers)
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
					if cd.s3TickersCount == g.connCfg.S3.TickerCommitBuf {
						err := g.s3.CommitTickers(ctx, cd.s3Tickers)
						if err != nil {
							return err
						}
						cd.s3TickersCount = 0
						cd.s3Tickers = nil
					}
				}
			case "trade":
				req.URL.RawQuery = q.Encode()
				resp, err := g.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := []restRespGemini{}
				if err := jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				for i := range rr {
					r := rr[i]

					size, err := strconv.ParseFloat(r.Amount, 64)
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
					timestamp := time.Unix(0, r.Timestamp*int64(time.Millisecond)).UTC()

					trade := storage.Trade{
						Exchange:      "gemini",
						MktID:         mktID,
						MktCommitName: mktCommitName,
						TradeID:       strconv.FormatUint(r.TradeID, 10),
						Side:          r.Type,
						Size:          size,
						Price:         price,
						Timestamp:     timestamp,
					}

					key := cfgLookupKey{market: strings.ToUpper(trade.MktID), channel: "trade"}
					val := g.cfgMap[key]
					if val.terStr {
						cd.terTradesCount++
						cd.terTrades = append(cd.terTrades, trade)
						if cd.terTradesCount == g.connCfg.Terminal.TradeCommitBuf {
							g.ter.CommitTrades(cd.terTrades)
							cd.terTradesCount = 0
							cd.terTrades = nil
						}
					}
					if val.mysqlStr {
						cd.mysqlTradesCount++
						cd.mysqlTrades = append(cd.mysqlTrades, trade)
						if cd.mysqlTradesCount == g.connCfg.MySQL.TradeCommitBuf {
							err := g.mysql.CommitTrades(ctx, cd.mysqlTrades)
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
						if cd.esTradesCount == g.connCfg.ES.TradeCommitBuf {
							err := g.es.CommitTrades(ctx, cd.esTrades)
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
						if cd.influxTradesCount == g.connCfg.InfluxDB.TradeCommitBuf {
							err := g.influx.CommitTrades(ctx, cd.influxTrades)
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
						if cd.natsTradesCount == g.connCfg.NATS.TradeCommitBuf {
							err := g.nats.CommitTrades(cd.natsTrades)
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
						if cd.clickHouseTradesCount == g.connCfg.ClickHouse.TradeCommitBuf {
							err := g.clickhouse.CommitTrades(ctx, cd.clickHouseTrades)
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
						if cd.s3TradesCount == g.connCfg.S3.TradeCommitBuf {
							err := g.s3.CommitTrades(ctx, cd.s3Trades)
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
