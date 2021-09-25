package initializer

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/milkywaybrain/cryptogalaxy/internal/config"
	"github.com/milkywaybrain/cryptogalaxy/internal/connector"
	"github.com/milkywaybrain/cryptogalaxy/internal/exchange"
	"github.com/milkywaybrain/cryptogalaxy/internal/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"golang.org/x/sync/errgroup"
)

// Start will initialize various required systems and then execute the app.
func Start(mainCtx context.Context, cfg *config.Config) error {

	// Setting up logger.
	// If the path given in the config for logging ends with .log then create a log file with the same name and
	// write log messages to it. Otherwise, create a new log file with a timestamp attached to it's name in the given path.
	var (
		logFile *os.File
		err     error
	)
	if strings.HasSuffix(cfg.Log.FilePath, ".log") {
		logFile, err = os.OpenFile(cfg.Log.FilePath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
		if err != nil {
			return fmt.Errorf("not able to open or create log file: %v", cfg.Log.FilePath)
		}
	} else {
		logFile, err = os.Create(cfg.Log.FilePath + "_" + strconv.Itoa(int(time.Now().Unix())) + ".log")
		if err != nil {
			return fmt.Errorf("not able to create log file: %v", cfg.Log.FilePath+"_"+strconv.Itoa(int(time.Now().Unix()))+".log")
		}
	}
	defer logFile.Close()

	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	switch cfg.Log.Level {
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	fileLogger := zerolog.New(logFile).With().Timestamp().Logger()
	log.Logger = fileLogger
	log.Info().Msg("logger setup is done")

	// Establish connections to different storage systems, connectors and
	// also validate few user defined config values.
	var (
		restConn      bool
		terStr        bool
		sqlStr        bool
		esStr         bool
		influxStr     bool
		natsStr       bool
		clickHouseStr bool
		s3Str         bool
	)
	for _, exch := range cfg.Exchanges {
		for _, market := range exch.Markets {
			for _, info := range market.Info {
				for _, str := range info.Storages {
					switch str {
					case "terminal":
						if !terStr {
							_ = storage.InitTerminal(os.Stdout)
							terStr = true
							log.Info().Msg("terminal connected")
						}
					case "mysql":
						if !sqlStr {
							_, err = storage.InitMySQL(&cfg.Connection.MySQL)
							if err != nil {
								err = errors.Wrap(err, "mysql connection")
								log.Error().Stack().Err(errors.WithStack(err)).Msg("")
								return err
							}
							sqlStr = true
							log.Info().Msg("mysql connected")
						}
					case "elastic_search":
						if !esStr {
							_, err = storage.InitElasticSearch(&cfg.Connection.ES)
							if err != nil {
								err = errors.Wrap(err, "elastic search connection")
								log.Error().Stack().Err(errors.WithStack(err)).Msg("")
								return err
							}
							esStr = true
							log.Info().Msg("elastic search connected")
						}
					case "influxdb":
						if !influxStr {
							_, err = storage.InitInfluxDB(&cfg.Connection.InfluxDB)
							if err != nil {
								err = errors.Wrap(err, "influxdb connection")
								log.Error().Stack().Err(errors.WithStack(err)).Msg("")
								return err
							}
							influxStr = true
							log.Info().Msg("influxdb connected")
						}
					case "nats":
						if !natsStr {
							_, err = storage.InitNATS(&cfg.Connection.NATS)
							if err != nil {
								err = errors.Wrap(err, "nats connection")
								log.Error().Stack().Err(errors.WithStack(err)).Msg("")
								return err
							}
							natsStr = true
							log.Info().Msg("nats connected")
						}
					case "clickhouse":
						if !clickHouseStr {
							_, err = storage.InitClickHouse(&cfg.Connection.ClickHouse)
							if err != nil {
								err = errors.Wrap(err, "clickhouse connection")
								log.Error().Stack().Err(errors.WithStack(err)).Msg("")
								return err
							}
							clickHouseStr = true
							log.Info().Msg("clickhouse connected")
						}
					case "s3":
						if !s3Str {
							_, err = storage.InitS3(&cfg.Connection.S3)
							if err != nil {
								err = errors.Wrap(err, "s3 connection")
								log.Error().Stack().Err(errors.WithStack(err)).Msg("")
								return err
							}
							s3Str = true
							log.Info().Msg("s3 connected")
						}
					}
				}
				if info.Connector == "rest" {
					if !restConn {
						_ = connector.InitREST(&cfg.Connection.REST)
						restConn = true
					}
					if info.RESTPingIntSec < 1 {
						err = errors.New("rest_ping_interval_sec should be greater than zero")
						log.Error().Stack().Err(errors.WithStack(err)).Msg("")
						return err
					}
				}
			}
		}
	}

	// Start each exchange function. If any exchange fails after retry, force all the other exchanges to stop and
	// exit the app.
	appErrGroup, appCtx := errgroup.WithContext(mainCtx)

	for _, exch := range cfg.Exchanges {
		markets := exch.Markets
		retry := exch.Retry
		switch exch.Name {
		case "ftx":
			appErrGroup.Go(func() error {
				return exchange.StartFtx(appCtx, markets, &retry, &cfg.Connection)
			})
		case "coinbase-pro":
			appErrGroup.Go(func() error {
				return exchange.StartCoinbasePro(appCtx, markets, &retry, &cfg.Connection)
			})
		case "binance":
			appErrGroup.Go(func() error {
				return exchange.StartBinance(appCtx, markets, &retry, &cfg.Connection)
			})
		case "bitfinex":
			appErrGroup.Go(func() error {
				return exchange.StartBitfinex(appCtx, markets, &retry, &cfg.Connection)
			})
		case "hbtc":
			appErrGroup.Go(func() error {
				return exchange.StartHbtc(appCtx, markets, &retry, &cfg.Connection)
			})
		case "huobi":
			appErrGroup.Go(func() error {
				return exchange.StartHuobi(appCtx, markets, &retry, &cfg.Connection)
			})
		case "gateio":
			appErrGroup.Go(func() error {
				return exchange.StartGateio(appCtx, markets, &retry, &cfg.Connection)
			})
		case "kucoin":
			appErrGroup.Go(func() error {
				return exchange.StartKucoin(appCtx, markets, &retry, &cfg.Connection)
			})
		case "bitstamp":
			appErrGroup.Go(func() error {
				return exchange.StartBitstamp(appCtx, markets, &retry, &cfg.Connection)
			})
		case "bybit":
			appErrGroup.Go(func() error {
				return exchange.StartBybit(appCtx, markets, &retry, &cfg.Connection)
			})
		case "probit":
			appErrGroup.Go(func() error {
				return exchange.StartProbit(appCtx, markets, &retry, &cfg.Connection)
			})
		case "gemini":
			appErrGroup.Go(func() error {
				return exchange.StartGemini(appCtx, markets, &retry, &cfg.Connection)
			})
		case "bitmart":
			appErrGroup.Go(func() error {
				return exchange.StartBitmart(appCtx, markets, &retry, &cfg.Connection)
			})
		case "digifinex":
			appErrGroup.Go(func() error {
				return exchange.StartDigifinex(appCtx, markets, &retry, &cfg.Connection)
			})
		case "ascendex":
			appErrGroup.Go(func() error {
				return exchange.StartAscendex(appCtx, markets, &retry, &cfg.Connection)
			})
		case "kraken":
			appErrGroup.Go(func() error {
				return exchange.StartKraken(appCtx, markets, &retry, &cfg.Connection)
			})
		case "binance-us":
			appErrGroup.Go(func() error {
				return exchange.StartBinanceUS(appCtx, markets, &retry, &cfg.Connection)
			})
		case "okex":
			appErrGroup.Go(func() error {
				return exchange.StartOKEx(appCtx, markets, &retry, &cfg.Connection)
			})
		case "ftx-us":
			appErrGroup.Go(func() error {
				return exchange.StartFtxUS(appCtx, markets, &retry, &cfg.Connection)
			})
		}
	}

	err = appErrGroup.Wait()
	if err != nil {
		log.Error().Msg("exiting the app")
		return err
	}
	return nil
}
