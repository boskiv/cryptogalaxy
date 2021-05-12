package main

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"os"

	jsoniter "github.com/json-iterator/go"
	"github.com/milkywaybrain/cryptogalaxy/internal/config"
	"github.com/rs/zerolog/log"
)

// This function will query all the exchanges for market info and store it in a csv file.
// Users can look up to this csv file to give market ID in the app configuration.
// CSV file created at ./examples/markets.csv.
func main() {
	f, err := os.Create("./examples/markets.csv")
	if err != nil {
		log.Error().Err(err).Str("exchange", "ftx").Msg("csv file create")
		return
	}
	w := csv.NewWriter(f)
	defer w.Flush()
	defer f.Close()

	// FTX exchange.
	resp, err := http.Get(config.FtxRESTBaseURL + "markets")
	if err != nil {
		log.Error().Err(err).Str("exchange", "ftx").Msg("exchange request for markets")
		return
	}
	ftxMarkets := ftxResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&ftxMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "ftx").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range ftxMarkets.Result {
		if record.Type != "spot" {
			continue
		}
		if err = w.Write([]string{"ftx", record.Name}); err != nil {
			log.Error().Err(err).Str("exchange", "ftx").Msg("writing markets to csv")
			return
		}
	}
	fmt.Println("got market info from FTX")

	// Coinbase-Pro exchange.
	resp, err = http.Get(config.CoinbaseProRESTBaseURL + "products")
	if err != nil {
		log.Error().Err(err).Str("exchange", "coinbase-pro").Msg("exchange request for markets")
		return
	}
	coinbaseProMarkets := []coinbaseProResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&coinbaseProMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "coinbase-pro").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range coinbaseProMarkets {
		if err = w.Write([]string{"coinbase-pro", record.Name}); err != nil {
			log.Error().Err(err).Str("exchange", "coinbase-pro").Msg("writing markets to csv")
			return
		}
	}
	fmt.Println("got market info from Coinbase Pro")

	// Binance exchange.
	resp, err = http.Get(config.BinanceRESTBaseURL + "exchangeInfo")
	if err != nil {
		log.Error().Err(err).Str("exchange", "binance").Msg("exchange request for markets")
		return
	}
	binanceMarkets := binanceResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&binanceMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "binance").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range binanceMarkets.Result {
		if err = w.Write([]string{"binance", record.Name}); err != nil {
			log.Error().Err(err).Str("exchange", "binance").Msg("writing markets to csv")
			return
		}
	}
	fmt.Println("got market info from Binance")

	// Bitfinex exchange.
	resp, err = http.Get(config.BitfinexRESTBaseURL + "conf/pub:list:pair:exchange")
	if err != nil {
		log.Error().Err(err).Str("exchange", "bitfinex").Msg("exchange request for markets")
		return
	}
	bitfinexMarkets := bitfinexResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&bitfinexMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "bitfinex").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range bitfinexMarkets[0] {
		if err = w.Write([]string{"bitfinex", record}); err != nil {
			log.Error().Err(err).Str("exchange", "bitfinex").Msg("writing markets to csv")
			return
		}
	}
	fmt.Println("got market info from Bitfinex")

	// Hbtc exchange.
	resp, err = http.Get(config.HbtcRESTBaseURL + "openapi/v1/pairs")
	if err != nil {
		log.Error().Err(err).Str("exchange", "hbtc").Msg("exchange request for markets")
		return
	}
	hbtcMarkets := []hbtcRespRes{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&hbtcMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "hbtc").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range hbtcMarkets {
		if err = w.Write([]string{"hbtc", record.Name}); err != nil {
			log.Error().Err(err).Str("exchange", "hbtc").Msg("writing markets to csv")
			return
		}
	}
	fmt.Println("got market info from Hbtc")

	// Huobi exchange.
	resp, err = http.Get(config.HuobiRESTBaseURL + "v1/common/symbols")
	if err != nil {
		log.Error().Err(err).Str("exchange", "huobi").Msg("exchange request for markets")
		return
	}
	huobiMarkets := huobiResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&huobiMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "huobi").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range huobiMarkets.Data {
		if err = w.Write([]string{"huobi", record.Symbol}); err != nil {
			log.Error().Err(err).Str("exchange", "huobi").Msg("writing markets to csv")
			return
		}
	}
	fmt.Println("got market info from Huobi")

	// Gateio exchange.
	resp, err = http.Get(config.GateioRESTBaseURL + "spot/currency_pairs")
	if err != nil {
		log.Error().Err(err).Str("exchange", "gateio").Msg("exchange request for markets")
		return
	}
	gateioMarkets := []gateioResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&gateioMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "gateio").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range gateioMarkets {
		if err = w.Write([]string{"gateio", record.Name}); err != nil {
			log.Error().Err(err).Str("exchange", "gateio").Msg("writing markets to csv")
			return
		}
	}
	fmt.Println("got market info from Gateio")

	// Kucoin exchange.
	resp, err = http.Get(config.KucoinRESTBaseURL + "symbols")
	if err != nil {
		log.Error().Err(err).Str("exchange", "kucoin").Msg("exchange request for markets")
		return
	}
	kucoinMarkets := kucoinResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&kucoinMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "kucoin").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range kucoinMarkets.Data {
		if err = w.Write([]string{"kucoin", record.Symbol}); err != nil {
			log.Error().Err(err).Str("exchange", "kucoin").Msg("writing markets to csv")
			return
		}
	}
	fmt.Println("got market info from Kucoin")

	fmt.Println("CSV file generated successfully at ./examples/markets.csv")
}

type ftxResp struct {
	Result []ftxRespRes `json:"result"`
}
type ftxRespRes struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type coinbaseProResp struct {
	Name string `json:"id"`
}

type binanceResp struct {
	Result []binanceRespRes `json:"symbols"`
}
type binanceRespRes struct {
	Name string `json:"symbol"`
}

type bitfinexResp [][]string

type hbtcRespRes struct {
	Name string `json:"symbol"`
}

type huobiResp struct {
	Data []huobiRespData `json:"data"`
}
type huobiRespData struct {
	Symbol string `json:"symbol"`
}

type gateioResp struct {
	Name string `json:"id"`
}

type kucoinResp struct {
	Data []kucoinRespData `json:"data"`
}
type kucoinRespData struct {
	Symbol string `json:"symbol"`
}
