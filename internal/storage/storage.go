package storage

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// Ticker represents final form of market ticker info received from exchange
// ready to store.
type Ticker struct {
	Exchange      string
	MktID         string
	MktCommitName string
	Price         float64
	Timestamp     time.Time
	InfluxVal     int64 `json:",omitempty"`
}

// Trade represents final form of market trade info received from exchange
// ready to store.
type Trade struct {
	Exchange      string
	MktID         string
	MktCommitName string
	TradeID       string
	Side          string
	Size          float64
	Price         float64
	Timestamp     time.Time
	InfluxVal     int64 `json:",omitempty"`
}

// Storage represents different storage options where the ticker and trade data can be stored.
type Storage interface {
	CommitTickers(context.Context, []Ticker) error
	CommitTrades(context.Context, []Trade) error
}

// TickersToStorage batch inserts input ticker data to specified storage.
func TickersToStorage(ctx context.Context, str Storage, tickers <-chan []Ticker) error {
	for {
		select {
		case data := <-tickers:
			err := str.CommitTickers(ctx, data)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					log.Error().Stack().Err(errors.WithStack(err)).Msg("")
				}
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// TradesToStorage batch inserts input trade data to specified storage.
func TradesToStorage(ctx context.Context, str Storage, trades <-chan []Trade) error {
	for {
		select {
		case data := <-trades:
			err := str.CommitTrades(ctx, data)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					log.Error().Stack().Err(errors.WithStack(err)).Msg("")
				}
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
