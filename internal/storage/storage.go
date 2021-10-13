package storage

import (
	"context"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"time"
)

type Storage interface {
	CommitTrades(context.Context, []Trade) error
	CommitTickers(context.Context, []Ticker) error
}

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

func TickersToStorage(ctx context.Context, storage Storage, input chan []Ticker) error {
	for {
		select {
		case data := <-input:
			err := storage.CommitTickers(ctx, data)
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

func TradesToStorage(ctx context.Context, storage Storage, input chan []Trade) error {
	for {
		select {
		case data := <-input:
			err := storage.CommitTrades(ctx, data)
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
