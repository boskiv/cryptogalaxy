package storage

import (
	"context"
	"time"
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
