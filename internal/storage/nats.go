package storage

import (
	"context"
	"strings"
	"time"

	"github.com/milkywaybrain/cryptogalaxy/internal/config"
	nc "github.com/nats-io/nats.go"
)

// NATS is for connecting and inserting data to NATS.
type NATS struct {
	Basic  *nc.Conn
	Client *nc.EncodedConn
	Cfg    *config.NATS
}

var nats NATS

// InitNATS initializes nats connection with configured values.
func InitNATS(cfg *config.NATS) (*NATS, error) {
	basic, err := nc.Connect(strings.Join(cfg.Addresses, ","),
		nc.Name("Cryptogalaxy Publisher"),
		nc.Timeout(time.Duration(cfg.ReqTimeoutSec)*time.Second),
		nc.PingInterval(-1),
		nc.NoReconnect(),
		nc.UserInfo(cfg.Username, cfg.Password))
	if err != nil {
		return nil, err
	}

	// Client for structured data publish.
	client, err := nc.NewEncodedConn(basic, nc.JSON_ENCODER)
	if err != nil {
		return nil, err
	}

	nats = NATS{
		Basic:  basic,
		Client: client,
		Cfg:    cfg,
	}
	return &nats, nil
}

// GetNATS returns already prepared nats instance.
func GetNATS() *NATS {
	return &nats
}

// CommitTickers batch inserts input ticker data to nats.
func (n *NATS) CommitTickers(_ context.Context, data []Ticker) error {
	for i := range data {
		ticker := data[i]

		// Variable is used only for InfluxDB.
		ticker.InfluxVal = 0

		err := n.Client.Publish(n.Cfg.SubjectBaseName+".ticker", &ticker)
		if err != nil {
			return err
		}
	}

	// Maybe data is pushed to NATS server before the flush also.
	// Because client library of NATS automatically buffers and pushes the data to server.
	// This is just to confirm.
	err := n.Client.Flush()
	if err != nil {
		return err
	}
	return nil
}

// CommitTrades batch inserts input trade data to nats.
func (n *NATS) CommitTrades(_ context.Context, data []Trade) error {
	for i := range data {
		trade := data[i]

		// Variable is used only for InfluxDB.
		trade.InfluxVal = 0

		err := n.Client.Publish(n.Cfg.SubjectBaseName+".trade", &trade)
		if err != nil {
			return err
		}
	}

	// Maybe data is pushed to NATS server before the flush also.
	// Because client library of NATS automatically buffers and pushes the data to server.
	// This is just to confirm.
	err := n.Client.Flush()
	if err != nil {
		return err
	}
	return nil
}
