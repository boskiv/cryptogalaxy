package storage

import (
	"context"
	"fmt"
	"io"
)

// Terminal is for displaying data on terminal.
type Terminal struct {
	out io.Writer
}

var terminal Terminal

// TerminalTimestamp is used as a format to display only the time.
const TerminalTimestamp = "15:04:05.999"

// InitTerminal initializes terminal display.
// Output writer is always os.Stdout except in case of testing where file will be set as output terminal.
func InitTerminal(out io.Writer) *Terminal {
	if terminal.out == nil {
		terminal = Terminal{
			out: out,
		}
	}
	return &terminal
}

// GetTerminal returns already prepared terminal instance.
func GetTerminal() *Terminal {
	return &terminal
}

// CommitTickers batch outputs input ticker data to terminal.
func (t *Terminal) CommitTickers(_ context.Context, data []Ticker) error {
	for i := range data {
		ticker := data[i]
		fmt.Fprintf(t.out, "%-15s%-15s%-15s%20f%20s\n\n", "Ticker", ticker.Exchange, ticker.MktCommitName, ticker.Price, ticker.Timestamp.Local().Format(TerminalTimestamp))
	}
	return nil
}

// CommitTrades batch outputs input trade data to terminal.
func (t *Terminal) CommitTrades(_ context.Context, data []Trade) error {
	for i := range data {
		trade := data[i]
		fmt.Fprintf(t.out, "%-15s%-15s%-5s%20f%20f%20s\n\n", "Trade", trade.Exchange, trade.MktCommitName, trade.Size, trade.Price, trade.Timestamp.Local().Format(TerminalTimestamp))
	}
	return nil
}
