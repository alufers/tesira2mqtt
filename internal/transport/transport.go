// Package transport provides the byte pipes that carry a Tesira Text Protocol
// session: SSH and Telnet.
package transport

import (
	"context"
	"io"
)

// Dialer opens a fresh connection to a Tesira device. A Dialer is reusable:
// the client calls Dial again after every disconnect.
type Dialer interface {
	// Dial establishes a connection and returns the duplex stream carrying the
	// TTP session.
	Dial(ctx context.Context) (io.ReadWriteCloser, error)
	// String describes the endpoint, for logging.
	String() string
}

// rwc joins a separate reader and writer with a close function into one
// io.ReadWriteCloser.
type rwc struct {
	io.Reader
	io.Writer
	closeFn func() error
}

func (c *rwc) Close() error { return c.closeFn() }
