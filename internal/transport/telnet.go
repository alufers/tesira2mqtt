package transport

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"
)

// Telnet command bytes (RFC 854).
const (
	iac  = 255 // Interpret As Command
	dont = 254
	do   = 253
	wont = 252
	will = 251
	sb   = 250 // Subnegotiation Begin
	se   = 240 // Subnegotiation End
)

// TelnetConfig describes a Telnet connection to a Tesira device.
type TelnetConfig struct {
	Host    string
	Port    int
	Timeout time.Duration
}

// TelnetDialer dials Tesira's TTP service over Telnet.
type TelnetDialer struct{ cfg TelnetConfig }

// NewTelnetDialer builds a Telnet dialer.
func NewTelnetDialer(cfg TelnetConfig) *TelnetDialer { return &TelnetDialer{cfg: cfg} }

func (d *TelnetDialer) String() string {
	return "telnet://" + net.JoinHostPort(d.cfg.Host, fmt.Sprint(d.cfg.Port))
}

// Dial opens the TCP connection and wraps it in a reader that answers option
// negotiation, so the option bytes never reach the TTP line parser.
func (d *TelnetDialer) Dial(ctx context.Context) (io.ReadWriteCloser, error) {
	addr := net.JoinHostPort(d.cfg.Host, fmt.Sprint(d.cfg.Port))
	dialer := net.Dialer{Timeout: d.cfg.Timeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return &rwc{
		Reader:  &telnetReader{conn: conn},
		Writer:  conn,
		closeFn: conn.Close,
	}, nil
}

// telnetReader strips IAC sequences out of the stream and refuses every option
// it is offered, which is enough to get a Tesira device to settle into plain
// line mode. Refusal is deliberate: TTP is a line protocol and needs no
// negotiated options, and answering keeps the peer from waiting on us.
type telnetReader struct {
	conn net.Conn
	buf  [1024]byte
}

func (t *telnetReader) Read(p []byte) (int, error) {
	for {
		n, err := t.conn.Read(t.buf[:min(len(p), len(t.buf))])
		if n > 0 {
			out := t.filter(t.buf[:n])
			if len(out) > 0 {
				return copy(p, out), nil
			}
			// The chunk held nothing but negotiation; read again rather than
			// returning 0, which a bufio.Scanner would treat as a stall.
			if err == nil {
				continue
			}
		}
		if err != nil {
			return 0, err
		}
	}
}

// filter removes telnet commands from data, replying to each option request.
func (t *telnetReader) filter(data []byte) []byte {
	out := data[:0:0]
	for i := 0; i < len(data); i++ {
		if data[i] != iac {
			out = append(out, data[i])
			continue
		}
		if i+1 >= len(data) {
			break
		}
		switch cmd := data[i+1]; cmd {
		case iac: // escaped 0xFF, a literal data byte
			out = append(out, iac)
			i++
		case do, dont, will, wont:
			if i+2 >= len(data) {
				return out
			}
			t.refuse(cmd, data[i+2])
			i += 2
		case sb:
			// Skip the subnegotiation payload up to IAC SE.
			for i += 2; i+1 < len(data); i++ {
				if data[i] == iac && data[i+1] == se {
					i++
					break
				}
			}
		default:
			i++ // two-byte command with no option
		}
	}
	return out
}

// refuse answers an option negotiation in the negative: WILL/WONT is met with
// DONT, DO/DONT with WONT.
func (t *telnetReader) refuse(cmd, option byte) {
	var reply byte
	switch cmd {
	case will, wont:
		reply = dont
	case do, dont:
		reply = wont
	default:
		return
	}
	// A failed reply is not fatal on its own; the read loop will surface the
	// broken connection on its next Read.
	_, _ = t.conn.Write([]byte{iac, reply, option})
}
