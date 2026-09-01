// Package fakedsp is a scriptable stand-in for a Tesira device, used by tests
// to exercise the protocol, reconnection and block logic without hardware.
package fakedsp

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"regexp"
	"strings"
	"sync"
)

// Welcome is the banner a real TTP server prints once a session is usable.
const Welcome = "Welcome to the Tesira Text Protocol Server..."

// Server is a fake TTP server listening on localhost.
type Server struct {
	ln net.Listener

	mu        sync.Mutex
	replies   map[string]string // command -> full response line
	subValues map[string]string // "attr|index" -> current value literal
	tokens    map[string]string // publish token -> "attr|index"
	conns     map[net.Conn]bool
	commands  []string
	sessions  int
	verbose   bool
	detailed  bool
}

// New starts a fake DSP on an ephemeral port.
func New() (*Server, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	s := &Server{
		ln:        ln,
		replies:   map[string]string{},
		subValues: map[string]string{},
		tokens:    map[string]string{},
		conns:     map[net.Conn]bool{},
	}
	go s.accept()
	return s, nil
}

// Addr returns the host and port the fake DSP listens on.
func (s *Server) Addr() (string, int) {
	a := s.ln.Addr().(*net.TCPAddr)
	return "127.0.0.1", a.Port
}

// Close shuts the fake DSP down.
func (s *Server) Close() {
	s.ln.Close()
	s.DropConnections()
}

// SetValue makes a command answer with a value.
func (s *Server) SetValue(command, valueLiteral string) {
	s.setReply(command, fmt.Sprintf(`+OK [ %s ] "value":%s`, command, valueLiteral))
}

// SetList makes a command answer with a list.
func (s *Server) SetList(command, listLiteral string) {
	s.setReply(command, fmt.Sprintf(`+OK [ %s ] "list":%s`, command, listLiteral))
}

// SetOK makes a command answer with a bare success.
func (s *Server) SetOK(command string) {
	s.setReply(command, fmt.Sprintf(`+OK [ %s ]`, command))
}

// SetError makes a command answer with a failure.
func (s *Server) SetError(command, message string) {
	s.setReply(command, fmt.Sprintf(`-ERR [ %s ] %s`, command, message))
}

func (s *Server) setReply(command, line string) {
	s.mu.Lock()
	s.replies[command] = line
	s.mu.Unlock()
}

// SetSubscriptionValue sets the value published when an attribute is
// subscribed. Use "ALL" as index for block-wide attributes.
func (s *Server) SetSubscriptionValue(attr, index, valueLiteral string) {
	s.mu.Lock()
	s.subValues[attr+"|"+index] = valueLiteral
	s.mu.Unlock()
}

// PublishUpdate pushes a new value for a subscribed attribute to every
// connected session, as the device does when an attribute changes.
func (s *Server) PublishUpdate(attr, index, valueLiteral string) {
	s.mu.Lock()
	s.subValues[attr+"|"+index] = valueLiteral
	var lines []string
	for token, key := range s.tokens {
		if key == attr+"|"+index {
			lines = append(lines, fmt.Sprintf(`! "publishToken":"%s" "value":%s`, token, valueLiteral))
		}
	}
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	for _, c := range conns {
		for _, l := range lines {
			fmt.Fprintf(c, "%s\r\n", l)
		}
	}
}

// DropConnections closes every live session, simulating a network drop.
func (s *Server) DropConnections() {
	s.mu.Lock()
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.conns = map[net.Conn]bool{}
	s.tokens = map[string]string{}
	s.mu.Unlock()

	for _, c := range conns {
		c.Close()
	}
}

// Sessions returns how many sessions have been opened, which is how tests
// observe reconnection.
func (s *Server) Sessions() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions
}

// Commands returns every command the fake DSP has received.
func (s *Server) Commands() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.commands...)
}

// Received reports whether a command was received.
func (s *Server) Received(command string) bool {
	for _, c := range s.Commands() {
		if c == command {
			return true
		}
	}
	return false
}

func (s *Server) accept() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns[conn] = true
		s.sessions++
		s.mu.Unlock()
		go s.serve(conn)
	}
}

const (
	iacByte  = 255
	doByte   = 253
	willByte = 251
	sbByte   = 250
	seByte   = 240
	optEcho  = 1
)

func (s *Server) serve(conn net.Conn) {
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		conn.Close()
	}()

	// Open with telnet negotiation, so the transport's IAC filter is exercised.
	conn.Write([]byte{iacByte, doByte, optEcho, iacByte, willByte, optEcho})
	fmt.Fprintf(conn, "%s\r\n", Welcome)

	scanner := bufio.NewScanner(&iacStripper{r: conn})
	for scanner.Scan() {
		cmd := strings.TrimSpace(scanner.Text())
		if cmd == "" {
			continue
		}
		s.mu.Lock()
		s.commands = append(s.commands, cmd)
		s.mu.Unlock()

		fmt.Fprintf(conn, "%s\r\n", s.reply(conn, cmd))
	}
}

var subscribeRe = regexp.MustCompile(`^"?([^"]+)"?\s+subscribe\s+(\S+)(?:\s+(\d+))?\s+"([^"]+)"$`)

func (s *Server) reply(conn net.Conn, cmd string) string {
	switch cmd {
	case "SESSION set verbose true":
		s.mu.Lock()
		s.verbose = true
		s.mu.Unlock()
		return "+OK"
	case "SESSION set detailedResponse true":
		s.mu.Lock()
		s.detailed = true
		s.mu.Unlock()
		return "+OK"
	}

	if m := subscribeRe.FindStringSubmatch(cmd); m != nil {
		return s.subscribe(conn, cmd, m)
	}

	s.mu.Lock()
	line, ok := s.replies[cmd]
	s.mu.Unlock()
	if ok {
		return line
	}
	return fmt.Sprintf(`-ERR [ %s ] unrecognized command`, cmd)
}

func (s *Server) subscribe(conn net.Conn, cmd string, m []string) string {
	attr, index, token := m[2], m[3], m[4]
	if index == "" {
		index = "ALL"
	}
	key := attr + "|" + index

	s.mu.Lock()
	if existing, dup := s.tokens[token]; dup && existing == key {
		s.mu.Unlock()
		// A live subscription is declined, exactly as the device does.
		return fmt.Sprintf(`-ERR [ %s ] ALREADY_SUBSCRIBED`, cmd)
	}
	s.tokens[token] = key
	value, hasValue := s.subValues[key]
	s.mu.Unlock()

	// The device pushes the current value before acknowledging the subscribe.
	if hasValue {
		fmt.Fprintf(conn, "! \"publishToken\":\"%s\" \"value\":%s\r\n", token, value)
	}
	return fmt.Sprintf(`+OK [ %s ]`, cmd)
}

// iacStripper removes telnet command sequences from a client's input, so the
// client's replies to our negotiation do not end up prepended to a command.
type iacStripper struct {
	r     io.Reader
	buf   [512]byte
	state int
	skip  int
}

const (
	stText = iota
	stIAC
	stOption
	stSubneg
	stSubnegIAC
)

func (s *iacStripper) Read(p []byte) (int, error) {
	for {
		n, err := s.r.Read(s.buf[:min(len(p), len(s.buf))])
		out := s.buf[:0:0]
		for _, b := range s.buf[:n] {
			switch s.state {
			case stText:
				if b == iacByte {
					s.state = stIAC
					continue
				}
				out = append(out, b)
			case stIAC:
				switch b {
				case iacByte:
					out = append(out, b) // escaped literal 0xFF
					s.state = stText
				case sbByte:
					s.state = stSubneg
				default:
					s.state = stOption
				}
			case stOption:
				s.state = stText // consume the option byte
			case stSubneg:
				if b == iacByte {
					s.state = stSubnegIAC
				}
			case stSubnegIAC:
				if b == seByte {
					s.state = stText
				} else {
					s.state = stSubneg
				}
			}
		}
		if len(out) > 0 {
			return copy(p, out), nil
		}
		if err != nil {
			return 0, err
		}
	}
}
