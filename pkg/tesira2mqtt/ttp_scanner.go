package tesira2mqtt

import (
	"fmt"
	"log"
	"strconv"
	"strings"
)

type TTPError struct {
	Message string
}

func (e *TTPError) Error() string {
	return fmt.Sprintf("TTP error: %s", e.Message)
}

type TTPResponse struct {
	Value any
}

type TTPSubscriptionData struct {
	PublishToken string
	Data         any
}

func ScanTTPLine(line string) (any, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return nil, nil
	}

	if strings.HasPrefix(line, "-ERR ") {
		return &TTPError{Message: strings.TrimPrefix(line, "-ERR ")}, nil
	} else if strings.HasPrefix(line, "+OK") {
		valueStr := strings.TrimSpace(strings.TrimPrefix(line, "+OK"))
		if valueStr == "" {
			return &TTPResponse{Value: nil}, nil
		}
		value, err := ParseTTPValue(valueStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse +OK value: %v", err)
		}
		return &TTPResponse{Value: value}, nil
	} else if strings.HasPrefix(line, "! ") {
		valueStr := strings.TrimSpace(strings.TrimPrefix(line, "! "))
		if valueStr == "" {
			return nil, nil
		}
		value, err := ParseTTPValue(valueStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse +OK value: %v", err)
		}
		return &TTPSubscriptionData{PublishToken: value.(map[string]any)["publishToken"].(string), Data: value.(map[string]any)["value"]}, nil
	} else if strings.HasPrefix(line, "Last login:") || strings.HasPrefix(line, "Welcome to the Tesira Text Protocol Server") {
		return nil, nil
	}

	log.Printf("Wawning: Could not parse TTP line: '%v'", line)
	return nil, nil
}

func ParseTTPValue(s string) (any, error) {
	p := &parser{src: s}
	p.skipSpace()

	var (
		out any
		err error
	)
	if p.atPair() {
		out, err = p.parsePairs(0)
	} else {
		out, err = p.parseValue()
	}
	if err != nil {
		return nil, err
	}

	p.skipSpace()
	if p.pos < len(p.src) {
		return nil, p.errf("trailing data %q", p.rest())
	}
	return out, nil
}

type parser struct {
	src string
	pos int
}

func (p *parser) errf(format string, args ...any) error {
	return fmt.Errorf("kvparse: offset %d: %s", p.pos, fmt.Sprintf(format, args...))
}

func (p *parser) rest() string {
	r := p.src[p.pos:]
	if len(r) > 24 {
		r = r[:24] + "..."
	}
	return r
}

func (p *parser) skipSpace() {
	for p.pos < len(p.src) {
		switch p.src[p.pos] {
		case ' ', '\t', '\n', '\r':
			p.pos++
		default:
			return
		}
	}
}

func isDelim(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r', ',', ':', '[', ']', '{', '}', '"':
		return true
	}
	return false
}

func (p *parser) atPair() bool {
	save := p.pos
	defer func() { p.pos = save }()

	if p.pos < len(p.src) && p.src[p.pos] == '"' {
		if _, err := p.parseString(); err != nil {
			return false
		}
	} else {
		if p.readToken() == "" {
			return false
		}
	}
	p.skipSpace()
	return p.pos < len(p.src) && p.src[p.pos] == ':'
}

// parsePairs reads `key:value` pairs until end (0 means end of input).
// Duplicate keys: last one wins.
func (p *parser) parsePairs(end byte) (map[string]any, error) {
	m := map[string]any{}
	for {
		p.skipSpace()
		if p.pos >= len(p.src) {
			if end == 0 {
				return m, nil
			}
			return nil, p.errf("unterminated object, expected %q", string(end))
		}
		if end != 0 && p.src[p.pos] == end {
			p.pos++
			return m, nil
		}

		var key string
		if p.src[p.pos] == '"' {
			s, err := p.parseString()
			if err != nil {
				return nil, err
			}
			key = s
		} else {
			key = p.readToken()
			if key == "" {
				return nil, p.errf("expected a key, got %q", p.rest())
			}
		}

		p.skipSpace()
		if p.pos >= len(p.src) || p.src[p.pos] != ':' {
			return nil, p.errf("expected ':' after key %q", key)
		}
		p.pos++

		v, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		m[key] = v
	}
}

func (p *parser) parseValue() (any, error) {
	p.skipSpace()
	if p.pos >= len(p.src) {
		return nil, p.errf("expected a value, got end of input")
	}
	switch c := p.src[p.pos]; {
	case c == '"':
		return p.parseString()
	case c == '[':
		return p.parseArray()
	case c == '{':
		p.pos++
		return p.parsePairs('}')
	case c == '-' || c == '+' || (c >= '0' && c <= '9'):
		return p.parseNumber()
	default:
		return p.parseBareword()
	}
}

func (p *parser) parseArray() ([]any, error) {
	p.pos++ // '['
	arr := []any{}
	for {
		p.skipSpace()
		if p.pos >= len(p.src) {
			return nil, p.errf("unterminated array")
		}
		if p.src[p.pos] == ']' {
			p.pos++
			return arr, nil
		}
		v, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		arr = append(arr, v)
	}
}

// readToken consumes everything up to the next delimiter.
func (p *parser) readToken() string {
	start := p.pos
	for p.pos < len(p.src) && !isDelim(p.src[p.pos]) {
		p.pos++
	}
	return p.src[start:p.pos]
}

func (p *parser) parseNumber() (any, error) {
	start := p.pos
	tok := p.readToken()
	if i, err := strconv.ParseInt(tok, 10, 64); err == nil {
		return i, nil
	}
	f, err := strconv.ParseFloat(tok, 64)
	if err != nil {
		p.pos = start
		return nil, p.errf("invalid number %q", tok)
	}
	return f, nil
}

func (p *parser) parseBareword() (any, error) {
	tok := p.readToken()
	switch tok {
	case "":
		return nil, p.errf("unexpected character %q", string(p.src[p.pos]))
	case "true":
		return true, nil
	case "false":
		return false, nil
	}
	return tok, nil
}

func (p *parser) parseString() (string, error) {
	p.pos++ // opening quote
	start := p.pos
	for p.pos < len(p.src) {
		if p.src[p.pos] == '"' {
			s := p.src[start:p.pos]
			p.pos++
			return s, nil
		}
		p.pos++
	}
	return "", p.errf("unterminated string")
}
