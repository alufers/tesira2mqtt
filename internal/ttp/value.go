// Package ttp implements parsing of the Tesira Text Protocol (TTP) wire format.
package ttp

import (
	"fmt"
	"strconv"
	"strings"
)

// Values on the wire look like JSON that lost its commas, e.g.
//
//	+OK "value":[-72.699997 -72.699997]
//	! "publishToken":"S1_levels_ALL_level_anc" "value":{"a":1 "b":[true false]}
//
// parseValue decodes one value into a Go representation: string, float64,
// bool, []any or map[string]any.

type parser struct {
	s   string
	pos int
}

func (p *parser) skipSpace() {
	for p.pos < len(p.s) && (p.s[p.pos] == ' ' || p.s[p.pos] == '\t') {
		p.pos++
	}
}

func (p *parser) peek() byte {
	p.skipSpace()
	if p.pos < len(p.s) {
		return p.s[p.pos]
	}
	return 0
}

// parseString reads a double quoted string, honouring backslash escapes.
func (p *parser) parseString() (string, error) {
	if p.peek() != '"' {
		return "", fmt.Errorf("expected quote at offset %d", p.pos)
	}
	p.pos++
	var b strings.Builder
	for p.pos < len(p.s) {
		switch c := p.s[p.pos]; c {
		case '\\':
			if p.pos+1 >= len(p.s) {
				return "", fmt.Errorf("dangling escape at offset %d", p.pos)
			}
			b.WriteByte(p.s[p.pos+1])
			p.pos += 2
		case '"':
			p.pos++
			return b.String(), nil
		default:
			b.WriteByte(c)
			p.pos++
		}
	}
	return "", fmt.Errorf("unterminated string at offset %d", p.pos)
}

const structural = " \t:[]{}\""

// parseBare reads an unquoted token, terminated by whitespace or by any
// structural character.
func (p *parser) parseBare() any {
	p.skipSpace()
	start := p.pos
	for p.pos < len(p.s) && !strings.ContainsRune(structural, rune(p.s[p.pos])) {
		p.pos++
	}
	return convertScalar(p.s[start:p.pos])
}

// convertScalar guesses the type of a bare token the way the device means it.
func convertScalar(raw string) any {
	raw = strings.TrimSpace(raw)
	switch strings.ToLower(raw) {
	case "true":
		return true
	case "false":
		return false
	}
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return f
	}
	return raw
}

func (p *parser) parseValue() (any, error) {
	switch p.peek() {
	case 0:
		return nil, fmt.Errorf("unexpected end of input")
	case '"':
		return p.parseString()
	case '[':
		p.pos++
		list := []any{}
		for {
			switch p.peek() {
			case ']':
				p.pos++
				return list, nil
			case 0:
				return nil, fmt.Errorf("unterminated list")
			}
			v, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			list = append(list, v)
		}
	case '{':
		p.pos++
		m, err := p.parseDictBody('}')
		if err != nil {
			return nil, err
		}
		if p.peek() != '}' {
			return nil, fmt.Errorf("unterminated dict")
		}
		p.pos++
		return m, nil
	default:
		return p.parseBare(), nil
	}
}

// parseDictBody reads "key:value" pairs until end, or until the input runs out
// when end is 0. Responses carry a brace-less dict at the top level, which is
// why the terminator is a parameter.
func (p *parser) parseDictBody(end byte) (map[string]any, error) {
	m := map[string]any{}
	for {
		c := p.peek()
		if c == 0 || (end != 0 && c == end) {
			return m, nil
		}

		var key string
		var err error
		if c == '"' {
			if key, err = p.parseString(); err != nil {
				return nil, err
			}
		} else {
			start := p.pos
			for p.pos < len(p.s) && !strings.ContainsRune(" \t:", rune(p.s[p.pos])) {
				p.pos++
			}
			key = p.s[start:p.pos]
		}

		if p.peek() != ':' {
			return nil, fmt.Errorf("expected colon after key %q at offset %d", key, p.pos)
		}
		p.pos++

		val, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		// Repeated keys keep their first occurrence: an active fault list
		// contains several "fault" keys and the first is the one callers want.
		if _, dup := m[key]; !dup {
			m[key] = val
		}
	}
}

// ParseValue decodes a single standalone TTP value.
func ParseValue(s string) (any, error) {
	p := &parser{s: s}
	return p.parseValue()
}

// ParseDictBody decodes a brace-less sequence of "key:value" pairs.
func ParseDictBody(s string) (map[string]any, error) {
	p := &parser{s: s}
	return p.parseDictBody(0)
}
