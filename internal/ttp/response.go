package ttp

import (
	"fmt"
	"strings"
)

// Kind classifies a TTP protocol line.
type Kind int

const (
	// KindOK is a successful command response, with or without a value.
	KindOK Kind = iota
	// KindError is a "-ERR ..." response.
	KindError
	// KindPublish is an unsolicited "!" subscription update.
	KindPublish
)

func (k Kind) String() string {
	switch k {
	case KindOK:
		return "OK"
	case KindError:
		return "ERR"
	case KindPublish:
		return "PUBLISH"
	}
	return "UNKNOWN"
}

// Response is one parsed TTP line.
type Response struct {
	Raw  string
	Kind Kind

	// Command is the originating command, echoed back by the device inside the
	// response because the session runs with "detailedResponse true". It is how
	// a response is associated with the request that produced it. It is empty
	// for the handful of responses the device sends without an echo, notably
	// the reply to the command that enables detailedResponse itself.
	Command string

	// Value holds the decoded "value" (or "list") payload, if the response
	// carried one.
	Value    any
	HasValue bool

	// PublishToken identifies the subscription a KindPublish line belongs to.
	PublishToken string

	// Error is the message text of a KindError response.
	Error string
}

// ParseLine decodes a single line of device output. Lines that are not TTP
// protocol lines - the login banner, the welcome message, or the terminal's
// echo of the command we just sent - report ok=false and should be ignored.
func ParseLine(line string) (*Response, bool) {
	line = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(line), "\r"))
	if line == "" {
		return nil, false
	}

	switch {
	case strings.HasPrefix(line, "+OK"):
		r := &Response{Raw: line, Kind: KindOK}
		body, cmd := splitDetailedEcho(strings.TrimSpace(line[len("+OK"):]))
		r.Command = cmd
		if body == "" {
			return r, true
		}
		fields, err := ParseDictBody(body)
		if err != nil {
			// A session that lost its "verbose true" baseline answers with a
			// bare value ("+OK true"). Decode that rather than erroring, so
			// data stays usable even then.
			if v, verr := ParseValue(body); verr == nil {
				r.Value, r.HasValue = v, true
			}
			return r, true
		}
		if v, ok := fields["value"]; ok {
			r.Value, r.HasValue = v, true
		} else if v, ok := fields["list"]; ok {
			r.Value, r.HasValue = v, true
		}
		return r, true

	case strings.HasPrefix(line, "-ERR"):
		body, cmd := splitDetailedEcho(strings.TrimSpace(line[len("-ERR"):]))
		return &Response{Raw: line, Kind: KindError, Command: cmd, Error: body}, true

	case strings.HasPrefix(line, "!"):
		fields, err := ParseDictBody(strings.TrimSpace(line[1:]))
		if err != nil {
			return nil, false
		}
		tok, _ := fields["publishToken"].(string)
		if tok == "" {
			return nil, false
		}
		r := &Response{Raw: line, Kind: KindPublish, PublishToken: tok}
		if v, ok := fields["value"]; ok {
			r.Value, r.HasValue = v, true
		}
		return r, true
	}

	return nil, false
}

// splitDetailedEcho peels off the "[ <command> ]" prefix the device prepends to
// every response once detailedResponse is enabled, returning the remaining body
// and the echoed command. Both "+OK" and "-ERR" carry it.
func splitDetailedEcho(body string) (rest, command string) {
	if !strings.HasPrefix(body, "[") {
		return body, ""
	}
	end := strings.Index(body, "]")
	if end < 0 {
		return body, ""
	}
	return strings.TrimSpace(body[end+1:]), strings.TrimSpace(body[1:end])
}

// Float returns the response value as a float64.
func (r *Response) Float() (float64, error) {
	f, ok := r.Value.(float64)
	if !ok {
		return 0, fmt.Errorf("value %v (%T) is not a number", r.Value, r.Value)
	}
	return f, nil
}

// Int returns the response value as an int.
func (r *Response) Int() (int, error) {
	f, err := r.Float()
	if err != nil {
		return 0, err
	}
	return int(f), nil
}

// Bool returns the response value as a bool.
func (r *Response) Bool() (bool, error) {
	b, ok := r.Value.(bool)
	if !ok {
		return false, fmt.Errorf("value %v (%T) is not a boolean", r.Value, r.Value)
	}
	return b, nil
}

// Str returns the response value as a string. Numbers and booleans are
// rendered rather than rejected, since a label made only of digits comes back
// from the device as a number.
func (r *Response) Str() (string, error) {
	switch v := r.Value.(type) {
	case string:
		return v, nil
	case bool:
		return fmt.Sprintf("%v", v), nil
	case float64:
		return FormatFloat(v), nil
	}
	return "", fmt.Errorf("value %v (%T) is not a string", r.Value, r.Value)
}

// List returns the response value as a slice.
func (r *Response) List() ([]any, error) {
	l, ok := r.Value.([]any)
	if !ok {
		return nil, fmt.Errorf("value %v (%T) is not a list", r.Value, r.Value)
	}
	return l, nil
}

// Strings returns the response value as a slice of strings.
func (r *Response) Strings() ([]string, error) {
	l, err := r.List()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(l))
	for _, item := range l {
		if s, ok := item.(string); ok {
			out = append(out, s)
			continue
		}
		out = append(out, fmt.Sprintf("%v", item))
	}
	return out, nil
}

// Floats returns the response value as a slice of float64, the shape of the
// "levels" subscription payload.
func (r *Response) Floats() ([]float64, error) {
	l, err := r.List()
	if err != nil {
		return nil, err
	}
	out := make([]float64, 0, len(l))
	for i, item := range l {
		f, ok := item.(float64)
		if !ok {
			return nil, fmt.Errorf("list item %d (%v) is not a number", i, item)
		}
		out = append(out, f)
	}
	return out, nil
}

// Bools returns the response value as a slice of bool, the shape of the
// "mutes" subscription payload.
func (r *Response) Bools() ([]bool, error) {
	l, err := r.List()
	if err != nil {
		return nil, err
	}
	out := make([]bool, 0, len(l))
	for i, item := range l {
		b, ok := item.(bool)
		if !ok {
			return nil, fmt.Errorf("list item %d (%v) is not a boolean", i, item)
		}
		out = append(out, b)
	}
	return out, nil
}

// FormatFloat renders a number the way the device would, without a trailing
// ".000000" and without scientific notation.
func FormatFloat(f float64) string {
	return strings.TrimSuffix(strings.TrimRight(fmt.Sprintf("%.6f", f), "0"), ".")
}
