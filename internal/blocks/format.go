package blocks

import (
	"fmt"
	"strconv"
	"strings"
)

// FormatBool renders a boolean the way both Tesira and this bridge's MQTT
// topics express one.
func FormatBool(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// ParseBool accepts the spellings people reasonably send to a /set topic.
func ParseBool(payload string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(payload)) {
	case "true", "1", "on", "yes", "y":
		return true, nil
	case "false", "0", "off", "no", "n":
		return false, nil
	}
	return false, fmt.Errorf("%q is not a boolean", payload)
}

// FormatDB renders a decibel value for publication.
func FormatDB(v float64) string { return strconv.FormatFloat(round(v, 2), 'f', -1, 64) }

// FormatPercent renders a percentage for publication.
func FormatPercent(v float64) string { return strconv.FormatFloat(round(v, 1), 'f', -1, 64) }

// FormatInt renders a count for publication.
func FormatInt(v int) string { return strconv.Itoa(v) }

// ParseFloat reads a number from a /set payload.
func ParseFloat(payload string) (float64, error) {
	f, err := strconv.ParseFloat(strings.TrimSpace(payload), 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not a number", payload)
	}
	return f, nil
}

// Clamp confines v to [lo, hi].
func Clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func round(v float64, places int) float64 {
	f, _ := strconv.ParseFloat(fmt.Sprintf("%.*f", places, v), 64)
	return f
}
