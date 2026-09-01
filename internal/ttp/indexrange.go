package ttp

import (
	"regexp"
	"strconv"
)

// The device reports the valid bounds of an index inside its error text:
//
//	-ERR INVALID_PARAMETER Index out of range:wallId min:1 max:8 received:0
//
// Room Combiner blocks expose no numWalls/numRooms attribute, so deliberately
// asking for index 0 and reading the bounds out of the rejection is how a
// block's wall and room counts are discovered.
var indexRangeRe = regexp.MustCompile(`Index out of range:\s*(\S+)\s+min:\s*(-?\d+)\s+max:\s*(-?\d+)`)

// IndexRange is the valid index range of an indexed attribute.
type IndexRange struct {
	Name string
	Min  int
	Max  int
}

// ParseIndexRange extracts the bounds from an "Index out of range" error text.
func ParseIndexRange(errText string) (IndexRange, bool) {
	m := indexRangeRe.FindStringSubmatch(errText)
	if m == nil {
		return IndexRange{}, false
	}
	min, err1 := strconv.Atoi(m[2])
	max, err2 := strconv.Atoi(m[3])
	if err1 != nil || err2 != nil {
		return IndexRange{}, false
	}
	return IndexRange{Name: m[1], Min: min, Max: max}, true
}
