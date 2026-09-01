package ttp

import (
	"reflect"
	"testing"
)

func TestParseValue(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want any
	}{
		{"quoted string", `"HSKRK-Datacenter-Tesira"`, "HSKRK-Datacenter-Tesira"},
		{"negative float", `-72.699997`, -72.699997},
		{"integer", `2`, float64(2)},
		{"bare true", `true`, true},
		{"bare false", `false`, false},
		{"float list", `[-72.699997 -72.699997]`, []any{-72.699997, -72.699997}},
		{"bool list", `[false false]`, []any{false, false}},
		{"string list", `["AudioMeter1" "DEVICE" "level_anc"]`, []any{"AudioMeter1", "DEVICE", "level_anc"}},
		{"empty list", `[]`, []any{}},
		{"dict", `{"a":1 "b":true}`, map[string]any{"a": float64(1), "b": true}},
		{"nested", `{"x":[1 2] "y":{"z":"q"}}`, map[string]any{
			"x": []any{float64(1), float64(2)},
			"y": map[string]any{"z": "q"},
		}},
		{"escaped quote", `"say \"hi\""`, `say "hi"`},
		{"bare word", `INVALID_PARAMETER`, "INVALID_PARAMETER"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseValue(tc.in)
			if err != nil {
				t.Fatalf("ParseValue(%q) error: %v", tc.in, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("ParseValue(%q) = %#v, want %#v", tc.in, got, tc.want)
			}
		})
	}
}

func TestParseValueRejectsGarbage(t *testing.T) {
	for _, in := range []string{``, `[1 2`, `{"a":1`, `"unterminated`} {
		if _, err := ParseValue(in); err == nil {
			t.Errorf("ParseValue(%q) succeeded, want error", in)
		}
	}
}

func TestParseDictBodyKeepsFirstDuplicateKey(t *testing.T) {
	got, err := ParseDictBody(`"fault":"a" "fault":"b"`)
	if err != nil {
		t.Fatal(err)
	}
	if got["fault"] != "a" {
		t.Errorf(`fault = %v, want "a"`, got["fault"])
	}
}

// Every line below was captured verbatim from a Tesira TESIRAFORTE at
// 10.12.10.104 running with "verbose true" and "detailedResponse true".
func TestParseLine(t *testing.T) {
	tests := []struct {
		name     string
		in       string
		wantOK   bool
		kind     Kind
		command  string
		value    any
		hasValue bool
		token    string
		errText  string
	}{
		{
			name: "bare ok", in: `+OK`,
			wantOK: true, kind: KindOK,
		},
		{
			name: "value with echo", in: `+OK [ level_anc get label 1 ] "value":"Chan 1"`,
			wantOK: true, kind: KindOK, command: "level_anc get label 1",
			value: "Chan 1", hasValue: true,
		},
		{
			name: "float value", in: `+OK [ level_anc get level 1 ] "value":-76.800003`,
			wantOK: true, kind: KindOK, command: "level_anc get level 1",
			value: -76.800003, hasValue: true,
		},
		{
			name: "list response", in: `+OK [ SESSION get aliases ] "list":["DEVICE" "level_anc"]`,
			wantOK: true, kind: KindOK, command: "SESSION get aliases",
			value: []any{"DEVICE", "level_anc"}, hasValue: true,
		},
		{
			name: "ok with echo only", in: `+OK [ SESSION set detailedResponse false ]`,
			wantOK: true, kind: KindOK, command: "SESSION set detailedResponse false",
		},
		{
			name: "value without echo", in: `+OK "value":2`,
			wantOK: true, kind: KindOK, value: float64(2), hasValue: true,
		},
		{
			name: "non-verbose bare value", in: `+OK true`,
			wantOK: true, kind: KindOK, value: true, hasValue: true,
		},
		{
			name: "block type error", in: `-ERR [ level_anc get BLOCKTYPE ] 'BLOCKTYPE' is not supported by LevelControlInterface::Attributes`,
			wantOK: true, kind: KindError, command: "level_anc get BLOCKTYPE",
			errText: `'BLOCKTYPE' is not supported by LevelControlInterface::Attributes`,
		},
		{
			name: "index out of range", in: `-ERR [ RoomCombiner1 get wallState 0 ] INVALID_PARAMETER Index out of range:wallId min:1 max:8 received:0`,
			wantOK: true, kind: KindError, command: "RoomCombiner1 get wallState 0",
			errText: `INVALID_PARAMETER Index out of range:wallId min:1 max:8 received:0`,
		},
		{
			name: "error without echo", in: `-ERR Parse error at 35: could not parse value`,
			wantOK: true, kind: KindError, errText: `Parse error at 35: could not parse value`,
		},
		{
			name: "publish bool", in: `! "publishToken":"S1_wallState_1_RoomCombiner1" "value":true`,
			wantOK: true, kind: KindPublish, token: "S1_wallState_1_RoomCombiner1",
			value: true, hasValue: true,
		},
		{
			name: "publish level array", in: `! "publishToken":"S2_levels_ALL_level_anc" "value":[-76.800003 -76.800003]`,
			wantOK: true, kind: KindPublish, token: "S2_levels_ALL_level_anc",
			value: []any{-76.800003, -76.800003}, hasValue: true,
		},
		{name: "banner", in: `Welcome to the Tesira Text Protocol Server...`},
		{name: "last login", in: `Last login: Tue Sep  1 22:23:55 2026 from 10.12.10.127`},
		{name: "terminal echo", in: `level_anc get level 1`},
		{name: "blank", in: `   `},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseLine(tc.in)
			if ok != tc.wantOK {
				t.Fatalf("ParseLine(%q) ok = %v, want %v", tc.in, ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if got.Kind != tc.kind {
				t.Errorf("Kind = %v, want %v", got.Kind, tc.kind)
			}
			if got.Command != tc.command {
				t.Errorf("Command = %q, want %q", got.Command, tc.command)
			}
			if got.HasValue != tc.hasValue {
				t.Errorf("HasValue = %v, want %v", got.HasValue, tc.hasValue)
			}
			if tc.hasValue && !reflect.DeepEqual(got.Value, tc.value) {
				t.Errorf("Value = %#v, want %#v", got.Value, tc.value)
			}
			if got.PublishToken != tc.token {
				t.Errorf("PublishToken = %q, want %q", got.PublishToken, tc.token)
			}
			if got.Error != tc.errText {
				t.Errorf("Error = %q, want %q", got.Error, tc.errText)
			}
		})
	}
}

func TestResponseAccessors(t *testing.T) {
	r, _ := ParseLine(`! "publishToken":"t" "value":[-76.800003 -1.5]`)
	floats, err := r.Floats()
	if err != nil || !reflect.DeepEqual(floats, []float64{-76.800003, -1.5}) {
		t.Fatalf("Floats() = %v, %v", floats, err)
	}
	if _, err := r.Bools(); err == nil {
		t.Error("Bools() on a float list should fail")
	}

	r, _ = ParseLine(`! "publishToken":"t" "value":[false true]`)
	bools, err := r.Bools()
	if err != nil || !reflect.DeepEqual(bools, []bool{false, true}) {
		t.Fatalf("Bools() = %v, %v", bools, err)
	}

	r, _ = ParseLine(`+OK "value":2`)
	if n, err := r.Int(); err != nil || n != 2 {
		t.Fatalf("Int() = %v, %v", n, err)
	}

	r, _ = ParseLine(`+OK "list":["a" "b"]`)
	if ss, err := r.Strings(); err != nil || !reflect.DeepEqual(ss, []string{"a", "b"}) {
		t.Fatalf("Strings() = %v, %v", ss, err)
	}
}

func TestParseIndexRange(t *testing.T) {
	tests := []struct {
		in       string
		wantOK   bool
		name     string
		min, max int
	}{
		{
			in:     `INVALID_PARAMETER Index out of range:wallId min:1 max:8 received:0`,
			wantOK: true, name: "wallId", min: 1, max: 8,
		},
		{
			in:     `INVALID_PARAMETER Index out of range:channelIdx min:1 max:8 received:0`,
			wantOK: true, name: "channelIdx", min: 1, max: 8,
		},
		{
			in:     `INVALID_PARAMETER Index out of range:channelIndex min:1 max:2 received:0`,
			wantOK: true, name: "channelIndex", min: 1, max: 2,
		},
		{in: `'BLOCKTYPE' is not supported by LevelControlInterface::Attributes`},
	}
	for _, tc := range tests {
		got, ok := ParseIndexRange(tc.in)
		if ok != tc.wantOK {
			t.Fatalf("ParseIndexRange(%q) ok = %v, want %v", tc.in, ok, tc.wantOK)
		}
		if !ok {
			continue
		}
		if got.Name != tc.name || got.Min != tc.min || got.Max != tc.max {
			t.Errorf("ParseIndexRange(%q) = %+v", tc.in, got)
		}
	}
}

func TestFormatFloat(t *testing.T) {
	for in, want := range map[float64]string{
		-76.800003: "-76.800003",
		-92:        "-92",
		2:          "2",
		-40.5:      "-40.5",
	} {
		if got := FormatFloat(in); got != want {
			t.Errorf("FormatFloat(%v) = %q, want %q", in, got, want)
		}
	}
}
