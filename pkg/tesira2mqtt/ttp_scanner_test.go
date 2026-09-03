package tesira2mqtt

import "testing"

func TestParseTTPValue(t *testing.T) {
	ret, err := ParseTTPValue(`"value":[-100.000000 -100.000000 -100.000000 -100.000000 -100.000000 -100.000000 -100.000000 -100.000000]`)

	if err != nil {
		t.Fatalf("ParseTTPValue failed: %v", err)
	}

	if obj, ok := ret.(map[string]any); !ok {
		t.Fatalf("ParseTTPValue returned unexpected type: %T", ret)
	} else {
		if value, ok := obj["value"]; !ok {
			t.Fatalf("ParseTTPValue returned object without 'value' key")
		} else if arr, ok := value.([]any); !ok {
			t.Fatalf("ParseTTPValue returned 'value' of unexpected type: %T", value)
		} else if len(arr) != 8 {
			t.Fatalf("ParseTTPValue returned 'value' array of unexpected length: %d", len(arr))
		} else {
			for i, v := range arr {
				if f, ok := v.(float64); !ok {
					t.Fatalf("ParseTTPValue returned 'value' array element %d of unexpected type: %T", i, v)
				} else if f != -100.0 {
					t.Fatalf("ParseTTPValue returned 'value' array element %d with unexpected value: %f", i, f)
				}
			}
		}

	}
}
