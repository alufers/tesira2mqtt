package tesira2mqtt

func toFloat64Slice(value any) ([]float64, bool) {
	if arr, ok := value.([]any); ok {
		result := make([]float64, len(arr))
		for i, v := range arr {
			if f, ok := v.(float64); ok {
				result[i] = f
			} else {
				return nil, false
			}
		}
		return result, true
	}
	return nil, false
}
func toBoolSlice(value any) ([]bool, bool) {
	if arr, ok := value.([]any); ok {
		result := make([]bool, len(arr))
		for i, v := range arr {
			if b, ok := v.(bool); ok {
				result[i] = b
			} else {
				return nil, false
			}
		}
		return result, true
	}
	return nil, false
}
