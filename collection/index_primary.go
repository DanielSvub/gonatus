package collection

// PRIMARY INDEX
type primaryIndexer struct {
	index map[CId][]any
}

func primaryIndexerCreate(rows map[CId][]any) *primaryIndexer {
	ego := new(primaryIndexer)
	ego.index = rows

	return ego
}

func (ego *primaryIndexer) Get(arg []any) ([]CId, error) {
	v := arg
	ret := make([]CId, 0)

	for id, row := range ego.index {
		found := true

		for j, col := range row {
			if v[j] == nil {
				continue
			}

			if v[j] == col {
				continue
			}

			found = false
			break
		}

		if found {
			ret = append(ret, id)
		}
	}

	return ret, nil
}

func (ego *primaryIndexer) getPrefix(arg []any) ([]CId, error) {
	ret := make([]CId, 0)

	// iterate over rows
	for id, row := range ego.index {
		found := true

		// iterate over each column
		for j, col := range row {
			if arg[j] == nil {
				continue
			}
			// In case this is the column, where we findin
			if isMatch, length := cmpValues(0, col, arg[j]); !isMatch {
				found = false
				break
			} else {
				for i := 1; i < length; i++ {
					// findin remaining matches
					if isMatch, _ := cmpValues(i, col, arg[j]); !isMatch {
						found = false
						break
					}
				}
			}
		}

		if found {
			ret = append(ret, id)
		}

	}
	return ret, nil
}

func cmpValues(idx int, tableValue any, queryValue any) (bool, int) {

	switch tValue := tableValue.(type) {
	case string:
		if qValue, isMatch := queryValue.(string); isMatch {
			return (len([]rune(tValue)) >= len([]rune(qValue))) && ([]rune(tValue)[idx] == []rune(qValue)[idx]), len([]rune(qValue))
		}
		return false, -1
	case []string:
		if qValue, isMatch := queryValue.([]string); isMatch {
			return (len(tValue) >= len(qValue)) && (tValue[idx] == qValue[idx]), len(qValue)
		}
		return false, -1
	case []int:
		if qValue, isMatch := queryValue.([]int); isMatch {
			return (len(tValue) >= len(qValue)) && (tValue[idx] == qValue[idx]), len(qValue)
		}
		return false, -1
	case []int8:
		if qValue, isMatch := queryValue.([]int8); isMatch {
			return (len(tValue) >= len(qValue)) && (tValue[idx] == qValue[idx]), len(qValue)
		}
		return false, -1
	case []int16:
		if qValue, isMatch := queryValue.([]int16); isMatch {
			return (len(tValue) >= len(qValue)) && (tValue[idx] == qValue[idx]), len(qValue)
		}
		return false, -1
	case []int32:
		if qValue, isMatch := queryValue.([]int32); isMatch {
			return (len(tValue) >= len(qValue)) && (tValue[idx] == qValue[idx]), len(qValue)
		}
		return false, -1
	case []int64:
		if qValue, isMatch := queryValue.([]int64); isMatch {
			return (len(tValue) >= len(qValue)) && (tValue[idx] == qValue[idx]), len(qValue)
		}
		return false, -1
	case []uint:
		if qValue, isMatch := queryValue.([]uint); isMatch {
			return (len(tValue) >= len(qValue)) && (tValue[idx] == qValue[idx]), len(qValue)
		}
		return false, -1
	case []uint8:
		if qValue, isMatch := queryValue.([]uint8); isMatch {
			return (len(tValue) >= len(qValue)) && (tValue[idx] == qValue[idx]), len(qValue)
		}
		return false, -1
	case []uint16:
		if qValue, isMatch := queryValue.([]uint16); isMatch {
			return (len(tValue) >= len(qValue)) && (tValue[idx] == qValue[idx]), len(qValue)
		}
		return false, -1
	case []uint32:
		if qValue, isMatch := queryValue.([]uint32); isMatch {
			return (len(tValue) >= len(qValue)) && (tValue[idx] == qValue[idx]), len(qValue)
		}
		return false, -1
	case []uint64:
		if qValue, isMatch := queryValue.([]uint64); isMatch {
			return (len(tValue) >= len(qValue)) && (tValue[idx] == qValue[idx]), len(qValue)
		}
		return false, -1
	case []float32:
		if qValue, isMatch := queryValue.([]float32); isMatch {
			return (len(tValue) >= len(qValue)) && (tValue[idx] == qValue[idx]), len(qValue)
		}
		return false, -1
	case []float64:
		if qValue, isMatch := queryValue.([]float64); isMatch {
			return (len(tValue) >= len(qValue)) && (tValue[idx] == qValue[idx]), len(qValue)
		}
		return false, -1
	}
	return false, -1
}

// func (ego *primaryIndexer) Add(s any, id CId) error {
// 	val, found := ego.index[id]

// 	if found {
// 		return errors.NewMisappError(ego, "Row with id already registered")
// 	}

// 	ego.index[id] = val
// 	return nil
// }

// func (ego *primaryIndexer) Del(s any, id CId) error {
// 	rows, err := ego.Get(s.([]any))

// 	if err != nil {
// 		return err
// 	}

// 	for _, r := range rows {
// 		delete(ego.index, r)
// 	}

// 	return nil
// }
