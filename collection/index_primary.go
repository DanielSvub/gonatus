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
