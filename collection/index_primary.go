package collection

// PRIMARY INDEX
type primaryIndexer struct {
	index map[CId][]any
}

/*
Creates new primaryIndexer.

Parameters:
  - rows - Rows of RamCollection.

Returns:
  - pointer to a new instance of primaryIndexer.
*/
func primaryIndexerCreate(rows map[CId][]any) *primaryIndexer {
	ego := new(primaryIndexer)
	ego.index = rows

	return ego
}

/*
Searches for rows that full match the specified patter in <<arg>>.

Parameters:
  - arg -  Array (len(Array) == len(columns)) filled with null except for the index of the search column that contains the pattern.

Returns:
  - CIds of rows that match,
  - error, if any.
*/
func (ego *primaryIndexer) Get(arg []any) ([]CId, error) {
	v := arg
	ret := make([]CId, 0)

	for id, row := range ego.index {
		found := true

		for j, col := range row {
			if v[j] == nil {
				continue
			}

			if cmpFullmatchValues(col, v[j]) {
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

/*
Prefix searches for rows that match the specified patter in <<arg>>.

Parameters:
  - arg -  Array (len(Array) == len(columns)) filled with null except for the index of the search column that contains the pattern.

Returns:
  - CId of rows that match,
  - error, if any.
*/
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
			if isMatch, length := cmpPrefixValues(0, col, arg[j]); !isMatch {
				found = false
				break
			} else {
				for i := 1; i < length; i++ {
					// findin remaining matches
					if isMatch, _ := cmpPrefixValues(i, col, arg[j]); !isMatch {
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
