// Moved type switches to a separate file due to code clarity.

package collection

import (
	"time"

	"github.com/SpongeData-cz/gonatus/errors"
)

// RAM COLLECTION

/*
Interprets the Field passed in the parameter.

Parameters:
  - fc - FielderConf.

Returns:
  - Value of FieldConf,
  - error, if any.
*/
func (ego *RamCollection) InterpretField(fc FielderConf) (any, error) {
	// TODO: need to copy return value <<v.Value>>

	switch v := fc.(type) {

	case FieldConf[[]string]:
		return v.Value, nil
	case FieldConf[[]int]:
		return v.Value, nil
	case FieldConf[[]int8]:
		return v.Value, nil
	case FieldConf[[]int16]:
		return v.Value, nil
	case FieldConf[[]int32]:
		return v.Value, nil
	case FieldConf[[]int64]:
		return v.Value, nil
	case FieldConf[[]uint]:
		return v.Value, nil
	case FieldConf[[]uint8]:
		return v.Value, nil
	case FieldConf[[]uint16]:
		return v.Value, nil
	case FieldConf[[]uint32]:
		return v.Value, nil
	case FieldConf[[]uint64]:
		return v.Value, nil
	case FieldConf[[]float32]:
		return v.Value, nil
	case FieldConf[[]float64]:
		return v.Value, nil
	case FieldConf[string]:
		return v.Value, nil
	case FieldConf[int]:
		return v.Value, nil
	case FieldConf[int8]:
		return v.Value, nil
	case FieldConf[int16]:
		return v.Value, nil
	case FieldConf[int32]:
		return v.Value, nil
	case FieldConf[int64]:
		return v.Value, nil
	case FieldConf[uint]:
		return v.Value, nil
	case FieldConf[uint8]:
		return v.Value, nil
	case FieldConf[uint16]:
		return v.Value, nil
	case FieldConf[uint32]:
		return v.Value, nil
	case FieldConf[uint64]:
		return v.Value, nil
	case FieldConf[float32]:
		return v.Value, nil
	case FieldConf[float64]:
		return v.Value, nil
	case FieldConf[time.Time]:
		return v.Value, nil
	default:
		return nil, errors.NewNotImplError(ego)
	}
}

/*
Deinterprets the Field on the given index.

Parameters:
  - val - value of FieldConf,
  - nth - index of column.

Returns:
  - FielderConf,
  - error, if any.
*/
func (ego *RamCollection) DeinterpretField(val any, nth int) (FielderConf, error) {
	fc := ego.param.SchemaConf.Fields[nth]

	switch fc.(type) {
	case FieldConf[[]string]:
		return FieldConf[[]string]{Value: val.([]string)}, nil
	case FieldConf[[]int]:
		return FieldConf[[]int]{Value: val.([]int)}, nil
	case FieldConf[[]int8]:
		return FieldConf[[]int8]{Value: val.([]int8)}, nil
	case FieldConf[[]int16]:
		return FieldConf[[]int16]{Value: val.([]int16)}, nil
	case FieldConf[[]int32]:
		return FieldConf[[]int32]{Value: val.([]int32)}, nil
	case FieldConf[[]int64]:
		return FieldConf[[]int64]{Value: val.([]int64)}, nil
	case FieldConf[[]uint]:
		return FieldConf[[]uint]{Value: val.([]uint)}, nil
	case FieldConf[[]uint8]:
		return FieldConf[[]uint8]{Value: val.([]uint8)}, nil
	case FieldConf[[]uint16]:
		return FieldConf[[]uint16]{Value: val.([]uint16)}, nil
	case FieldConf[[]uint32]:
		return FieldConf[[]uint32]{Value: val.([]uint32)}, nil
	case FieldConf[[]uint64]:
		return FieldConf[[]uint64]{Value: val.([]uint64)}, nil
	case FieldConf[[]float32]:
		return FieldConf[[]float32]{Value: val.([]float32)}, nil
	case FieldConf[[]float64]:
		return FieldConf[[]float64]{Value: val.([]float64)}, nil
	case FieldConf[string]:
		return FieldConf[string]{Value: val.(string)}, nil
	case FieldConf[int]:
		return FieldConf[int]{Value: val.(int)}, nil
	case FieldConf[int8]:
		return FieldConf[int8]{Value: val.(int8)}, nil
	case FieldConf[int16]:
		return FieldConf[int16]{Value: val.(int16)}, nil
	case FieldConf[int32]:
		return FieldConf[int32]{Value: val.(int32)}, nil
	case FieldConf[int64]:
		return FieldConf[int64]{Value: val.(int64)}, nil
	case FieldConf[uint]:
		return FieldConf[uint]{Value: val.(uint)}, nil
	case FieldConf[uint8]:
		return FieldConf[uint8]{Value: val.(uint8)}, nil
	case FieldConf[uint16]:
		return FieldConf[uint16]{Value: val.(uint16)}, nil
	case FieldConf[uint32]:
		return FieldConf[uint32]{Value: val.(uint32)}, nil
	case FieldConf[uint64]:
		return FieldConf[uint64]{Value: val.(uint64)}, nil
	case FieldConf[float32]:
		return FieldConf[float32]{Value: val.(float32)}, nil
	case FieldConf[float64]:
		return FieldConf[float64]{Value: val.(float64)}, nil
	case FieldConf[time.Time]:
		return FieldConf[time.Time]{Value: val.(time.Time)}, nil
	default:
		return nil, errors.NewNotImplError(ego)
	}
}

/*
Registers the index to the given column.
Checks for duplicate indexers, type, and column existence.

Returns:
  - Error, if any.
*/
func (ego *RamCollection) registerIndexes() error {
	ego.primaryIndex = primaryIndexerCreate(ego.rows)

	columns := cols{}
	for i, name := range ego.param.FieldsNaming {
		columns[name] = colTuple{fc: ego.param.Fields[i], indexers: 0}
	}

	var name string
	indexes := ego.param.Indexes

	for _, idxcol := range indexes {
		for _, idx := range idxcol {
			switch v := idx.(type) {
			case PrefixIndexConf[string]:
				if _, found := columns[v.Name].fc.(FieldConf[string]); !found || !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], stringPrefixIndexerNew(v))
				name = v.Name
			case PrefixIndexConf[[]string]:
				if _, found := columns[v.Name].fc.(FieldConf[[]string]); !found || !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[string](v))
				name = v.Name
			case PrefixIndexConf[[]int]:
				if _, found := columns[v.Name].fc.(FieldConf[[]int]); !found || !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[int](v))
				name = v.Name
			case PrefixIndexConf[[]int8]:
				if _, found := columns[v.Name].fc.(FieldConf[[]int8]); !found || !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[int8](v))
				name = v.Name
			case PrefixIndexConf[[]int16]:
				if _, found := columns[v.Name].fc.(FieldConf[[]int16]); !found || !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[int16](v))
				name = v.Name
			case PrefixIndexConf[[]int32]:
				if _, found := columns[v.Name].fc.(FieldConf[[]int32]); !found || !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[int32](v))
				name = v.Name
			case PrefixIndexConf[[]int64]:
				if _, found := columns[v.Name].fc.(FieldConf[[]int64]); !found || !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[int64](v))
				name = v.Name
			case PrefixIndexConf[[]uint]:
				if _, found := columns[v.Name].fc.(FieldConf[[]uint]); !found || !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[uint](v))
				name = v.Name
			case PrefixIndexConf[[]uint8]:
				if _, found := columns[v.Name].fc.(FieldConf[[]uint8]); !found || !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[uint8](v))
				name = v.Name
			case PrefixIndexConf[[]uint16]:
				if _, found := columns[v.Name].fc.(FieldConf[[]uint16]); !found || !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[uint16](v))
				name = v.Name
			case PrefixIndexConf[[]uint32]:
				if _, found := columns[v.Name].fc.(FieldConf[[]uint32]); !found || !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[uint32](v))
				name = v.Name
			case PrefixIndexConf[[]uint64]:
				if _, found := columns[v.Name].fc.(FieldConf[[]uint64]); !found || !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[uint64](v))
				name = v.Name
			case PrefixIndexConf[[]float32]:
				if _, found := columns[v.Name].fc.(FieldConf[[]float32]); !found || !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[float32](v))
				name = v.Name
			case PrefixIndexConf[[]float64]:
				if _, found := columns[v.Name].fc.(FieldConf[[]float64]); !found || !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[float64](v))
				name = v.Name
			case FullmatchIndexConf[string]:
				if _, found := columns[v.Name].fc.(FieldConf[string]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[string](v))
				name = v.Name
			case FullmatchIndexConf[int]:
				if _, found := columns[v.Name].fc.(FieldConf[int]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[int](v))
				name = v.Name
			case FullmatchIndexConf[int8]:
				if _, found := columns[v.Name].fc.(FieldConf[int8]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[int8](v))
				name = v.Name
			case FullmatchIndexConf[int16]:
				if _, found := columns[v.Name].fc.(FieldConf[int16]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[int16](v))
				name = v.Name
			case FullmatchIndexConf[int32]:
				if _, found := columns[v.Name].fc.(FieldConf[int32]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[int32](v))
				name = v.Name
			case FullmatchIndexConf[int64]:
				if _, found := columns[v.Name].fc.(FieldConf[int64]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[int64](v))
				name = v.Name
			case FullmatchIndexConf[uint]:
				if _, found := columns[v.Name].fc.(FieldConf[uint]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[uint](v))
				name = v.Name
			case FullmatchIndexConf[uint8]:
				if _, found := columns[v.Name].fc.(FieldConf[uint8]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[uint8](v))
				name = v.Name
			case FullmatchIndexConf[uint16]:
				if _, found := columns[v.Name].fc.(FieldConf[uint16]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[uint16](v))
				name = v.Name
			case FullmatchIndexConf[uint32]:
				if _, found := columns[v.Name].fc.(FieldConf[uint32]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[uint32](v))
				name = v.Name
			case FullmatchIndexConf[uint64]:
				if _, found := columns[v.Name].fc.(FieldConf[uint64]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[uint64](v))
				name = v.Name
			case FullmatchIndexConf[float32]:
				if _, found := columns[v.Name].fc.(FieldConf[float32]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[float32](v))
				name = v.Name
			case FullmatchIndexConf[float64]:
				if _, found := columns[v.Name].fc.(FieldConf[float64]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[float64](v))
				name = v.Name
			case FullmatchIndexConf[time.Time]:
				if _, found := columns[v.Name].fc.(FieldConf[time.Time]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[time.Time](v))
				name = v.Name
			case FullmatchIndexConf[[]string]:
				if _, found := columns[v.Name].fc.(FieldConf[[]string]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[string](v))
				name = v.Name
			case FullmatchIndexConf[[]int]:
				if _, found := columns[v.Name].fc.(FieldConf[[]int]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[int](v))
				name = v.Name
			case FullmatchIndexConf[[]int8]:
				if _, found := columns[v.Name].fc.(FieldConf[[]int8]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[int8](v))
				name = v.Name
			case FullmatchIndexConf[[]int16]:
				if _, found := columns[v.Name].fc.(FieldConf[[]int16]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[int16](v))
				name = v.Name
			case FullmatchIndexConf[[]int32]:
				if _, found := columns[v.Name].fc.(FieldConf[[]int32]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[int32](v))
				name = v.Name
			case FullmatchIndexConf[[]int64]:
				if _, found := columns[v.Name].fc.(FieldConf[[]int64]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[int64](v))
				name = v.Name
			case FullmatchIndexConf[[]uint]:
				if _, found := columns[v.Name].fc.(FieldConf[[]uint]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[uint](v))
				name = v.Name
			case FullmatchIndexConf[[]uint8]:
				if _, found := columns[v.Name].fc.(FieldConf[[]uint8]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[uint8](v))
				name = v.Name
			case FullmatchIndexConf[[]uint16]:
				if _, found := columns[v.Name].fc.(FieldConf[[]uint16]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[uint16](v))
				name = v.Name
			case FullmatchIndexConf[[]uint32]:
				if _, found := columns[v.Name].fc.(FieldConf[[]uint32]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[uint32](v))
				name = v.Name
			case FullmatchIndexConf[[]uint64]:
				if _, found := columns[v.Name].fc.(FieldConf[[]uint64]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[uint64](v))
				name = v.Name
			case FullmatchIndexConf[[]float32]:
				if _, found := columns[v.Name].fc.(FieldConf[[]float32]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[float32](v))
				name = v.Name
			case FullmatchIndexConf[[]float64]:
				if _, found := columns[v.Name].fc.(FieldConf[[]float64]); !found || !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[float64](v))
				name = v.Name
			default:
				return errors.NewNotImplError(ego)
			}
		}
	}
	if len(indexes) > 0 && len(indexes[0]) > 0 && !ego.checkName(name) {
		return errors.NewNotImplError(ego)
	}

	return nil
}

/*
Compares the indexer kind specified in the query
and the indexers specified in the RamCollection.

Parameters:
  - qIdx - Query indexer,
  - iidx - RamCollection indexer.

Returns:
  - True, if match, false otherwise.
*/
func cmpIndexKind(qIdx IndexerConf, iidx ramCollectionIndexer) bool {
	switch qIdx.(type) {
	case PrefixIndexConf[string]:
		_, ok := iidx.(*stringPrefixIndexer)
		if ok {
			return true
		}
	case PrefixIndexConf[[]string]:
		indexer, ok := iidx.(*prefixIndexer[string])
		if ok && !indexer.ignoreChildren {
			return true
		}
	case PrefixIndexConf[[]int]:
		indexer, ok := iidx.(*prefixIndexer[int])
		if ok && !indexer.ignoreChildren {
			return true
		}
	case PrefixIndexConf[[]int8]:
		indexer, ok := iidx.(*prefixIndexer[int8])
		if ok && !indexer.ignoreChildren {
			return true
		}
	case PrefixIndexConf[[]int16]:
		indexer, ok := iidx.(*prefixIndexer[int16])
		if ok && !indexer.ignoreChildren {
			return true
		}
	case PrefixIndexConf[[]int32]:
		indexer, ok := iidx.(*prefixIndexer[int32])
		if ok && !indexer.ignoreChildren {
			return true
		}
	case PrefixIndexConf[[]int64]:
		indexer, ok := iidx.(*prefixIndexer[int64])
		if ok && !indexer.ignoreChildren {
			return true
		}
	case PrefixIndexConf[[]uint]:
		indexer, ok := iidx.(*prefixIndexer[uint])
		if ok && !indexer.ignoreChildren {
			return true
		}
	case PrefixIndexConf[[]uint8]:
		indexer, ok := iidx.(*prefixIndexer[uint8])
		if ok && !indexer.ignoreChildren {
			return true
		}
	case PrefixIndexConf[[]uint16]:
		indexer, ok := iidx.(*prefixIndexer[uint16])
		if ok && !indexer.ignoreChildren {
			return true
		}
	case PrefixIndexConf[[]uint32]:
		indexer, ok := iidx.(*prefixIndexer[uint32])
		if ok && !indexer.ignoreChildren {
			return true
		}
	case PrefixIndexConf[[]uint64]:
		indexer, ok := iidx.(*prefixIndexer[uint64])
		if ok && !indexer.ignoreChildren {
			return true
		}
	case PrefixIndexConf[[]float32]:
		indexer, ok := iidx.(*prefixIndexer[float32])
		if ok && !indexer.ignoreChildren {
			return true
		}
	case PrefixIndexConf[[]float64]:
		indexer, ok := iidx.(*prefixIndexer[float64])
		if ok && !indexer.ignoreChildren {
			return true
		}
	case FullmatchIndexConf[string]:
		_, ok := iidx.(*fullmatchIndexer[string])
		if ok {
			return true
		}
	case FullmatchIndexConf[int]:
		_, ok := iidx.(*fullmatchIndexer[int])
		if ok {
			return true
		}
	case FullmatchIndexConf[int8]:
		_, ok := iidx.(*fullmatchIndexer[int8])
		if ok {
			return true
		}
	case FullmatchIndexConf[int16]:
		_, ok := iidx.(*fullmatchIndexer[int16])
		if ok {
			return true
		}
	case FullmatchIndexConf[int32]:
		_, ok := iidx.(*fullmatchIndexer[int32])
		if ok {
			return true
		}
	case FullmatchIndexConf[int64]:
		_, ok := iidx.(*fullmatchIndexer[int64])
		if ok {
			return true
		}
	case FullmatchIndexConf[uint]:
		_, ok := iidx.(*fullmatchIndexer[uint])
		if ok {
			return true
		}
	case FullmatchIndexConf[uint8]:
		_, ok := iidx.(*fullmatchIndexer[uint8])
		if ok {
			return true
		}
	case FullmatchIndexConf[uint16]:
		_, ok := iidx.(*fullmatchIndexer[uint16])
		if ok {
			return true
		}
	case FullmatchIndexConf[uint32]:
		_, ok := iidx.(*fullmatchIndexer[uint32])
		if ok {
			return true
		}
	case FullmatchIndexConf[uint64]:
		_, ok := iidx.(*fullmatchIndexer[uint64])
		if ok {
			return true
		}
	case FullmatchIndexConf[float32]:
		_, ok := iidx.(*fullmatchIndexer[float32])
		if ok {
			return true
		}
	case FullmatchIndexConf[float64]:
		_, ok := iidx.(*fullmatchIndexer[float64])
		if ok {
			return true
		}
	case FullmatchIndexConf[time.Time]:
		_, ok := iidx.(*fullmatchIndexer[time.Time])
		if ok {
			return true
		}
	case FullmatchIndexConf[[]string]:
		indexer, ok := iidx.(*prefixIndexer[string])
		if ok && indexer.ignoreChildren {
			return true
		}
	case FullmatchIndexConf[[]int]:
		indexer, ok := iidx.(*prefixIndexer[int])
		if ok && indexer.ignoreChildren {
			return true
		}
	case FullmatchIndexConf[[]int8]:
		indexer, ok := iidx.(*prefixIndexer[int8])
		if ok && indexer.ignoreChildren {
			return true
		}
	case FullmatchIndexConf[[]int16]:
		indexer, ok := iidx.(*prefixIndexer[int16])
		if ok && indexer.ignoreChildren {
			return true
		}
	case FullmatchIndexConf[[]int32]:
		indexer, ok := iidx.(*prefixIndexer[int32])
		if ok && indexer.ignoreChildren {
			return true
		}
	case FullmatchIndexConf[[]int64]:
		indexer, ok := iidx.(*prefixIndexer[int64])
		if ok && indexer.ignoreChildren {
			return true
		}
	case FullmatchIndexConf[[]uint]:
		indexer, ok := iidx.(*prefixIndexer[uint])
		if ok && indexer.ignoreChildren {
			return true
		}
	case FullmatchIndexConf[[]uint8]:
		indexer, ok := iidx.(*prefixIndexer[uint8])
		if ok && indexer.ignoreChildren {
			return true
		}
	case FullmatchIndexConf[[]uint16]:
		indexer, ok := iidx.(*prefixIndexer[uint16])
		if ok && indexer.ignoreChildren {
			return true
		}
	case FullmatchIndexConf[[]uint32]:
		indexer, ok := iidx.(*prefixIndexer[uint32])
		if ok && indexer.ignoreChildren {
			return true
		}
	case FullmatchIndexConf[[]uint64]:
		indexer, ok := iidx.(*prefixIndexer[uint64])
		if ok && indexer.ignoreChildren {
			return true
		}
	case FullmatchIndexConf[[]float32]:
		indexer, ok := iidx.(*prefixIndexer[float32])
		if ok && indexer.ignoreChildren {
			return true
		}
	case FullmatchIndexConf[[]float64]:
		indexer, ok := iidx.(*prefixIndexer[float64])
		if ok && indexer.ignoreChildren {
			return true
		}
	}
	return false
}

// QUERY

/*
Checks if the MatchType is null. If not, it checks if
it is a PrefixIndexConf[T] and also checks if the
type [T] matches the type of the column it is bound to.

Parameters:
  - rc - RamCollection for check FieldConf types,
  - idx - The column index to be checked.

Returns:
  - True, if MatchType == PrefixIndexConf[T] and the types match, false otherwise.
*/
func (ego *QueryAtomConf) isPrefix(rc *RamCollection, idx int) bool {

	if ego.MatchType == nil {
		return false
	}

	switch ego.MatchType.(type) {
	case PrefixIndexConf[string]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[string]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]string]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]string]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]int]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]int]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]int8]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]int8]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]int16]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]int16]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]int32]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]int32]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]int64]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]int64]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]uint]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]uint]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]uint8]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]uint8]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]uint16]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]uint16]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]uint32]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]uint32]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]uint64]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]uint64]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]float32]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]float32]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]float64]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]float64]); !isMatch {
			return false
		}
		return true
	default:
		return false
	}
}

/*
Checks if the MatchType is null. If not, it checks if
it is a FullmatchIndexConf[T] and also checks if the
type [T] matches the type of the column it is bound to.

Parameters:
  - rc - RamCollection for check FieldConf types,
  - idx - The column index to be checked.

Returns:
  - True, if MatchType == FullmatchIndexConf[T] and the types match, false otherwise.
*/
func (ego *QueryAtomConf) isFullmatch(rc *RamCollection, idx int) bool {

	if ego.MatchType == nil {
		return false
	}

	switch ego.MatchType.(type) {
	case FullmatchIndexConf[string]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[string]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[int]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[int]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[int8]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[int8]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[int16]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[int16]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[int32]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[int32]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[int64]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[int64]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[uint]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[uint]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[uint8]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[uint8]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[uint16]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[uint16]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[uint32]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[uint32]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[uint64]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[uint64]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[float32]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[float32]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[float64]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[float64]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[time.Time]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[time.Time]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[[]string]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]string]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[[]int]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]int]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[[]int8]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]int8]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[[]int16]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]int16]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[[]int32]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]int32]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[[]int64]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]int64]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[[]uint]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]uint]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[[]uint8]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]uint8]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[[]uint16]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]uint16]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[[]uint32]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]uint32]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[[]uint64]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]uint64]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[[]float32]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]float32]); !isMatch {
			return false
		}
		return true
	case FullmatchIndexConf[[]float64]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]float64]); !isMatch {
			return false
		}
		return true
	default:
		return false
	}
}

// PRIMARY INDEX

/*
Compares the value from the RamCollection table and the value from the query.
Checks if it is the same type. If so, it returns whether the values at the given
index match and the length of the value from the query.

Parameters:
  - idx - Which index should be checked,
  - tableValue - RamCollection table value,
  - queryValue - query value.

Returns:
  - True, if the values match and if len(tableValue) >= len(queryValue), false otherwise,
  - lenght of the value from the query.
*/
func cmpPrefixValues(idx int, tableValue any, queryValue any) (bool, int) {

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

/*
Compares the value from the RamCollection table and the value from the query.
Checks if it is the same type. If so, it returns whether the values match.

Parameters:
  - tableValue - RamCollection table value,
  - queryValue - query value.

Returns:
  - True, if the values match, false otherwise.
*/
func cmpFullmatchValues(tableValue any, queryValue any) bool {

	switch tValue := tableValue.(type) {
	case string:
		return tableValue == queryValue
	case int:
		return tableValue == queryValue
	case int8:
		return tableValue == queryValue
	case int16:
		return tableValue == queryValue
	case int32:
		return tableValue == queryValue
	case int64:
		return tableValue == queryValue
	case uint:
		return tableValue == queryValue
	case uint8:
		return tableValue == queryValue
	case uint16:
		return tableValue == queryValue
	case uint32:
		return tableValue == queryValue
	case uint64:
		return tableValue == queryValue
	case float32:
		return tableValue == queryValue
	case float64:
		return tableValue == queryValue
	case time.Time:
		if qValue, isMatch := queryValue.(time.Time); isMatch {
			return tValue.Equal(qValue)
		}
		return false
	case []string:
		if qValue, isMatch := queryValue.([]string); isMatch && (len(qValue) == len(tValue)) {
			for i, elem := range tValue {
				if elem != qValue[i] {
					return false
				}
			}
			return true
		}
		return false
	case []int:
		if qValue, isMatch := queryValue.([]int); isMatch && (len(qValue) == len(tValue)) && (len(qValue) == len(tValue)) {
			for i, elem := range tValue {
				if elem != qValue[i] {
					return false
				}
			}
			return true
		}
		return false
	case []int8:
		if qValue, isMatch := queryValue.([]int8); isMatch && (len(qValue) == len(tValue)) {
			for i, elem := range tValue {
				if elem != qValue[i] {
					return false
				}
			}
			return true
		}
		return false
	case []int16:
		if qValue, isMatch := queryValue.([]int16); isMatch && (len(qValue) == len(tValue)) {
			for i, elem := range tValue {
				if elem != qValue[i] {
					return false
				}
			}
			return true
		}
		return false
	case []int32:
		if qValue, isMatch := queryValue.([]int32); isMatch && (len(qValue) == len(tValue)) {
			for i, elem := range tValue {
				if elem != qValue[i] {
					return false
				}
			}
			return true
		}
		return false
	case []int64:
		if qValue, isMatch := queryValue.([]int64); isMatch && (len(qValue) == len(tValue)) {
			for i, elem := range tValue {
				if elem != qValue[i] {
					return false
				}
			}
			return true
		}
		return false
	case []uint:
		if qValue, isMatch := queryValue.([]uint); isMatch && (len(qValue) == len(tValue)) {
			for i, elem := range tValue {
				if elem != qValue[i] {
					return false
				}
			}
			return true
		}
		return false
	case []uint8:
		if qValue, isMatch := queryValue.([]uint8); isMatch && (len(qValue) == len(tValue)) {
			for i, elem := range tValue {
				if elem != qValue[i] {
					return false
				}
			}
			return true
		}
		return false
	case []uint16:
		if qValue, isMatch := queryValue.([]uint16); isMatch && (len(qValue) == len(tValue)) {
			for i, elem := range tValue {
				if elem != qValue[i] {
					return false
				}
			}
			return true
		}
		return false
	case []uint32:
		if qValue, isMatch := queryValue.([]uint32); isMatch && (len(qValue) == len(tValue)) {
			for i, elem := range tValue {
				if elem != qValue[i] {
					return false
				}
			}
			return true
		}
		return false
	case []uint64:
		if qValue, isMatch := queryValue.([]uint64); isMatch && (len(qValue) == len(tValue)) {
			for i, elem := range tValue {
				if elem != qValue[i] {
					return false
				}
			}
			return true
		}
		return false
	case []float32:
		if qValue, isMatch := queryValue.([]float32); isMatch && (len(qValue) == len(tValue)) {
			for i, elem := range tValue {
				if elem != qValue[i] {
					return false
				}
			}
			return true
		}
		return false
	case []float64:
		if qValue, isMatch := queryValue.([]float64); isMatch && (len(qValue) == len(tValue)) {
			for i, elem := range tValue {
				if elem != qValue[i] {
					return false
				}
			}
			return true
		}
		return false
	}
	return false
}
