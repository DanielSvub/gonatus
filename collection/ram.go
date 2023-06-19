package collection

import (
	"github.com/SpongeData-cz/gonatus"
	"github.com/SpongeData-cz/gonatus/errors"
)

const MaxUint = ^uint(0)
const MinUint = 0
const MaxInt = int(MaxUint >> 1)
const MinInt = -MaxInt - 1

type RamCollectionConf struct {
	SchemaConf
	MaxMemory uint64
}

type ramCollectionIndexer interface {
	gonatus.Gobjecter
	Get(any) ([]CId, error)
	Add(any, CId) error
	Del(any, CId) error
}

type RamCollection struct {
	gonatus.Gobject
	param         RamCollectionConf
	autoincrement CId
	rows          map[CId][]any
	indexes       map[string]ramCollectionIndexer
}

func NewRamCollection(rc RamCollectionConf) *RamCollection {
	ego := new(RamCollection)
	ego.param = rc
	ego.rows = make(map[CId][]any, 0)
	ego.indexes = make(map[string]ramCollectionIndexer, 0)

	return ego
}

func (ego *RamCollection) InterpretField(fc FielderConf) (any, error) {
	switch v := fc.(type) {
	case FieldStringConf:
		return v, nil
	default:
		return nil, errors.NewNotImplError(ego)
	}
}

func (ego *RamCollection) InterpretRecord(rc *Record) ([]any, error) {
	ret := make([]any, 0)

	for c := range rc.Cols {
		rf, err := ego.InterpretField(c)
		if err != nil {
			return nil, err
		}

		ret = append(ret, rf)
	}

	return ret, nil
}

func (ego *RamCollection) AddRecord(rc *Record) (CId, error) {
	ego.autoincrement++

	if ego.autoincrement == CId(MaxUint) {
		return 0, errors.NewValueError(ego, errors.LevelFatal, "Id pool depleted!")
	}

	// TODO: Check mandatory fields

	// TODO: Update default fields

	// Add to main index
	record, err := ego.InterpretRecord(rc)

	if err != nil {
		return 0, err
	}

	ego.rows[ego.autoincrement] = record

	// Add to lookup indexes
	for i, name := range ego.param.FieldsNaming {
		if idx, found := ego.indexes[name]; found {
			idx.Add(record[i], ego.autoincrement)
		}
	}

	return 0, nil
}

func prefixStringIndexImpl(c PrefixStringIndexConf) error {
	return nil
}

type fullmatchStringIndexer struct {
	ramCollectionIndexer
	index map[string][]CId
}

func fullmatchStringIndexerNew(c FullmatchStringIndexConf) *fullmatchStringIndexer {
	ego := new(fullmatchStringIndexer)
	ego.index = make(map[string][]CId)

	return ego
}

func (ego *fullmatchStringIndexer) Get(s any) ([]CId, error) {
	x, found := ego.index[s.(string)]
	if !found {
		return nil, nil
	}

	return x, nil
}

func sliceFind(rows []CId, idx CId) (uint64, bool) {
	for i, v := range rows {
		if v == idx {
			return uint64(i), true
		}
	}

	return uint64(MaxUint), false
}

func (ego *fullmatchStringIndexer) Add(s any, id CId) error {
	val, err := ego.Get(s)

	if err != nil {
		return err
	}

	if val == nil {
		// index record not set yet
		ego.index[s.(string)] = append(make([]CId, 0), id)
		return nil
	}

	if _, found := sliceFind(val, id); found {
		// index already in index record
		return nil
	}

	// extending existing index record by id
	ego.index[s.(string)] = append(val, id)
	return nil
}

func remove(rows []CId, ididx uint64) []CId {
	return append(rows[:ididx], rows[ididx+1:]...)
}

func (ego *fullmatchStringIndexer) Del(s any, id CId) error {
	val, err := ego.Get(s)

	if err != nil {
		return err
	}

	if val == nil {
		return errors.NewNotFoundError(ego, errors.LevelWarning, "Index trouble - value not found")
	}

	idx, found := sliceFind(val, id)

	if !found {
		return errors.NewNotFoundError(ego, errors.LevelWarning, "Index trouble - row not found within index record")
	}

	reduced := remove(val, idx)

	if len(reduced) <= 0 {
		delete(ego.index, s.(string))
		return nil
	}

	ego.index[s.(string)] = reduced
	return nil
}

func (ego *RamCollection) RegisterIndexes() error {
	for _, idx := range ego.param.Indexes {
		switch v := idx.(type) {
		case PrefixStringIndexConf:
			// ego.indexes[v.Name] = prefixStringIndexImpl(v)
			// Not Implemented
		case FullmatchStringIndexConf:
			ego.indexes[v.Name] = fullmatchStringIndexerNew(v)
		default:
			return errors.NewNotImplError(ego)
		}
	}

	return nil
}

func (ego *RamCollection) Serialize() gonatus.Conf {
	return ego.param
}
