package collection

import (
	"github.com/SpongeData-cz/gonatus"
	"github.com/SpongeData-cz/gonatus/errors"
	"github.com/SpongeData-cz/gonatus/streams"
)

const MaxUint = ^uint(0)
const MinUint = 0
const MaxInt = int(MaxUint >> 1)
const MinInt = -MaxInt - 1

type ramCollectionIndexer interface {
	gonatus.Gobjecter
	Get(any) ([]CId, error)
	Add(any, CId) error
	Del(any, CId) error
}

type primaryIndexer struct {
	ramCollectionIndexer
	index map[CId][]any
}

func primaryIndexerCreate(rows map[CId][]any) *primaryIndexer {
	ego := new(primaryIndexer)
	ego.index = rows

	return ego
}

func (ego *primaryIndexer) Get(arg any) ([]CId, error) {
	v := arg.([]any)
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

func (ego *primaryIndexer) Add(s any, id CId) error {
	val, found := ego.index[id]

	if found {
		return errors.NewMisappError(ego, "Row with id already registered")
	}

	ego.index[id] = val
	return nil
}

func (ego *primaryIndexer) Del(s any, id CId) error {
	rows, err := ego.Get(s.([]any))

	if err != nil {
		return err
	}

	for _, r := range rows {
		delete(ego.index, r)
	}

	return nil
}

// FULLMATCH INDEX

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

// /// RAM COLLECTION IMPL
//
// ========================================================
// ******* * * * * * * ************************************
//
//
// ---------------------------------------------- - - - - -

type RamCollectionConf struct {
	SchemaConf
	MaxMemory uint64
}

type RamCollection struct {
	gonatus.Gobject
	param         RamCollectionConf
	autoincrement CId
	rows          map[CId][]any
	indexes       map[string]ramCollectionIndexer // FIXME: make array of indexes for fields not one index as max
}

func NewRamCollection(rc RamCollectionConf) *RamCollection {
	ego := new(RamCollection)
	ego.param = rc
	ego.rows = make(map[CId][]any, 0)
	ego.indexes = make(map[string]ramCollectionIndexer, 0)
	// TODO: implement id index as default one ego.indexes["id"] = idIndexerNew() // must be present in every collection

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

type CIdSet map[CId]bool

func CIdSetFromSlice(s []CId) CIdSet {
	ret := make(CIdSet, 0)

	for _, v := range s {
		ret[v] = true
	}

	return ret
}

func CIdSetToSlice(u CIdSet) []CId {
	keys := make([]CId, len(u))
	i := 0

	for k := range u {
		keys[i] = k
		i++
	}

	return keys
}

func (ego CIdSet) Merge(s CIdSet) {
	for k, v := range s {
		ego[k] = v
	}
}

func (ego CIdSet) Intersect(s CIdSet) CIdSet {
	lesser := ego
	greater := s

	if len(greater) < len(lesser) {
		lesser = s
		greater = ego
	}

	out := make(CIdSet, len(lesser))

	for i, _ := range lesser {
		if greater[i] {
			out[i] = true
		}
	}

	return out
}

func (ego *RamCollection) getIndex(q QueryAtomConf) ramCollectionIndexer {
	if idx, found := ego.indexes[q.Name]; found {
		// index for that name found
		// try cast to the required index

		switch q.MatchType.(type) {
		case PrefixStringIndexConf:
			return nil
		case FullmatchStringIndexConf:
			if i, ok := idx.(*fullmatchStringIndexer); ok {
				return i
			}
		default:
			return idx.(*primaryIndexer)
		}
	}

	return nil
}

func (ego *RamCollection) primaryValue(q QueryAtomConf) []any {
	anys := make([]any, len(ego.param.SchemaConf.FieldsNaming))

	for i, n := range ego.param.SchemaConf.FieldsNaming {
		if n == q.Name {
			anys[i] = q.Value // FIXME: Design hack
			return anys
		}
	}

	return anys
}

func (ego *QueryAtomConf) eval(rc *RamCollection) (CIdSet, error) {
	indexer := rc.getIndex(*ego)
	if pi, ok := indexer.(*primaryIndexer); ok {
		rows, err := pi.Get(rc.primaryValue(*ego))
		if err != nil {
			return nil, err
		}

		return CIdSetFromSlice(rows), nil
	} else {
		rows, err := pi.Get(ego.Value)
		if err != nil {
			return nil, err
		}

		return CIdSetFromSlice(rows), nil
	}
}

func (ego *QueryAndConf) eval(rc *RamCollection) (CIdSet, error) {
	accum := make(CIdSet, 0)
	ctxlen := len(ego.QueryContextConf.Context)

	if ctxlen == 0 {
		return accum, nil
	}

	for i := 0; i < ctxlen; i++ {
		acc, err := rc.filterQueryEval(QueryConf(ego.QueryContextConf.Context[0]))
		accum = accum.Intersect(acc)

		if err != nil {
			return nil, err
		}
		if len(accum) == 0 {
			return make(CIdSet), nil
		}
	}

	return accum, nil
}

func (ego *QueryOrConf) eval(rc *RamCollection) (CIdSet, error) {
	accum := make(CIdSet, 0)
	ctxlen := len(ego.QueryContextConf.Context)

	if ctxlen == 0 {
		return accum, nil
	}

	for i := 0; i < ctxlen; i++ {
		acc, err := rc.filterQueryEval(QueryConf(ego.QueryContextConf.Context[0]))
		accum.Merge(acc)

		if err != nil {
			return nil, err
		}
		if len(accum) == len(rc.rows) {
			break
		}
	}

	return accum, nil
}

func (ego *QueryImplicatonConf) eval(rc *RamCollection) (CIdSet, error) {
	le, err := rc.filterQueryEval(ego.Left)
	if err != nil {
		return nil, err
	}

	re, rerr := rc.filterQueryEval(ego.Right)
	if rerr != nil {
		return nil, rerr
	}

	if len(le) == 0 {
		return re, nil
	}

	// filter out those elements which are on the left hand side and not on right hand side 1 => 0 = 0
	le.Merge(re)

	for i, _ := range le {
		if _, found := re[i]; !found {
			delete(le, i) // possible problem with le modification within for loop
		}
	}

	return le, nil
}

func (ego *RamCollection) filterQueryEval(q QueryConf) (CIdSet, error) {
	switch v := q.(type) {
	case QueryAndConf:
		return v.eval(ego)
	case QueryOrConf:
		return v.eval(ego)
	case QueryImplicatonConf:
		return v.eval(ego)
	case QueryAtomConf:
		return v.eval(ego)
	default:
		return nil, errors.NewMisappError(ego, "Unknown collection filter query.")
	}
}

func (ego *RamCollection) Filter(q QueryConf) (streams.ReadableOutputStreamer[*Record], error) {

	return nil, nil
}

func (ego *RamCollection) RegisterIndexes() error {
	ego.indexes["id"] = primaryIndexerCreate(ego.rows)

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
