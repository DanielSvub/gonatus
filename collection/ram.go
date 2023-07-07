package collection

import (
	"fmt"
	"time"

	"github.com/SpongeData-cz/gonatus"
	"github.com/SpongeData-cz/gonatus/errors"
	"github.com/SpongeData-cz/gonatus/fs"
	"github.com/SpongeData-cz/gonatus/streams"
)

const MaxUint = ^uint64(0)
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

// FULLMATCH INDEX

type fullmatchIndexer[T comparable] struct {
	ramCollectionIndexer
	index map[T][]CId
}

func fullmatchIndexerNew[T comparable](c FullmatchIndexConf[T]) *fullmatchIndexer[T] {
	ego := new(fullmatchIndexer[T])
	ego.index = make(map[T][]CId)

	return ego
}

func (ego *fullmatchIndexer[T]) Get(v any) ([]CId, error) {
	s := v.(T)
	x, found := ego.index[s]
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

func sliceAddUnique(slice []CId, cid CId) []CId {
	if slice == nil {
		return append(make([]CId, 0), cid)
	}

	if _, found := sliceFind(slice, cid); found {
		return slice
	}

	// extending existing index record by id
	return append(slice, cid)
}

func (ego *fullmatchIndexer[T]) Add(v any, id CId) error {
	val, err := ego.Get(v)
	s := v.(T)

	if err != nil {
		return err
	}

	// extending existing index record by id
	ego.index[s] = sliceAddUnique(val, id)
	return nil
}

func remove(rows []CId, ididx uint64) []CId {
	return append(rows[:ididx], rows[ididx+1:]...)
}

func (ego *fullmatchIndexer[T]) Del(v any, id CId) error {
	s := v.(T)
	val, _ := ego.Get(v)

	// Can not happen within RamCollection
	// if err != nil {
	// 	return err
	// }

	if val == nil {
		return errors.NewNotFoundError(ego, errors.LevelWarning, "Index trouble - value not found")
	}

	idx, found := sliceFind(val, id)

	if !found {
		return errors.NewNotFoundError(ego, errors.LevelWarning, "Index trouble - row not found within index record")
	}

	reduced := remove(val, idx)

	if len(reduced) <= 0 {
		delete(ego.index, s)
		return nil
	}

	ego.index[s] = reduced
	return nil
}

// Prefix implementation

type trieNode[E comparable] struct {
	children map[E]trieNode[E]
	cids     []CId
}

type prefixIndexer[E comparable] struct {
	ramCollectionIndexer
	index *trieNode[E] // entry
}

func prefixIndexerNew[E comparable](c PrefixIndexConf[[]E]) *prefixIndexer[E] {
	ego := new(prefixIndexer[E])

	ego.index = new(trieNode[E])
	ego.index.children = make(map[E]trieNode[E])

	return ego
}

func (ego *prefixIndexer[E]) getImpl(n trieNode[E], accumpath []E) ([]CId, error) {
	first := accumpath[0]

	if ch, found := n.children[first]; found {
		return ego.getImpl(ch, accumpath[1:])
	}

	return nil, nil
}

func (ego *prefixIndexer[E]) Get(v any) ([]CId, error) {
	val := v.([]E)

	return ego.getImpl(*ego.index, val)
}

func (ego *prefixIndexer[E]) addImpl(n trieNode[E], accumpath []E, cid CId) error {
	var first E

	if l := len(accumpath); l >= 1 {
		first = accumpath[0]
		if l > 1 {
			accumpath = accumpath[1:]
		} else { // really?
			accumpath = make([]E, 0)
		}
	} else {
		return errors.NewValueError(ego, errors.LevelError, "Cannot use zero path.")
	}

	if len(accumpath) == 0 {
		// add id here
		n.cids = sliceAddUnique(n.cids, cid)
		return nil
	} else {
		if ch, found := n.children[first]; found {
			return ego.addImpl(ch, accumpath, cid)
		} else {
			nnode := *new(trieNode[E])
			nnode.children = make(map[E]trieNode[E])

			n.children[first] = nnode
		}
	}

	return nil

}

func (ego *prefixIndexer[E]) Add(v any, id CId) error {
	val := v.([]E)
	return ego.addImpl(*ego.index, val, id)
}

func (ego *prefixIndexer[E]) delImpl(n trieNode[E], accumpath []E, id CId) (deleteP bool, err error) {
	first := accumpath[0]

	if len(accumpath) == 1 {
		// we are in the leaf
		// remove id here
		idx, found := sliceFind(n.cids, id)

		if !found {
			return false, errors.NewNotFoundError(ego, errors.LevelWarning, "Index trouble - row not found within index record")
		}

		reduced := remove(n.cids, idx)
		n.cids = reduced

		if len(reduced) <= 0 && len(n.children) <= 0 {
			n.cids = nil
			return true, nil
		}

		return false, nil
	}

	if ch, found := n.children[first]; found {
		toRemove, err := ego.delImpl(ch, accumpath[1:], id)
		if err != nil {
			return false, err
		}

		if toRemove {
			delete(n.children, first)
		}

		if len(n.cids) <= 0 && len(n.children) <= 0 {
			return true, nil
		}
	}

	return false, nil
}

func (ego *prefixIndexer[E]) Del(v any, id CId) error {
	// find from top to bottom, cleanup on going back
	delP, err := ego.delImpl(*ego.index, v.([]E), id)
	if err != nil {
		return err
	}
	if delP {
		// index completely removed so reset index for sure?
		ego.index = new(trieNode[E])
		ego.index.children = make(map[E]trieNode[E])
	}

	return nil
}

// RAM COLLECTION IMPL

type RamCollectionConf struct {
	SchemaConf
	MaxMemory uint64
}

type RamCollection struct {
	gonatus.Gobject
	Collection
	param         RamCollectionConf
	autoincrement CId
	rows          map[CId][]any
	indexes       map[string][]ramCollectionIndexer // FIXME: make array of indexes for fields not one index as max
	primaryIndex  *primaryIndexer
}

func NewRamCollection(rc RamCollectionConf) *RamCollection {
	ego := new(RamCollection)

	// TODO: check if implementing given fields

	ego.param = rc
	ego.rows = make(map[CId][]any, 0)
	ego.indexes = make(map[string][]ramCollectionIndexer, 0)
	// TODO: implement id index as default one ego.indexes["id"] = idIndexerNew() // must be present in every collection

	if err := ego.RegisterIndexes(); err != nil {
		panic(err)
	}
	return ego
}

func (ego *RamCollection) InterpretField(fc FielderConf) (any, error) {
	switch v := fc.(type) {
	case FieldConf[string]:
		return v.Value, nil
	default:
		return nil, errors.NewNotImplError(ego)
	}
}

func (ego *RamCollection) InterpretRecord(rc RecordConf) ([]any, error) {
	ret := make([]any, 0)

	for _, c := range rc.Cols {
		rf, err := ego.InterpretField(c)
		if err != nil {
			return nil, err
		}

		ret = append(ret, rf)
	}

	return ret, nil
}

func (ego *RamCollection) DeinterpretField(val any, nth int) (FielderConf, error) {
	fc := ego.param.SchemaConf.Fields[nth]

	switch fc.(type) {
	case FieldConf[string]:
		return FieldConf[string]{Value: val.(string)}, nil
	default:
		return nil, errors.NewNotImplError(ego)
	}
}

func (ego *RamCollection) DeinterpretRecord(r []any) (RecordConf, error) {
	ret := RecordConf{
		Cols: make([]FielderConf, len(ego.param.SchemaConf.Fields)),
	}

	for i := range ego.param.SchemaConf.Fields {

		field, err := ego.DeinterpretField(r[i], i)
		if err != nil {
			return RecordConf{}, err
		}

		ret.Cols[i] = field
	}

	return ret, nil
}

func (ego *RamCollection) AddRecord(rc RecordConf) (CId, error) {
	cid := rc.Id
	if !cid.ValidP() {
		// need to generate a new one
		ego.autoincrement++
		cid = ego.autoincrement
	} else {
		// have from the user
		if cid >= ego.autoincrement {
			// move id generator behind user defined cid
			ego.autoincrement = cid + 1
		} else {
			//possibly reusing existing id
			if _, found := ego.rows[cid]; found {
				return 0, errors.NewValueError(ego, errors.LevelFatal, "Can not reuse id!")
			}
		}
	}

	if cid == CId(MaxUint) {
		return 0, errors.NewValueError(ego, errors.LevelFatal, "Id pool depleted!")
	}

	// TODO: Check mandatory fields

	// TODO: Update default fields

	// Add to main index
	record, err := ego.InterpretRecord(rc)

	if err != nil {
		return 0, err
	}

	ego.rows[cid] = record

	// Add to lookup indexes
	for i, name := range ego.param.FieldsNaming {
		if colidx, found := ego.indexes[name]; found {
			for _, idx := range colidx {
				idx.Add(record[i], cid)
			}
		}
	}

	return cid, nil
}

func (c CId) ValidP() bool {
	return c > 0
}

func (ego *RamCollection) DeleteRecord(rc RecordConf) error {
	cid := rc.Id

	if !cid.ValidP() {
		return errors.NewMisappError(ego, "Invalid Id field in record.")
	}

	record, found := ego.rows[cid]

	if !found {
		return errors.NewNotFoundError(ego, errors.LevelWarning, fmt.Sprintf("Record with id %d not found.", cid))
	}

	// Add to lookup indexes
	for i, name := range ego.param.FieldsNaming {
		if colidx, found := ego.indexes[name]; found {
			for _, idx := range colidx {
				idx.Del(record[i], cid)
			}
		}
	}

	delete(ego.rows, cid)

	return nil
}

type CIdSet map[CId]bool

func CIdSetFromSlice(s []CId) CIdSet {
	ret := make(CIdSet, 0)

	for _, v := range s {
		ret[v] = true
	}

	return ret
}

// func CIdSetToSlice(u CIdSet) []CId {
// 	keys := make([]CId, len(u))
// 	i := 0

// 	for k := range u {
// 		keys[i] = k
// 		i++
// 	}

// 	return keys
// }

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

	for i := range lesser {

		if greater[i] {
			out[i] = true
		}
	}

	return out
}

func cmpIndexKind(qIdx IndexerConf, iidx ramCollectionIndexer) bool {
	switch qIdx.(type) {
	case PrefixIndexConf[fs.Path]:
		_, ok := iidx.(*prefixIndexer[string])
		if ok {
			return true
		}
	case PrefixIndexConf[string]:
		_, ok := iidx.(*prefixIndexer[rune])
		if ok {
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
	case FullmatchIndexConf[float64]:
		_, ok := iidx.(*fullmatchIndexer[float64])
		if ok {
			return true
		}
	case FullmatchIndexConf[int64]:
		_, ok := iidx.(*fullmatchIndexer[int64])
		if ok {
			return true
		}
	case FullmatchIndexConf[uint64]:
		_, ok := iidx.(*fullmatchIndexer[uint64])
		if ok {
			return true
		}
	case FullmatchIndexConf[time.Time]:
		_, ok := iidx.(*fullmatchIndexer[time.Time])
		if ok {
			return true
		}
	}
	return false
}

func (ego *RamCollection) getIndex(q QueryAtomConf) ramCollectionIndexer {
	if idxcol, found := ego.indexes[q.Name]; found {
		// index for that name found
		// try cast to the required index
		for _, idx := range idxcol {
			if cmpIndexKind(q.MatchType, idx) {
				return idx
			}
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

	if indexer == nil {
		pi := rc.primaryIndex
		rows, err := pi.Get(rc.primaryValue(*ego))

		if err != nil {
			return nil, err
		}

		return CIdSetFromSlice(rows), nil
	} else {
		rows, err := indexer.Get(ego.Value)
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
		return rc.allRowsSet(), nil // Returns whole space
	}

	for i := 0; i < ctxlen; i++ {
		acc, err := rc.filterQueryEval(QueryConf(ego.QueryContextConf.Context[0]))
		if i > 0 {
			accum = accum.Intersect(acc)
		} else {
			accum = acc
		}

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
		return rc.noRowsSet(), nil // // Returns empty set
	}

	for i := 0; i < ctxlen; i++ {
		acc, err := rc.filterQueryEval(QueryConf(ego.QueryContextConf.Context[i]))
		if err != nil {
			return nil, err
		}

		accum.Merge(acc)

		if len(accum) == len(rc.rows) {
			break
		}
	}

	return accum, nil
}

func (ego *RamCollection) every() CIdSet {
	result := make(CIdSet, len(ego.rows))

	for k := range ego.rows {
		result[k] = true
	}
	return result
}

func (ego *QueryImplicationConf) eval(rc *RamCollection) (CIdSet, error) {
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

	rws := rc.every()

	for i := range re {
		if _, found := le[i]; !found {
			delete(rws, i)
		}
	}

	return rws, nil
}

func (ego *RamCollection) allRowsSet() CIdSet {
	ids := make(CIdSet, len(ego.rows))

	for key := range ego.rows {
		ids[key] = true
	}

	return ids
}

func (ego *RamCollection) noRowsSet() CIdSet {
	return make(CIdSet, len(ego.rows))
}

func (ego *RamCollection) filterQueryEval(q QueryConf) (CIdSet, error) {
	switch v := q.(type) {
	case QueryAndConf:
		return v.eval(ego)
	case QueryOrConf:
		return v.eval(ego)
	case QueryImplicationConf:
		return v.eval(ego)
	case QueryAtomConf:
		return v.eval(ego)
	case QueryConf:
		return ego.allRowsSet(), nil
	default:
		return nil, errors.NewMisappError(ego, "Unknown collection filter query.")
	}
}

func (ego *RamCollection) Rows() map[CId][]any {
	return ego.rows
}

func (ego *RamCollection) Inspect() {
	print("ID, ")
	for _, r := range ego.param.SchemaConf.FieldsNaming {
		print(r, ", ")
	}

	print("\n")

	for i, r := range ego.rows {
		print(i)
		for _, c := range r {
			fmt.Printf(", %+v", c)
		}
		print("\n")
	}
}

func (ego *RamCollection) Filter(q QueryConf) (streams.ReadableOutputStreamer[RecordConf], error) {
	ret, err := ego.filterQueryEval(q)

	if err != nil {
		return nil, err
	}

	sbuf := streams.NewBufferInputStream[RecordConf](100)

	fetchRows := func() {
		for i := range ret {
			rec, err := ego.DeinterpretRecord(ego.rows[i])
			rec.Id = i

			if err != nil {
				// FIXME: sbuf.SetError() pass error! return nil, err
				panic(err)
			}

			sbuf.Write(rec)
		}
		sbuf.Close()
	}

	outs := streams.NewReadableOutputStream[RecordConf]()
	sbuf.Pipe(outs)

	go fetchRows()

	return outs, nil
}

func (ego *RamCollection) RegisterIndexes() error {
	ego.primaryIndex = primaryIndexerCreate(ego.rows)

	for _, idxcol := range ego.param.Indexes {
		for _, idx := range idxcol {
			switch v := idx.(type) {
			case PrefixIndexConf[[]string]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[string](v))
			case PrefixIndexConf[[]rune]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[rune](v))
			case FullmatchIndexConf[string]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[string](v))
			case FullmatchIndexConf[int]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[int](v))
			case FullmatchIndexConf[float64]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[float64](v))
			case FullmatchIndexConf[int64]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[int64](v))
			case FullmatchIndexConf[uint64]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[uint64](v))
			case FullmatchIndexConf[time.Time]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[time.Time](v))
			default:
				return errors.NewNotImplError(ego)
			}
		}
	}

	return nil
}

func (ego *RamCollection) Serialize() gonatus.Conf {
	return ego.param
}

func (ego *RamCollection) Commit() error {
	// Doing nothing - in future possibly commit content/oplog to a ndjson file?
	return nil
}
