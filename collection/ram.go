package collection

import (
	"fmt"
	"time"

	"github.com/SpongeData-cz/gonatus"
	"github.com/SpongeData-cz/gonatus/errors"
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
	children map[E]*trieNode[E]
	cids     []CId
}

type prefixIndexer[T comparable] struct {
	ramCollectionIndexer
	index *trieNode[T] // entry
}

func prefixIndexerNew[T comparable](c PrefixIndexConf[[]T]) *prefixIndexer[T] {
	ego := new(prefixIndexer[T])

	ego.index = new(trieNode[T])
	ego.index.children = make(map[T]*trieNode[T])

	return ego
}

func (ego *prefixIndexer[T]) accumulateSubtree(n *trieNode[T]) CIdSet {
	out := make(CIdSet, 0)

	for _, c := range n.cids {
		out[c] = true
	}

	for _, c := range n.children {
		out.Merge(ego.accumulateSubtree(c))
	}

	return out
}

func (ego *prefixIndexer[T]) getImpl(n *trieNode[T], accumpath []T) (*trieNode[T], error) {
	if len(accumpath) <= 0 {
		return n, nil
	}

	first := accumpath[0]

	if ch, found := n.children[first]; found {
		return ego.getImpl(ch, accumpath[1:])
	}

	return nil, nil
}

func (ego *prefixIndexer[T]) Get(v any) ([]CId, error) {
	val := v.([]T)

	node, err := ego.getImpl(ego.index, val)
	if err != nil {
		return nil, err
	}

	if node == nil {
		// not found
		return []CId{}, nil
	}

	idset := ego.accumulateSubtree(node)

	return idset.ToSlice(), nil
}

func (ego *prefixIndexer[T]) addImpl(n *trieNode[T], accumpath []T, cid CId) error {
	var first T

	if l := len(accumpath); l >= 1 {
		first = accumpath[0]
		if l > 1 {
			accumpath = accumpath[1:]
		} else { // really?
			accumpath = make([]T, 0)
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
			nnode := new(trieNode[T])
			nnode.children = make(map[T]*trieNode[T])

			n.children[first] = nnode
			return ego.addImpl(nnode, accumpath[:1], cid)
		}
	}
}

func (ego *prefixIndexer[T]) checkForString(v any) []rune {
	vs, ok := v.(string)

	if ok {
		return []rune(vs)
	}

	return nil
}

func (ego *prefixIndexer[T]) Add(v any, id CId) error {
	val := v.([]T)
	return ego.addImpl(ego.index, val, id)
}

func (ego *prefixIndexer[T]) delImpl(n *trieNode[T], accumpath []T, id CId) (deleteP bool, err error) {
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

func (ego *prefixIndexer[T]) Del(v any, id CId) error {
	// find from top to bottom, cleanup on going back
	delP, err := ego.delImpl(ego.index, v.([]T), id)
	if err != nil {
		return err
	}
	if delP {
		// index completely removed so reset index for sure?
		ego.index = new(trieNode[T])
		ego.index.children = make(map[T]*trieNode[T])
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
	if len(rc.SchemaConf.FieldsNaming) != len(rc.SchemaConf.Fields) {
		// TODO: Fatal log || panic?
		return nil
	}

	ego := new(RamCollection)

	// TODO: check if implementing given fields'
	for _, field := range rc.Fields {
		if _, err := ego.InterpretField(field); err != nil {
			panic(errors.NewNotImplError(ego))
		}
	}

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

func (ego CIdSet) ToSlice() []CId {
	out := make([]CId, len(ego))
	i := 0
	for k := range ego {
		out[i] = k
		i++
	}

	return out
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
	case PrefixIndexConf[[]string]:
		_, ok := iidx.(*prefixIndexer[string])
		if ok {
			return true
		}
	case PrefixIndexConf[[]int]:
		_, ok := iidx.(*prefixIndexer[int])
		if ok {
			return true
		}
	case PrefixIndexConf[[]int8]:
		_, ok := iidx.(*prefixIndexer[int8])
		if ok {
			return true
		}
	case PrefixIndexConf[[]int16]:
		_, ok := iidx.(*prefixIndexer[int16])
		if ok {
			return true
		}
	case PrefixIndexConf[[]int32]:
		_, ok := iidx.(*prefixIndexer[int32])
		if ok {
			return true
		}
	case PrefixIndexConf[[]int64]:
		_, ok := iidx.(*prefixIndexer[int64])
		if ok {
			return true
		}
	case PrefixIndexConf[[]uint]:
		_, ok := iidx.(*prefixIndexer[uint])
		if ok {
			return true
		}
	case PrefixIndexConf[[]uint8]:
		_, ok := iidx.(*prefixIndexer[uint8])
		if ok {
			return true
		}
	case PrefixIndexConf[[]uint16]:
		_, ok := iidx.(*prefixIndexer[uint16])
		if ok {
			return true
		}
	case PrefixIndexConf[[]uint32]:
		_, ok := iidx.(*prefixIndexer[uint32])
		if ok {
			return true
		}
	case PrefixIndexConf[[]uint64]:
		_, ok := iidx.(*prefixIndexer[uint64])
		if ok {
			return true
		}
	case PrefixIndexConf[[]float32]:
		_, ok := iidx.(*prefixIndexer[float32])
		if ok {
			return true
		}
	case PrefixIndexConf[[]float64]:
		_, ok := iidx.(*prefixIndexer[float64])
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
	fmt.Printf("Table Name: %s\n", ego.param.Name)

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
			case PrefixIndexConf[[]int]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[int](v))
			case PrefixIndexConf[[]int8]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[int8](v))
			case PrefixIndexConf[[]int16]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[int16](v))
			case PrefixIndexConf[[]int32]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[int32](v))
			case PrefixIndexConf[[]int64]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[int64](v))
			case PrefixIndexConf[[]uint]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[uint](v))
			case PrefixIndexConf[[]uint8]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[uint8](v))
			case PrefixIndexConf[[]uint16]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[uint16](v))
			case PrefixIndexConf[[]uint32]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[uint32](v))
			case PrefixIndexConf[[]uint64]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[uint64](v))
			case PrefixIndexConf[[]float32]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[float32](v))
			case PrefixIndexConf[[]float64]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[float64](v))
			case FullmatchIndexConf[string]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[string](v))
			case FullmatchIndexConf[int]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[int](v))
			case FullmatchIndexConf[int8]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[int8](v))
			case FullmatchIndexConf[int16]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[int16](v))
			case FullmatchIndexConf[int32]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[int32](v))
			case FullmatchIndexConf[int64]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[int64](v))
			case FullmatchIndexConf[uint]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[uint](v))
			case FullmatchIndexConf[uint8]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[uint8](v))
			case FullmatchIndexConf[uint16]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[uint16](v))
			case FullmatchIndexConf[uint32]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[uint32](v))
			case FullmatchIndexConf[uint64]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[uint64](v))
			case FullmatchIndexConf[float32]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[float32](v))
			case FullmatchIndexConf[float64]:
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[float64](v))
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
