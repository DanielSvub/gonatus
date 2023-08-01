package collection

import (
	"fmt"
	"sync"
	"time"

	"github.com/SpongeData-cz/gonatus"
	"github.com/SpongeData-cz/gonatus/errors"
	"github.com/SpongeData-cz/gonatus/streams"
	"golang.org/x/exp/slices"
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

// RAM COLLECTION IMPL
type RamCollectionConf struct {
	SchemaConf
	MaxMemory uint64
}

type RamCollection struct {
	gonatus.Gobject
	param         RamCollectionConf
	autoincrement CId
	rows          map[CId][]any
	indexes       map[string][]ramCollectionIndexer // FIXME: make array of indexes for fields not one index as max
	primaryIndex  *primaryIndexer
	mutex         *sync.RWMutex
}

func NewRamCollection(rc RamCollectionConf) *RamCollection {
	if len(rc.SchemaConf.FieldsNaming) != len(rc.SchemaConf.Fields) {
		// TODO: Fatal log || panic?
		return nil
	}

	ego := new(RamCollection)

	for _, field := range rc.Fields {
		if _, err := ego.InterpretField(field); err != nil {
			panic(errors.NewNotImplError(ego))
		}
	}

	ego.param = rc
	ego.mutex = new(sync.RWMutex)
	ego.rows = make(map[CId][]any, 0)
	ego.indexes = make(map[string][]ramCollectionIndexer, 0)
	// TODO: implement id index as default one ego.indexes["id"] = idIndexerNew() // must be present in every collection

	if err := ego.registerIndexes(); err != nil {
		panic(err)
	}
	return ego
}

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
	ego.mutex.Lock()
	defer ego.mutex.Unlock()

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
				if err := idx.Add(record[i], cid); err != nil {
					return 0, err //FIXME: inconsitent state if any call of Add fails
				}
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

	ego.mutex.Lock()
	defer ego.mutex.Unlock()

	record, found := ego.rows[cid]

	if !found {
		return errors.NewNotFoundError(ego, errors.LevelWarning, fmt.Sprintf("Record with id %d not found.", cid))
	}

	// Delete from lookup indices
	for i, name := range ego.param.FieldsNaming {
		if colidx, found := ego.indexes[name]; found {
			for _, idx := range colidx {
				if err := idx.Del(record[i], cid); err != nil {
					return err //FIXME: inconsitent state if any call of Del fails
				}
			}
		}
	}

	delete(ego.rows, cid)

	return nil
}

func (ego *RamCollection) DeleteByFilter(q QueryConf) error {
	if qq, ok := q.(QueryAndConf); ok && len(qq.Context) == 0 {
		ego.rows = make(map[CId][]any)
		ego.indexes = make(map[string][]ramCollectionIndexer)
		ego.registerIndexes()
		ego.autoincrement = 1
		return nil
	}

	if stream, err := ego.Filter(q); err != nil {
		return err
	} else {
		for !stream.Closed() {
			s := make([]RecordConf, 1)
			if _, err := stream.Read(s); err != nil {
				return err
			}
			rec := s[0]
			ego.DeleteRecord(RecordConf{Id: rec.Id})
		}
	}
	return nil
}

func (ego *RamCollection) EditRecord(rc RecordConf, col int, newValue any) error {
	cid := rc.Id

	if !cid.ValidP() {
		return errors.NewMisappError(ego, "Invalid Id field in record.")
	}

	ego.mutex.Lock()
	defer ego.mutex.Unlock()

	record, found := ego.rows[cid]

	if !found {
		return errors.NewNotFoundError(ego, errors.LevelWarning, fmt.Sprintf("Record with id %d not found.", cid))
	}

	name := ego.param.FieldsNaming[col]

	// Modify lookup indexes
	if colidx, found := ego.indexes[name]; found {
		for _, idx := range colidx {
			if err := idx.Del(record[col], cid); err != nil {
				return err //FIXME: inconsitent state if any call of Del fails
			}
			if err := idx.Add(record[col], cid); err != nil {
				return err //FIXME: inconsitent state if any call of Del fails
			}
		}
	}

	ego.rows[cid][col] = newValue

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

func (ego *RamCollection) getFieldIndex(q QueryAtomConf) int {

	for i, n := range ego.param.SchemaConf.FieldsNaming {
		if n == q.Name {
			return i
		}
	}

	return -1
}

func (ego *RamCollection) primaryValue(q QueryAtomConf, index int) []any {
	anys := make([]any, len(ego.param.SchemaConf.FieldsNaming))
	anys[index] = q.Value // FIXME: Design hack
	return anys
}

func (ego *RamCollection) every() CIdSet {
	result := make(CIdSet, len(ego.rows))

	for k := range ego.rows {
		result[k] = true
	}
	return result
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
	fmt.Printf("\nTable Name: %s\n", ego.param.Name)

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
	print("\n")
}

func (ego *RamCollection) Filter(q QueryConf) (streams.ReadableOutputStreamer[RecordConf], error) {
	ego.mutex.RLock()

	ret, err := ego.filterQueryEval(q)

	if err != nil {
		return nil, err
	}

	sbuf := streams.NewBufferInputStream[RecordConf](100)

	fetchRows := func() {
		defer ego.mutex.RUnlock()
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

// Mapping columns names to a structure containing fielders and indexers.
type cols map[string]colTuple

type colTuple struct {
	fc       FielderConf
	indexers uint16 // The individual nth bits represent individual indexers.
}

const prefixIndexBit = 0    // 0th bit
const fullmatchIndexBit = 1 // 1st bit

func (ego *cols) checkNum(name string, nthBit int) bool {
	if tuple, found := (*ego)[name]; found {
		if (tuple.indexers & (1 << nthBit)) != 0 {
			return false
		}
		tuple.indexers = (1 << nthBit) | tuple.indexers
		(*ego)[name] = tuple
		return true
	}
	return false
}

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
				if _, found := columns[v.Name].fc.(FieldConf[string]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], stringPrefixIndexerNew(v))
				name = v.Name
			case PrefixIndexConf[[]string]:
				if _, found := columns[v.Name].fc.(FieldConf[[]string]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[string](v))
				name = v.Name
			case PrefixIndexConf[[]int]:
				if _, found := columns[v.Name].fc.(FieldConf[[]int]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[int](v))
				name = v.Name
			case PrefixIndexConf[[]int8]:
				if _, found := columns[v.Name].fc.(FieldConf[[]int8]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[int8](v))
				name = v.Name
			case PrefixIndexConf[[]int16]:
				if _, found := columns[v.Name].fc.(FieldConf[[]int16]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[int16](v))
				name = v.Name
			case PrefixIndexConf[[]int32]:
				if _, found := columns[v.Name].fc.(FieldConf[[]int32]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[int32](v))
				name = v.Name
			case PrefixIndexConf[[]int64]:
				if _, found := columns[v.Name].fc.(FieldConf[[]int64]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[int64](v))
				name = v.Name
			case PrefixIndexConf[[]uint]:
				if _, found := columns[v.Name].fc.(FieldConf[[]uint]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[uint](v))
				name = v.Name
			case PrefixIndexConf[[]uint8]:
				if _, found := columns[v.Name].fc.(FieldConf[[]uint8]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[uint8](v))
				name = v.Name
			case PrefixIndexConf[[]uint16]:
				if _, found := columns[v.Name].fc.(FieldConf[[]uint16]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[uint16](v))
				name = v.Name
			case PrefixIndexConf[[]uint32]:
				if _, found := columns[v.Name].fc.(FieldConf[[]uint32]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[uint32](v))
				name = v.Name
			case PrefixIndexConf[[]uint64]:
				if _, found := columns[v.Name].fc.(FieldConf[[]uint64]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[uint64](v))
				name = v.Name
			case PrefixIndexConf[[]float32]:
				if _, found := columns[v.Name].fc.(FieldConf[[]float32]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[float32](v))
				name = v.Name
			case PrefixIndexConf[[]float64]:
				if _, found := columns[v.Name].fc.(FieldConf[[]float64]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, prefixIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNew[float64](v))
				name = v.Name
			case FullmatchIndexConf[string]:
				if _, found := columns[v.Name].fc.(FieldConf[string]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[string](v))
				name = v.Name
			case FullmatchIndexConf[int]:
				if _, found := columns[v.Name].fc.(FieldConf[int]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[int](v))
				name = v.Name
			case FullmatchIndexConf[int8]:
				if _, found := columns[v.Name].fc.(FieldConf[int8]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[int8](v))
				name = v.Name
			case FullmatchIndexConf[int16]:
				if _, found := columns[v.Name].fc.(FieldConf[int16]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[int16](v))
				name = v.Name
			case FullmatchIndexConf[int32]:
				if _, found := columns[v.Name].fc.(FieldConf[int32]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[int32](v))
				name = v.Name
			case FullmatchIndexConf[int64]:
				if _, found := columns[v.Name].fc.(FieldConf[int64]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[int64](v))
				name = v.Name
			case FullmatchIndexConf[uint]:
				if _, found := columns[v.Name].fc.(FieldConf[uint]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[uint](v))
				name = v.Name
			case FullmatchIndexConf[uint8]:
				if _, found := columns[v.Name].fc.(FieldConf[uint8]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[uint8](v))
				name = v.Name
			case FullmatchIndexConf[uint16]:
				if _, found := columns[v.Name].fc.(FieldConf[uint16]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[uint16](v))
				name = v.Name
			case FullmatchIndexConf[uint32]:
				if _, found := columns[v.Name].fc.(FieldConf[uint32]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[uint32](v))
				name = v.Name
			case FullmatchIndexConf[uint64]:
				if _, found := columns[v.Name].fc.(FieldConf[uint64]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[uint64](v))
				name = v.Name
			case FullmatchIndexConf[float32]:
				if _, found := columns[v.Name].fc.(FieldConf[float32]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[float32](v))
				name = v.Name
			case FullmatchIndexConf[float64]:
				if _, found := columns[v.Name].fc.(FieldConf[float64]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[float64](v))
				name = v.Name
			case FullmatchIndexConf[time.Time]:
				if _, found := columns[v.Name].fc.(FieldConf[time.Time]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], fullmatchIndexerNew[time.Time](v))
				name = v.Name
			case FullmatchIndexConf[[]string]:
				if _, found := columns[v.Name].fc.(FieldConf[[]string]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[string](v))
				name = v.Name
			case FullmatchIndexConf[[]int]:
				if _, found := columns[v.Name].fc.(FieldConf[[]int]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[int](v))
				name = v.Name
			case FullmatchIndexConf[[]int8]:
				if _, found := columns[v.Name].fc.(FieldConf[[]int8]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[int8](v))
				name = v.Name
			case FullmatchIndexConf[[]int16]:
				if _, found := columns[v.Name].fc.(FieldConf[[]int16]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[int16](v))
				name = v.Name
			case FullmatchIndexConf[[]int32]:
				if _, found := columns[v.Name].fc.(FieldConf[[]int32]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[int32](v))
				name = v.Name
			case FullmatchIndexConf[[]int64]:
				if _, found := columns[v.Name].fc.(FieldConf[[]int64]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[int64](v))
				name = v.Name
			case FullmatchIndexConf[[]uint]:
				if _, found := columns[v.Name].fc.(FieldConf[[]uint]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[uint](v))
				name = v.Name
			case FullmatchIndexConf[[]uint8]:
				if _, found := columns[v.Name].fc.(FieldConf[[]uint8]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[uint8](v))
				name = v.Name
			case FullmatchIndexConf[[]uint16]:
				if _, found := columns[v.Name].fc.(FieldConf[[]uint16]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[uint16](v))
				name = v.Name
			case FullmatchIndexConf[[]uint32]:
				if _, found := columns[v.Name].fc.(FieldConf[[]uint32]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[uint32](v))
				name = v.Name
			case FullmatchIndexConf[[]uint64]:
				if _, found := columns[v.Name].fc.(FieldConf[[]uint64]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[uint64](v))
				name = v.Name
			case FullmatchIndexConf[[]float32]:
				if _, found := columns[v.Name].fc.(FieldConf[[]float32]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
					return errors.NewNotImplError(ego)
				}
				ego.indexes[v.Name] = append(ego.indexes[v.Name], prefixIndexerNewIgnore[float32](v))
				name = v.Name
			case FullmatchIndexConf[[]float64]:
				if _, found := columns[v.Name].fc.(FieldConf[[]float64]); !found {
					return errors.NewNotImplError(ego)
				} else if !columns.checkNum(v.Name, fullmatchIndexBit) {
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

func (ego *RamCollection) checkName(name string) bool {
	return slices.Contains(ego.param.FieldsNaming, name)
}

func (ego *RamCollection) Serialize() gonatus.Conf {
	return ego.param
}

func (ego *RamCollection) Commit() error {
	// Doing nothing - in future possibly commit content/oplog to a ndjson file?
	return nil
}
