package collection

import (
	"fmt"
	"reflect"
	"sync"

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

/*
Creates new RamCollection.

Parameters:
  - rc - RamCollection Conf.

Returns:
  - pointer to a new instance of RamCollection.
*/
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
		// TODO: Fatal log || panic?
		panic(err)
	}
	return ego
}

/*
Interprets the Record passed in the parameter.

Parameters:
  - rc - Configuration of Record.

Returns:
  - Slice of values from a row,
  - error, if any.
*/
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

/*
Deinterprets the values of the row passed in the parameter.

Parameters:
  - r - Values of row.

Returns:
  - Configuration of Record,
  - error, if any.
*/
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

/*
Adds to the RamCollection the record whose configuration is passed by the parameter,
and adds it to the lookup indexes.

Parameters:
  - rc - Configuration of the Record.

Returns:
  - CId of newly added record,
  - error, if any.
*/
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

/*
Checks, if CId is valid.

Returns:
  - True, if CId is valid, false otherwise.
*/
func (c CId) ValidP() bool {
	return c > 0
}

/*
Deletes from the RamCollection the record whose configuration is passed by the parameter,
and deletes it from lookup indexes.

Parameters:
  - rc - Configuration of the Record.

Returns:
  - Error, if any.
*/
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

/*
Filters the matching rows according to the given query.
It then deletes them from the RamCollection and deletes
them from the search indexers.

Parameters:
  - q - Configuration of the query.

Returns:
  - Error, if any.
*/
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

/*
Edits the RamCollection record whose configuration is passed by the parameter
and modifies it in the lookup indexes.

Parameters:
  - rc - Configuration of Record,
  - col - the index of the column in the row to be overwriten,
  - newValue - new value to overwrite the old one.

Returns:
  - Error, if any.
*/
func (ego *RamCollection) EditRecord(rc RecordConf, col int, newValue any) error {
	cid := rc.Id

	if !cid.ValidP() {
		return errors.NewMisappError(ego, "Invalid Id field in record.")
	}

	ego.mutex.Lock()
	defer ego.mutex.Unlock()

	record, found := ego.rows[cid]

	oldType := reflect.TypeOf(record[col])
	newType := reflect.TypeOf(newValue)
	if oldType != newType {
		return errors.NewMisappError(ego, fmt.Sprintf("Type mismatch (%s given, %s expected).", newType.String(), oldType.String()))
	}

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

/*
Creates a CId set from the CId slice.

Parameters:
  - s - CId slice.

Returns:
  - New CId set.
*/
func CIdSetFromSlice(s []CId) CIdSet {
	ret := make(CIdSet, 0)

	for _, v := range s {
		ret[v] = true
	}

	return ret
}

/*
Creates a CId slice from the CId set.

Returns:
  - New CId slice.
*/
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

/*
Merges two CId sets.

Parameters:
  - s - Set to be merged.
*/
func (ego CIdSet) Merge(s CIdSet) {
	for k, v := range s {
		ego[k] = v
	}
}

/*
Makes the intersection of two CId sets.

Parameters:
  - s - One of the CId sets.

Returns:
  - Newly created CId set by intersection.
*/
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

/*
Returns the indexer that is bound to the given column in the query, if any.

Parameters:
  - q - Query.

Returns:
  - Indexer, if any, nil otherwise.
*/
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

/*
Returns the index of the column with the name specified in the query.
If such a column does not exist, returns -1.

Parameters:
  - q - Query.

Returns:
  - Column index, if it doesn't exist, returns -1.
*/
func (ego *RamCollection) getFieldIndex(q QueryAtomConf) int {

	for i, n := range ego.param.SchemaConf.FieldsNaming {
		if n == q.Name {
			return i
		}
	}

	return -1
}

/*
Creates an array as long as the number of columns.
The array is filled with nil. The value being searched
for is at the index of the column being searched.

Parameters:
  - q - Query,
  - index - where the value will be stored.

Returns:
  - array filled with nil, except on index of the searched column.
*/
func (ego *RamCollection) primaryValue(q QueryAtomConf, index int) []any {
	anys := make([]any, len(ego.param.SchemaConf.FieldsNaming))
	anys[index] = q.Value // FIXME: Design hack
	return anys
}

/*
Creates a CId set and sets all existing row indexes to true.

Returns:
  - New CId set.
*/
func (ego *RamCollection) setAllRows() CIdSet {
	ids := make(CIdSet, len(ego.rows))

	for key := range ego.rows {
		ids[key] = true
	}

	return ids
}

/*
Returns:
  - Empty set of CIds
*/
func (ego *RamCollection) noRowsSet() CIdSet {
	return make(CIdSet, len(ego.rows))
}

/*
Depending on the query type, it sends the query
and RamCollection to the corresponding evaluation function.

Parameters:
  - q - Query.

Returns:
  - The set of CIds that match the given query,
  - error, if any.
*/
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
		return ego.setAllRows(), nil
	default:
		return nil, errors.NewMisappError(ego, "Unknown collection filter query.")
	}
}

/*
Returns:
  - RamCollection's rows
*/
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

/*
Filters rows based on the type and content of the query
and writes them to the stream.

Parameters:
  - q - Query.

Returns:
  - Readable Output Streamer,
  - error, if any.
*/
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
	indexers uint16 // The individual bits represent individual indexers.
}

const prefixIndexBit = 0    // 0th bit
const fullmatchIndexBit = 1 // 1st bit

/*
Checks if a column with this name exists.
If it does, it checks to see if we are about to
bind a duplicate indexer to FielderConf.
If not, it sets the nth (by indexer type) bit of Fielder.

Parameters:
  - name - name of the column,
  - nthBit - represent individual indexers.

Returns:
  - True, if the indexer is correctly bound, false otherwise.
*/
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

/*
Checks if a column named <name> exists in the RamCollection.

Parameters:
  - name - name to be chacked in RamCollection.

Returns:
  - True, if exists, false otherwise.
*/
func (ego *RamCollection) checkName(name string) bool {
	return slices.Contains(ego.param.FieldsNaming, name)
}

/*
Serializes RamCollection.

Returns:
  - configuration of the Gobject.
*/
func (ego *RamCollection) Serialize() gonatus.Conf {
	return ego.param
}

/*
Returns:
  - error, if any.
*/
func (ego *RamCollection) Commit() error {
	// Doing nothing - in future possibly commit content/oplog to a ndjson file?
	return nil
}
