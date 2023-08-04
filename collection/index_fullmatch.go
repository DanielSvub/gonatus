package collection

import (
	"github.com/SpongeData-cz/gonatus"
	"github.com/SpongeData-cz/gonatus/errors"
)

// FULLMATCH INDEX
type fullmatchIndexer[T comparable] struct {
	ramCollectionIndexer
	index map[T][]CId
}

/*
Creates new fullmatchIndexer.

Parameters:
  - c - Configuration of FullmatchIndexer.

Returns:
  - pointer to a new instance of fullmatchIndexer.
*/
func fullmatchIndexerNew[T comparable](c FullmatchIndexConf[T]) *fullmatchIndexer[T] {
	ego := new(fullmatchIndexer[T])
	ego.index = make(map[T][]CId)

	return ego
}

/*
Searches for rows that full match the parameter <v>.

Parameters:
  - v - Searched value.

Returns:
  - CIds of rows that match,
  - error, if any.
*/
func (ego *fullmatchIndexer[T]) Get(v any) ([]CId, error) {
	s := v.(T)
	x, found := ego.index[s]
	if !found {
		return nil, nil
	}

	return x, nil
}

/*
Serializes fullmatchIndexer.

Returns:
  - Configuration of the Gobject.
*/
func (ego *fullmatchIndexer[T]) Serialize() gonatus.Conf {
	return nil
}

/*
Checks if the row with the given CId exists.

Parameters:
  - rows - CIds slice to be searched,
  - idx - CId of record.

Returns:
  - The index on which the row is located,
  - true, if row exists, false otherwise.
*/
func sliceFind(rows []CId, idx CId) (uint64, bool) {
	for i, v := range rows {
		if v == idx {
			return uint64(i), true
		}
	}

	return uint64(MaxUint), false
}

/*
Adds only unique values into slice of CIds.

Parameters:
  - slice - CIds slice to be modified,
  - cid - CId of record.

Returns:
  - slice of CIds.
*/
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

/*
Extends or adds an existing index record by id.

Parameters:
  - v - Value from specific row and column,
  - id - CId of record.

Returns:
  - Error, if any.
*/
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

/*
Removes the row passed by the <ididx> parameter from the row slice.

Parameters:
  - rows - Slice from which the row will be removed,
  - ididx - index of the row to be removed.

Returns:
  - CId slice after removal.
*/
func remove(rows []CId, ididx uint64) []CId {
	return append(rows[:ididx], rows[ididx+1:]...)
}

/*
Removes an existing index record by id.

Parameters:
  - v - Value from specific row and column,
  - id - CId of record.

Returns:
  - Error, if any.
*/
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
