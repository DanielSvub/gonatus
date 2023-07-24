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

func (ego *fullmatchIndexer[T]) Serialize() gonatus.Conf {
	return nil
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
