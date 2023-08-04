package collection

import (
	"errors"
)

/*
Filters rows based on the atom-query.

Parameters:
  - rc - Ram Collection.

Returns:
  - CId set of rows satisfying the atom-query condition,
  - error, if any.
*/
func (ego *QueryAtomConf) eval(rc *RamCollection) (CIdSet, error) {

	idx := rc.getFieldIndex(*ego)
	if idx == -1 {
		return nil, errors.New("column not found")
	}

	indexer := rc.getIndex(*ego)

	var rows []CId
	var err error

	if indexer == nil {
		pi := rc.primaryIndex
		if ego.isPrefix(rc, idx) {
			rows, err = pi.getPrefix(rc.primaryValue(*ego, idx))
		} else if ego.isFullmatch(rc, idx) {
			rows, err = pi.Get(rc.primaryValue(*ego, idx))
		} else {
			err = errors.New("not valid prefix in query")
		}
	} else {
		rows, err = indexer.Get(ego.Value)
	}

	if err != nil {
		return nil, err
	}

	return CIdSetFromSlice(rows), nil
}

/*
Filters rows based on the and-query.

Parameters:
  - rc - Ram Collection.

Returns:
  - CId set of rows satisfying the and-query condition,
  - error, if any.
*/
func (ego *QueryAndConf) eval(rc *RamCollection) (CIdSet, error) {
	accum := make(CIdSet, 0)
	ctxlen := len(ego.QueryContextConf.Context)

	if ctxlen == 0 {
		return rc.setAllRows(), nil // Returns whole space
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

/*
Filters rows based on the or-query.

Parameters:
  - rc - Ram Collection.

Returns:
  - CId set of rows satisfying the or-query condition,
  - error, if any.
*/
func (ego *QueryOrConf) eval(rc *RamCollection) (CIdSet, error) {
	accum := make(CIdSet, 0)
	ctxlen := len(ego.QueryContextConf.Context)

	if ctxlen == 0 {
		return rc.noRowsSet(), nil // Returns empty set
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

/*
Filters rows based on the implication-query.

Parameters:
  - rc - Ram Collection.

Returns:
  - CId set of rows satisfying the implication-query condition,
  - error, if any.
*/
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

	rws := rc.setAllRows()

	for i := range re {
		if _, found := le[i]; !found {
			delete(rws, i)
		}
	}

	return rws, nil
}
