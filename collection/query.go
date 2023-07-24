package collection

import (
	"errors"
)

func (ego *QueryAtomConf) eval(rc *RamCollection) (CIdSet, error) {

	i := rc.getFieldIndex(*ego)
	if i == -1 {
		return nil, errors.New("column not found")
	}

	indexer := rc.getIndex(*ego)

	if indexer == nil {
		pi := rc.primaryIndex
		rows, err := pi.Get(rc.primaryValue(*ego, i))
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
