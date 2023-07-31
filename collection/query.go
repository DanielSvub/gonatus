package collection

import (
	"errors"
)

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
		} else {
			rows, err = pi.Get(rc.primaryValue(*ego, idx))
		}
	} else {
		rows, err = indexer.Get(ego.Value)
	}

	if err != nil {
		return nil, err
	}

	return CIdSetFromSlice(rows), nil
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

func (ego *QueryAtomConf) isPrefix(rc *RamCollection, idx int) bool {

	if ego.MatchType == nil {
		return false
	}

	switch ego.MatchType.(type) {
	case PrefixIndexConf[string]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[string]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]string]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]string]); !isMatch {
			println(30)
			return false
		}
		return true
	case PrefixIndexConf[[]int]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]int]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]int8]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]int8]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]int16]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]int16]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]int32]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]int32]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]int64]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]int64]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]uint]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]uint]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]uint8]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]uint8]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]uint16]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]uint16]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]uint32]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]uint32]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]uint64]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]uint64]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]float32]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]float32]); !isMatch {
			return false
		}
		return true
	case PrefixIndexConf[[]float64]:
		if _, isMatch := rc.param.Fields[idx].(FieldConf[[]float64]); !isMatch {
			return false
		}
		return true
	default:
		return false
	}
}
