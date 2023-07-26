package collection

import (
	"fmt"

	"github.com/SpongeData-cz/gonatus"
	"github.com/SpongeData-cz/gonatus/errors"
)

// PREFIX INDEX
type trieNode[E comparable] struct {
	children map[E]*trieNode[E]
	cids     []CId
}

type prefixIndexer[T comparable] struct {
	ramCollectionIndexer
	index          *trieNode[T] // entry
	ignoreChildren bool
}

func prefixIndexerNew[T comparable](c PrefixIndexConf[[]T]) *prefixIndexer[T] {
	ego := new(prefixIndexer[T])

	ego.index = new(trieNode[T])
	ego.index.children = make(map[T]*trieNode[T])

	return ego
}

func prefixIndexerNewIgnore[T comparable](c FullmatchIndexConf[[]T]) *prefixIndexer[T] {
	ego := new(prefixIndexer[T])

	ego.index = new(trieNode[T])
	ego.index.children = make(map[T]*trieNode[T])
	ego.ignoreChildren = true

	return ego
}

func (ego *prefixIndexer[T]) accumulateSubtree(n *trieNode[T]) CIdSet {
	out := make(CIdSet, 0)

	for _, c := range n.cids {
		out[c] = true
	}

	if !ego.ignoreChildren {
		for _, c := range n.children {
			out.Merge(ego.accumulateSubtree(c))
		}
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

func (ego *prefixIndexer[T]) addImpl(n *trieNode[T], accumpath []T, cid CId) {

	if len(accumpath) == 0 {
		// add id here
		n.cids = sliceAddUnique(n.cids, cid)
		return
	}

	first := accumpath[0]

	if ch, found := n.children[first]; found {
		ego.addImpl(ch, accumpath[1:], cid)
		return
	}

	nnode := new(trieNode[T])
	nnode.children = make(map[T]*trieNode[T])

	n.children[first] = nnode
	ego.addImpl(nnode, accumpath[1:], cid)
}

func (ego *prefixIndexer[T]) Add(v any, id CId) error {
	val := v.([]T)
	ego.addImpl(ego.index, val, id)
	return nil
}

func (ego *prefixIndexer[T]) delImpl(n *trieNode[T], accumpath []T, id CId) (deleteP bool, err error) {

	if len(accumpath) == 0 {
		// we are in the leaf
		// remove id here
		idx, found := sliceFind(n.cids, id)

		if !found {
			msg := fmt.Sprintf("Index trouble - row %d not found within index record", idx)
			return false, errors.NewNotFoundError(ego, errors.LevelWarning, msg)
		}

		reduced := remove(n.cids, idx)
		n.cids = reduced

		if len(reduced) <= 0 && len(n.children) <= 0 {
			n.cids = nil
			return true, nil
		}

		return false, nil
	}

	first := accumpath[0]

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

func (ego *prefixIndexer[T]) Serialize() gonatus.Conf {
	return nil
}

// STRING PREFIX INDEXER
type stringPrefixIndexer struct {
	*prefixIndexer[rune]
}

func stringPrefixIndexerNew(c PrefixIndexConf[string]) *stringPrefixIndexer {
	ego := new(stringPrefixIndexer)
	runePrefixConf := PrefixIndexConf[[]rune]{Name: c.Name}
	ego.prefixIndexer = prefixIndexerNew[rune](runePrefixConf)
	return ego
}

func (ego *stringPrefixIndexer) Get(v any) ([]CId, error) {
	val := v.(string)
	return ego.prefixIndexer.Get([]rune(val))
}

func (ego *stringPrefixIndexer) Add(v any, id CId) error {
	val := (v.(string))
	return ego.prefixIndexer.Add([]rune(val), id)
}

func (ego *stringPrefixIndexer) Del(v any, id CId) error {
	val := v.(string)
	return ego.prefixIndexer.Del([]rune(val), id)
}
