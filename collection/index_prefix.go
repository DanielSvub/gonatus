package collection

import (
	"fmt"

	"github.com/DanielSvub/gonatus"
	"github.com/DanielSvub/gonatus/errors"
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

/*
Creates new prefixIndexer.

Parameters:
  - c - Configuration of PrefixIndexer.

Returns:
  - pointer to a new instance of prefixIndexer.
*/
func prefixIndexerNew[T comparable](c PrefixIndexConf[[]T]) *prefixIndexer[T] {
	ego := new(prefixIndexer[T])

	ego.index = new(trieNode[T])
	ego.index.children = make(map[T]*trieNode[T])

	return ego
}

/*
Creates new prefixIndexer that allows you to ignore the children..

Parameters:
  - c - Configuration of FullmatchIndex.

Returns:
  - pointer to a new instance of prefixIndexer.
*/
func prefixIndexerNewIgnore[T comparable](c FullmatchIndexConf[[]T]) *prefixIndexer[T] {
	ego := new(prefixIndexer[T])

	ego.index = new(trieNode[T])
	ego.index.children = make(map[T]*trieNode[T])
	ego.ignoreChildren = true

	return ego
}

/*
Merge the CIds of the children of a given node.
If ignoreChildren is set, this is done only up to the first level.

Parameters:
  - n - current node.

Returns:
  - CId set.
*/
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

/*
Searches the tree <<n>> to see if it matches accumpath.

Parameters:
  - n - Tree,
  - accumpath - path to be searched.

Returns:
  - A node that fully matches the accumpath,
  - error, if any.
*/
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

/*
Searches for rows that has prefix matching the <v> parameter.

Parameters:
  - v - Searched prefix.

Returns:
  - CId of rows that match,
  - error, if any.
*/
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

/*
Adds the <<accumpath>> to the tree <<n>>.

Parameters:
  - n - Tree,
  - accumpath - path to add,
  - cid - CId of accumpath.
*/
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

/*
Extends or adds an existing index record by id.

Parameters:
  - v - Value from specific row and column,
  - id - CId of record.

Returns:
  - Error, if any.
*/
func (ego *prefixIndexer[T]) Add(v any, id CId) error {
	val := v.([]T)
	ego.addImpl(ego.index, val, id)
	return nil
}

/*
Removes the <<accumpath>> from the tree <<n>>.

Parameters:
  - n - Tree,
  - accumpath - path to remove,
  - cid - CId of accumpath.

Returns:
  - True, if <<accumpath>> has been successfully removed,
  - error, if any.
*/
func (ego *prefixIndexer[T]) delImpl(n *trieNode[T], accumpath []T, cid CId) (deleteP bool, err error) {

	if len(accumpath) == 0 {
		// we are in the leaf
		// remove id here
		idx, found := sliceFind(n.cids, cid)

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
		toRemove, err := ego.delImpl(ch, accumpath[1:], cid)
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

/*
Removes an existing index record by id.

Parameters:
  - v - Value from specific row and column,
  - id - CId of record.

Returns:
  - Error, if any.
*/
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

/*
Serializes prefixIndexer.

Returns:
  - configuration of the Gobject.
*/
func (ego *prefixIndexer[T]) Serialize() gonatus.Conf {
	return nil
}

// STRING PREFIX INDEXER
type stringPrefixIndexer struct {
	*prefixIndexer[rune]
}

/*
Creates new stringPrefixIndexer.

Parameters:
  - c - PrefixIndex Conf of type string.

Returns:
  - pointer to a new instance of stringPrefixIndexer.
*/
func stringPrefixIndexerNew(c PrefixIndexConf[string]) *stringPrefixIndexer {
	ego := new(stringPrefixIndexer)
	runePrefixConf := PrefixIndexConf[[]rune]{Name: c.Name}
	ego.prefixIndexer = prefixIndexerNew[rune](runePrefixConf)
	return ego
}

/*
Searches for rows that has prefix matching the <v> parameter.

Parameters:
  - v - Searched prefix.

Returns:
  - CId of rows that match,
  - error, if any.
*/
func (ego *stringPrefixIndexer) Get(v any) ([]CId, error) {
	val := v.(string)
	return ego.prefixIndexer.Get([]rune(val))
}

/*
Extends or adds an existing index record by id.

Parameters:
  - v - Value from specific row and column,
  - id - CId of record.

Returns:
  - Error, if any.
*/
func (ego *stringPrefixIndexer) Add(v any, id CId) error {
	val := (v.(string))
	return ego.prefixIndexer.Add([]rune(val), id)
}

/*
Removes an existing index record by id.

Parameters:
  - v - Value from specific row and column,
  - id - CId of record.

Returns:
  - Error, if any.
*/
func (ego *stringPrefixIndexer) Del(v any, id CId) error {
	val := v.(string)
	return ego.prefixIndexer.Del([]rune(val), id)
}
