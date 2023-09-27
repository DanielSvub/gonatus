package collection

import (
	"github.com/SpongeData-cz/gonatus"
	"github.com/SpongeData-cz/stream"
)

/* Collection ID */
type CId uint64
type FieldType uint64

type FielderConf interface {
}

type FieldConf[T any] struct {
	FielderConf
	Value T
}

// Just registers Indexer
type IndexerConf interface {
}

type SpatialIndexerConf interface {
}

type PrefixIndexConf[T any] struct {
	IndexerConf
	Name      string
	MinPrefix uint
}

type FullmatchIndexConf[T any] struct {
	IndexerConf
	Name string
}

type SchemaConf struct {
	Name         string
	FieldsNaming []string
	Fields       []FielderConf
	Indexes      [][]IndexerConf
}

type RecordConf struct {
	Id   CId
	Cols []FielderConf
}

type QueryConf interface {
}

const (
	ASC = iota
	DESC
)

const NO_LIMIT = -1

type FilterArgument struct {
	QueryConf
	Sort      []string
	SortOrder int
	Skip      int
	Limit     int
}

type QueryAtomConf struct {
	QueryConf
	MatchType IndexerConf
	Name      string
	Value     any // TODO: Use generic here?
}

type QueryContextConf struct {
	QueryConf
	Context []QueryConf
}

type QueryAndConf struct {
	QueryContextConf
}

type QueryOrConf struct {
	QueryContextConf
}

type QueryNegConf struct {
	QueryConf
}

type QueryImplicationConf struct {
	QueryConf
	Left  QueryConf
	Right QueryConf
}

//type QuerySpatialConf interface {
//	QueryConf
//}

type QueryRange[T any] struct {
	QueryConf
	Name   string
	Lower  T
	Higher T
}

// Collection is the interface which specify the functionality common to all Collection:
// - Filter which returns records from the collection constrained by the FilterArguemnt in a stream and a count of records in the stream.
// - AddRecord adds specified record to collection and returns assigned id. Note that returned id may be different than the one specified in the record.
// - DeleteRecord deletes the given record from the Collection
// - DeleteByFilter deletes all the records which satisfy the FilterArgument and returns the number of deleted records.
// - EditRecord changes the record with id specified by RecordConf so it has specified values.
// - Commit commits all planned changes to the collection.
type Collection interface {
	gonatus.Gobjecter
	Filter(FilterArgument) (stream.Producer[RecordConf], uint64, error)
	// Group(QueryConf, GroupQueryConf) (streams.ReadableOutputStreamer[GroupRecordConf], error) // TODO: define grouping
	AddRecord(RecordConf) (CId, error)
	DeleteRecord(RecordConf) error
	DeleteByFilter(FilterArgument) (uint64, error)
	EditRecord(RecordConf) error
	Commit() error
}
