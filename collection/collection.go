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
	QueryAtomConf
}

type QueryImplicationConf struct {
	QueryConf
	Left  QueryAtomConf
	Right QueryAtomConf
}

type QuerySpatialConf interface {
	QueryConf
}

type QueryRange[T any] struct {
	QuerySpatialConf
	Lower  T
	Higher T
}

type Collection interface {
	gonatus.Gobjecter
	Filter(QueryConf) (stream.Producer[RecordConf], error)
	// Group(QueryConf, GroupQueryConf) (streams.ReadableOutputStreamer[GroupRecordConf], error) // TODO: define grouping
	AddRecord(RecordConf) (CId, error)
	DeleteRecord(RecordConf) error
	DeleteByFilter(QueryConf) error
	EditRecord(RecordConf, int, any) error
	Commit() error
}
