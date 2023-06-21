package collection

import "github.com/SpongeData-cz/gonatus"

/* Collection ID */
type CId uint64
type FieldType uint64

const (
	UNKNOWN    = 0
	STRING     = 1 << iota
	INT        = 1 << iota
	UINT       = 1 << iota
	FLOAT      = 1 << iota
	DATE       = 1 << iota
	LABEL      = 1 << iota // This is used for limited value space fields like image tagging and so.
	MULTIVALUE = 1 << iota // This is used for multivalued fields.
	SET        = 1 << iota // This implies multivalue and excludes VECTOR usage. Used for multivalue fields which do not need to keep order.
	VECTOR     = 1 << iota // This implies multivalue and excludes SET usage. Used for multivalue fields which need keep order.
	GEO        = 1 << iota // For geospatial usage.
	BINARY     = 1 << iota // For binary data aka []byte.
)

type IndexType uint64

const (
	PREFIX  = 1 << 32
	POSTFIX = 1 << 33
	INFIX   = 1 << 34
	RANGE   = 1 << 35
	SPATIAL = 1 << 36
)

/*
Verifies if input data are compatible with the stream
*/

type Verifier interface {
	Verify([]byte) bool
}

/*
Normalizes input data to internal representation
*/
type Normalizer interface {
	Normalize([]byte) []byte
}

/*
Denormalizes input data to internal representation
*/
type Denormalizer interface {
	Denormalize([]byte) []byte
}

type Getter interface {
	Get() any
}

type Setter interface {
	Set(any) error
}

type Fielder struct {
	Getter
	Setter
}

type Record struct {
	Cols []Fielder
}

type RecordSet struct {
	Naming  []string
	Records []Record
}

type FielderConf interface {
}

type FieldStringConf struct {
	FielderConf
	Value string
}

type FieldString struct {
	gonatus.Gobject
	Getter
	Setter
	value string
	param FieldStringConf
}

func NewFieldString(c FieldStringConf) *FieldString {
	ego := new(FieldString)
	ego.param = c

	if ego.Set(c.Value) != nil {
		return nil
	}

	return ego
}

func (ego *FieldString) Get() string {
	return ego.value
}

func (ego *FieldString) Set(s string) error {
	ego.value = s
	return nil
}

type Commiter interface {
	Commit() error
}

type Schemer interface {
	Commiter
}

type Schema struct {
	Name string
	RecordSet
}

// func NewSchema(sc SchemaConf) *Schema {

// }

// type Collectioner interface {
// 	Schema
// }

// type RamCollection struct {
// 	Collectioner
// }

// Just registers Indexer
type IndexerConf interface {
}

type IdIndexConf struct {
	IndexerConf
	Name string
}

type PrefixStringIndexConf struct {
	IndexerConf
	Name      string
	MinPrefix uint
}

type FullmatchStringIndexConf struct {
	IndexerConf
	Name string
}

type SchemaConf struct {
	Name         string
	FieldsNaming []string
	Fields       []FielderConf
	Indexes      []IndexerConf // map[string][]IndexerConf
}

type RecordConf struct {
	Row []FielderConf
}

func NewRecord(RecordConf) *Record {
	return nil
}

type QueryConf interface {
}

type QueryAtomConf struct {
	QueryConf
	Field     FielderConf
	MatchType IndexerConf
	Name      string
	Value     any
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

type QueryImplicatonConf struct {
	QueryConf
	Left  QueryAtomConf
	Right QueryAtomConf
}

func main() {
	rmC := RamCollectionConf{
		SchemaConf: SchemaConf{
			Name:         "FooBarTable",
			FieldsNaming: []string{"who", "whom"},
			Fields: []FielderConf{
				FieldStringConf{},
				FieldStringConf{},
			},
			Indexes: []IndexerConf{
				PrefixStringIndexConf{Name: "who", MinPrefix: 3},
			},
		},
		MaxMemory: 1024 * 1024 * 1024,
	}

	rmc := NewRamCollection(rmC)

	rec := NewRecord(RecordConf{
		Row: []FielderConf{
			FieldStringConf{
				Value: "a@b.cz",
			},
			FieldStringConf{
				Value: "c@d.com",
			},
		},
	})

	rmc.AddRecord(rec)

	query := QueryAndConf{
		QueryContextConf{
			Context: []QueryConf{
				QueryAtomConf{
					Field:     "who",
					Value:     "a@b.cz",
					MatchType: FullmatchStringIndexConf{},
				},
				QueryAtomConf{
					Field:     "whom",
					Value:     "c@d.com",
					MatchType: FullmatchStringIndexConf{},
				},
			},
		},
	}

	rmc.filterQueryEval(query)

	print(rmc)
}
