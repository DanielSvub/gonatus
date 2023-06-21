package collection_test

import (
	"testing"

	. "github.com/SpongeData-cz/gonatus/collection"
)

func TestRam(t *testing.T) {
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

	rec := RecordConf{
		Cols: []FielderConf{
			FieldStringConf{
				Value: "a@b.cz",
			},
			FieldStringConf{
				Value: "c@d.com",
			},
		},
	}

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

	_, err := rmc.Filter(query)
	if err != nil {
		panic("Filter failed")
	}

	print(rmc)
}
