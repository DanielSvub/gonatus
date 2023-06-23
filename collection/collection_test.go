package collection_test

import (
	"fmt"
	"testing"

	. "github.com/SpongeData-cz/gonatus/collection"
)

func TestSerialization(t *testing.T) {
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
	rmCT := rmc.Serialize().(RamCollectionConf)

	fmt.Printf("%+v\n", rmCT)

	if !(len(rmCT.Fields) == len(rmC.Fields) && len(rmCT.FieldsNaming) == len(rmC.FieldsNaming)) {
		t.Error("Not equal headers.")
	}

	if !(rmCT.FieldsNaming[0] == "who" && rmCT.FieldsNaming[1] == "whom") {
		t.Error("Field not named correctly.")
	}
}

func fillRecords(rows [][]string) []RecordConf {
	out := make([]RecordConf, len(rows))

	for i, r := range rows {
		rec := RecordConf{
			Cols: make([]FielderConf, len(r)),
		}

		for j, c := range r {
			rec.Cols[j] = FieldStringConf{
				Value: c,
			}
		}

		out[i] = rec
	}

	return out

}

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

	// rec := RecordConf{
	// 	Cols: []FielderConf{
	// 		FieldStringConf{
	// 			Value: "a@b.cz",
	// 		},
	// 		FieldStringConf{
	// 			Value: "c@d.com",
	// 		},
	// 	},
	// }

	rcrds := fillRecords([][]string{
		{"a@b.cz", "c@d.com"},
		{"x@y.tv", "b@a.co.uk"},
	})

	id, err := rmc.AddRecord(rcrds[0])

	if err != nil {
		t.Error("Adding record failed.")
	}

	println("Assigned id: ", id)

	id, err = rmc.AddRecord(rcrds[1])

	if err != nil {
		t.Error("Adding record failed.")
	}

	println("Assigned id: ", id)

	println("ROWS:", len(rmc.Rows()))
	rmc.Inspect()

	query := QueryAndConf{
		QueryContextConf{
			Context: []QueryConf{
				QueryAtomConf{
					Field:     "who",
					Value:     "a@b.cz",
					MatchType: FullmatchStringIndexConf{},
				},
				// QueryAtomConf{
				// 	Field:     "whom",
				// 	Value:     "c@d.com",
				// 	MatchType: FullmatchStringIndexConf{},
				// },
			},
		},
	}

	smc, err := rmc.Filter(query)
	if err != nil {
		panic("Filter failed")
	}

	print(" RMC_ADDDR: ", rmc)

	output, err := smc.Collect()
	print(" LEN:", len(output), " ERR: ", err)

}
