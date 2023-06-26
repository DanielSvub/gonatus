package collection_test

import (
	"errors"
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

func prepareTable() *RamCollection {
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

	return NewRamCollection(rmC)
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

func testFilling(rmc *RamCollection) error {
	rcrds := fillRecords([][]string{
		{"a@b.cz", "c@d.com"},
		{"x@y.tv", "b@a.co.uk"},
	})

	id, err := rmc.AddRecord(rcrds[0])

	if err != nil {
		return errors.New("Adding record failed.")
	}

	println("Assigned id: ", id)

	id, err = rmc.AddRecord(rcrds[1])

	if err != nil {
		return errors.New("Adding record failed.")
	}

	return nil
}

func testFirstLine(rc []RecordConf) error {
	if len(rc[0].Cols) != 2 {
		errors.New(fmt.Sprintf("Wrong number of result columns %d", len(rc[0].Cols)))
	}

	col1, ok1 := rc[0].Cols[0].(FieldStringConf)

	if !ok1 {
		errors.New(fmt.Sprintf("Cannot cast to the original FieldStringConf."))
	}

	if col1.Value != "a@b.cz" {
		return errors.New(fmt.Sprintf("Wrong number of result columns %d", len(rc[0].Cols)))
	}

	col2, ok2 := rc[0].Cols[1].(FieldStringConf)

	if !ok2 {
		errors.New(fmt.Sprintf("Cannot cast to the original FieldStringConf."))
	}

	if col2.Value != "c@d.com" {
		errors.New(fmt.Sprintf("Wrong number of result columns %d", len(rc[0].Cols)))
	}

	return nil
}

func TestNilQuery(t *testing.T) {

}

func TestAtom(t *testing.T) {
	rmc := prepareTable()

	//rmc.Inspect()

	err := testFilling(rmc)
	if err != nil {
		t.Error(err)
	}

	queryAtom := QueryAtomConf{
		Name:      "who",
		Value:     "a@b.cz",
		MatchType: FullmatchStringIndexConf{},
	}

	smc, err := rmc.Filter(queryAtom)
	if err != nil {
		t.Error(err)
	}

	output, err := smc.Collect()

	if len(output) != 1 {
		t.Errorf("Expected 1 got %d\n", len(output))
	}

	if err := testFirstLine(output); err != nil {
		t.Error(err)
	}
}

func TestLogical(t *testing.T) {
	rmc := prepareTable()

	//rmc.Inspect()

	err := testFilling(rmc)
	if err != nil {
		t.Error(err)
	}

	query := QueryAndConf{
		QueryContextConf{
			Context: []QueryConf{
				QueryAtomConf{
					Name:      "who",
					Value:     "a@b.cz",
					MatchType: FullmatchStringIndexConf{},
				},
				QueryAtomConf{
					Name:      "whom",
					Value:     "c@d.com",
					MatchType: FullmatchStringIndexConf{},
				},
			},
		},
	}

	smc, err := rmc.Filter(query)
	if err != nil {
		t.Error(err)
	}

	output, err := smc.Collect()

	if len(output) != 1 {
		t.Errorf("Expected 1 got %d\n", len(output))
	}

	if err := testFirstLine(output); err != nil {
		t.Error(err)
	}
}
