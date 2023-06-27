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

func prepareTable(indexP bool) *RamCollection {
	rmC := RamCollectionConf{
		SchemaConf: SchemaConf{
			Name:         "FooBarTable",
			FieldsNaming: []string{"who", "whom"},
			Fields: []FielderConf{
				FieldStringConf{},
				FieldStringConf{},
			},
			Indexes: []IndexerConf{},
		},
		MaxMemory: 1024 * 1024 * 1024,
	}

	if indexP {
		rmC.Indexes = []IndexerConf{
			FullmatchStringIndexConf{Name: "who"},
			FullmatchStringIndexConf{Name: "whom"},
		}
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

	if id != 1 {
		return errors.New("Expecting id = 1.")
	}

	id, err = rmc.AddRecord(rcrds[1])

	if id != 2 {
		return errors.New("Expecting id = 2.")
	}

	if err != nil {
		return errors.New("Adding record failed.")
	}

	if len(rmc.Rows()) != 2 {
		return errors.New("Expecting 2 rows now.")
	}

	return nil
}

func TestRemoval(t *testing.T) {
	rmc := prepareTable(true)

	err := testFilling(rmc)
	if err != nil {
		t.Error(err)
	}

	rcrds := fillRecords([][]string{
		{"a@b.cz", "c@d.com"},
	})

	record := rcrds[0]
	record.Id = 1337

	id, err := rmc.AddRecord(record)
	if id != 1337 {
		t.Errorf("Expected Id 1337 but %d given", id)
	}

	errd := rmc.DeleteRecord(RecordConf{Id: 1})
	if errd != nil {
		t.Errorf(errd.Error())
	}

	if len(rmc.Rows()) != 2 {
		t.Errorf("Exptecting 2 rows after remove third.")
	}

	// TestOr(t)
}

func testFirstLine(rc []RecordConf) error {

	rclen := len(rc[0].Cols)

	if rclen != 2 {
		return errors.New(fmt.Sprintf("Wrong number of result columns %d", rclen))
	}

	col1, ok1 := rc[0].Cols[0].(FieldStringConf)

	if !ok1 {
		return errors.New(fmt.Sprintf("Cannot cast to the original FieldStringConf."))
	}

	if col1.Value != "a@b.cz" {
		return errors.New(fmt.Sprintf("Wrong number of result columns %d", len(rc[0].Cols)))
	}

	col2, ok2 := rc[0].Cols[1].(FieldStringConf)

	if !ok2 {
		return errors.New(fmt.Sprintf("Cannot cast to the original FieldStringConf."))
	}

	if col2.Value != "c@d.com" {
		return errors.New(fmt.Sprintf("Wrong number of result columns %d", len(rc[0].Cols)))
	}

	return nil
}

func testSecondLine(rc []RecordConf) error {
	rclen := len(rc[1].Cols)

	if rclen != 2 {
		return errors.New(fmt.Sprintf("Wrong number of result columns %d", rclen))
	}

	col1, ok1 := rc[1].Cols[0].(FieldStringConf)

	if !ok1 {
		return errors.New(fmt.Sprintf("Cannot cast to the original FieldStringConf."))
	}

	if col1.Value != "x@y.tv" {
		return errors.New(fmt.Sprintf("Wrong number of result columns %d", len(rc[1].Cols)))
	}

	col2, ok2 := rc[1].Cols[1].(FieldStringConf)

	if !ok2 {
		return errors.New(fmt.Sprintf("Cannot cast to the original FieldStringConf."))
	}

	if col2.Value != "b@a.co.uk" {
		return errors.New(fmt.Sprintf("Wrong number of result columns %d", len(rc[1].Cols)))
	}

	return nil
}

func TestNilQuery(t *testing.T) {

}

func TestAtom(t *testing.T) {
	rmc := prepareTable(false)

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

func testLogical(t *testing.T, op string) []RecordConf {
	rmc := prepareTable(false)

	err := testFilling(rmc)
	if err != nil {
		t.Error(err)
	}

	var query QueryConf

	if op == "or" {
		query = QueryOrConf{
			QueryContextConf{
				Context: []QueryConf{
					QueryAtomConf{
						Name:      "who",
						Value:     "a@b.cz",
						MatchType: FullmatchStringIndexConf{},
					},
					QueryAtomConf{
						Name:      "whom",
						Value:     "b@a.co.uk",
						MatchType: FullmatchStringIndexConf{},
					},
				},
			},
		}
	} else if op == "and" {
		query = QueryAndConf{
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
	} else {
		t.Errorf("Unknown op: %s", op)
	}

	smc, err := rmc.Filter(query)
	if err != nil {
		t.Error(err)
	}

	output, err := smc.Collect()
	rmc.Inspect()

	return output
}

func TestAnd(t *testing.T) {
	output := testLogical(t, "and")
	if err := testFirstLine(output); err != nil {
		t.Error(err)
	}
}

func TestOr(t *testing.T) {
	output := testLogical(t, "or")

	if len(output) != 2 {
		t.Errorf("Expected 2 lines but %d given", len(output))
		return
	}

	if err := testFirstLine(output); err != nil {
		t.Error(err)
	}

	if err := testSecondLine(output); err != nil {
		t.Error(err)
	}
}

func TestIndex(t *testing.T) {
	rmc := prepareTable(true)

	err := testFilling(rmc)
	if err != nil {
		t.Error(err)
	}

	rmc.Inspect()

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
