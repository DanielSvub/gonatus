package collection_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	. "github.com/SpongeData-cz/gonatus/collection"
)

func TestSerialization(t *testing.T) {
	rmC := RamCollectionConf{
		SchemaConf: SchemaConf{
			Name:         "FooBarTable",
			FieldsNaming: []string{"who", "whom"},
			Fields: []FielderConf{
				FieldConf[string]{},
				FieldConf[string]{},
			},
			Indexes: [][]IndexerConf{
				{
					PrefixIndexConf[[]string]{Name: "who", MinPrefix: 3},
				},
			},
		},
		MaxMemory: 1024 * 1024 * 1024,
	}

	rmc := NewRamCollection(rmC)
	rmCT := rmc.Serialize().(RamCollectionConf)

	if !(len(rmCT.Fields) == len(rmC.Fields) && len(rmCT.FieldsNaming) == len(rmC.FieldsNaming)) {
		t.Error("Not equal headers.")
	}

	if !(rmCT.FieldsNaming[0] == "who" && rmCT.FieldsNaming[1] == "whom") {
		t.Error("Field not named correctly.")
	}

	// TODO: should panic in future

	// type FieldNotExistingConf struct {
	// 	FielderConf
	// 	Value string
	// }

	// rmCE := RamCollectionConf{
	// 	SchemaConf: SchemaConf{
	// 		Name:         "FooBarTable2",
	// 		FieldsNaming: []string{"err"},
	// 		Fields: []FielderConf{
	// 			FieldNotExistingConf{},
	// 		},
	// 		Indexes: []IndexerConf{},
	// 	},
	// 	MaxMemory: 1024 * 1024 * 1024,
	// }

	// assertPanic := func() {
	// 	defer func() {
	// 		if r := recover(); r == nil {
	// 			t.Errorf("The code did not panic")
	// 		}
	// 	}()
	// 	NewRamCollection(rmCE)
	// }

	// assertPanic()

}

func prepareTable(indexP bool, prefixIndexP bool) *RamCollection {
	rmC := RamCollectionConf{
		SchemaConf: SchemaConf{
			Name:         "FooBarTable",
			FieldsNaming: []string{"who", "whom"},
			Fields: []FielderConf{
				FieldConf[string]{},
				FieldConf[string]{},
			},
			Indexes: [][]IndexerConf{},
		},
		MaxMemory: 1024 * 1024 * 1024,
	}

	if indexP {
		rmC.Indexes = [][]IndexerConf{
			{
				FullmatchIndexConf[string]{Name: "who"},
				FullmatchIndexConf[string]{Name: "whom"},
			},
		}
	}

	if prefixIndexP {
		rmC.Indexes = append(rmC.Indexes, []IndexerConf{
			PrefixIndexConf[[]rune]{Name: "who"},
			PrefixIndexConf[[]rune]{Name: "whom"},
		})
	}

	return NewRamCollection(rmC)
}

func fillRecords[T any](rows [][]T) []RecordConf {
	out := make([]RecordConf, len(rows))

	for i, r := range rows {
		rec := RecordConf{
			Cols: make([]FielderConf, len(r)),
		}

		for j, c := range r {
			rec.Cols[j] = FieldConf[T]{
				Value: c,
			}
		}

		out[i] = rec
	}

	return out
}

func TestErrors(t *testing.T) {
	rmc := prepareTable(true, false)

	err := testFilling(rmc)

	if err != nil {
		t.Error(err)
	}

	rcrds := fillRecords([][]string{
		{"n@a.n", "n@n.y"},
	})

	rcrds[0].Id = CId(MaxUint)
	id, err := rmc.AddRecord(rcrds[0])

	if id.ValidP() {
		t.Errorf("Should return invalid CId but %d given", id)
	}

	// Test limit id

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
	rmc := prepareTable(true, false)

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

	id2, err2 := rmc.AddRecord(record)
	if err2 == nil {
		t.Errorf("Repeating Id should lead to error but got nil.")
	}

	if id2.ValidP() {
		t.Errorf("Repeating Id should lead to invalid Id got %d.", id2)
	}

	errd := rmc.DeleteRecord(RecordConf{Id: 1})
	if errd != nil {
		t.Errorf(errd.Error())
	}

	if len(rmc.Rows()) != 2 {
		t.Errorf("Exptecting 2 rows after remove third.")
	}

	rcr := RecordConf{Id: 2222}
	errd = rmc.DeleteRecord(rcr)

	if errd == nil {
		t.Errorf("Removal of not existing record should lead to error.")
	}
}

func testFirstLine(rc []RecordConf) error {
	rclen := len(rc[0].Cols)

	if rclen != 2 {
		return errors.New(fmt.Sprintf("Wrong number of result columns %d", rclen))
	}

	col1, ok1 := rc[0].Cols[0].(FieldConf[string])

	if !ok1 {
		return errors.New(fmt.Sprintf("Cannot cast to the original FieldStringConf."))
	}

	if col1.Value != "a@b.cz" {
		return errors.New(fmt.Sprintf("Wrong number of result columns %d", len(rc[0].Cols)))
	}

	col2, ok2 := rc[0].Cols[1].(FieldConf[string])

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

	col1, ok1 := rc[1].Cols[0].(FieldConf[string])

	if !ok1 {
		return errors.New(fmt.Sprintf("Cannot cast to the original FieldStringConf."))
	}

	if col1.Value != "x@y.tv" {
		return errors.New(fmt.Sprintf("Wrong number of result columns %d", len(rc[1].Cols)))
	}

	col2, ok2 := rc[1].Cols[1].(FieldConf[string])

	if !ok2 {
		return errors.New(fmt.Sprintf("Cannot cast to the original FieldStringConf."))
	}

	if col2.Value != "b@a.co.uk" {
		return errors.New(fmt.Sprintf("Wrong number of result columns %d", len(rc[1].Cols)))
	}

	return nil
}

func TestNilQuery(t *testing.T) {
	rmc := prepareTable(false, false)

	err := testFilling(rmc)
	if err != nil {
		t.Error(err)
	}

	queryAtom := new(QueryConf)

	smc, err := rmc.Filter(queryAtom)
	if err != nil {
		t.Error(err)
	}

	output, err := smc.Collect()

	if len(output) != 2 {
		t.Errorf("Expected 2 got %d\n", len(output))
	}

	if err := testFirstLine(output); err != nil {

		t.Error(err)

	}

	if err := testSecondLine(output); err != nil {

		t.Error(err)
	}
}

func TestAtom(t *testing.T) {
	rmc := prepareTable(false, false)

	err := testFilling(rmc)
	if err != nil {
		t.Error(err)
	}

	queryAtom := QueryAtomConf{
		Name:      "who",
		Value:     "a@b.cz",
		MatchType: FullmatchIndexConf[string]{},
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
	rmc := prepareTable(false, false)

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
						MatchType: FullmatchIndexConf[string]{},
					},
					QueryAtomConf{
						Name:      "whom",
						Value:     "b@a.co.uk",
						MatchType: FullmatchIndexConf[string]{},
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
						MatchType: FullmatchIndexConf[string]{},
					},
					QueryAtomConf{
						Name:      "whom",
						Value:     "c@d.com",
						MatchType: FullmatchIndexConf[string]{},
					},
				},
			},
		}
	} else if op == "implies" {
		query = QueryImplicationConf{
			Left: QueryAtomConf{
				Name:      "who",
				Value:     "a@b.cz",
				MatchType: FullmatchIndexConf[string]{},
			},
			Right: QueryAtomConf{
				Name:      "whom",
				Value:     "c@d.com",
				MatchType: FullmatchIndexConf[string]{},
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

func TestImplication(t *testing.T) {
	output := testLogical(t, "implies")

	if len(output) != 2 {
		t.Errorf("Expected 2 lines but %d given", len(output))
		//return
	}

	if err := testFirstLine(output); err != nil {
		t.Error(err)
	}

	if err := testSecondLine(output); err != nil {
		t.Error(err)
	}
}

func TestIndex(t *testing.T) {
	println(reflect.TypeOf(&FieldConf[string]{}).Kind())
	println(reflect.TypeOf(reflect.TypeOf(FieldConf[string]{})))
	println("TESTED")
	rmc := prepareTable(true, false)

	err := testFilling(rmc)
	if err != nil {
		t.Error(err)
	}

	rmc.Inspect()

	queryAtom := QueryAtomConf{
		Name:      "who",
		Value:     "a@b.cz",
		MatchType: FullmatchIndexConf[string]{},
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

	queryAtom = QueryAtomConf{
		Name:      "who",
		Value:     "notexisting",
		MatchType: FullmatchIndexConf[string]{},
	}

	smc, err = rmc.Filter(queryAtom)
	if err != nil {
		t.Error(err)
	}

	output, err = smc.Collect()
	if err != nil {
		t.Error(err)
	}

	if len(output) != 0 {
		t.Errorf("Expected [] output but got %+v instead.", output)
	}
}

func TestStringPrefix(t *testing.T) {
	// TODO: ...
	// rmc := prepareTable(true, true)

	// err := testFilling(rmc)
	// if err != nil {
	// 	t.Error(err)
	// }

	// rmc.Inspect()
}

func consumeGPfxOutput(rcrds []RecordConf) error {

	return nil
}

func TestGeneralPrefix(t *testing.T) {
	rmC := RamCollectionConf{
		SchemaConf: SchemaConf{
			Name:         "PathTable",
			FieldsNaming: []string{"path"},
			Fields: []FielderConf{
				FieldConf[[]string]{},
			},
			Indexes: [][]IndexerConf{},
		},
	}

	rmC.Indexes = append(rmC.Indexes, []IndexerConf{
		PrefixIndexConf[[]string]{Name: "path"},
	})

	rmc := NewRamCollection(rmC)

	if rmc == nil {
		t.Errorf("Should return valid instance of RamCollection.")
	}

	rs := make([][][]string, 5)
	rs[0] = [][]string{{"ab", "cdef", "ghi"}}
	rs[1] = [][]string{{"ab", "jkl"}}
	rs[2] = [][]string{{"mno", "pane", "pr"}}
	rs[3] = [][]string{{"ab", "pane", "pr"}}
	rs[4] = [][]string{{"root", "var", "storage"}}

	rcrds := fillRecords(rs)

	for _, r := range rcrds {
		_, err := rmc.AddRecord(r)
		if err != nil {
			t.Errorf("Addition cause error %s", err)
			return
		}
	}

	rmc.Inspect()

	queryAtom := QueryAtomConf{
		Name:      "path",
		Value:     []string{"ab"},
		MatchType: PrefixIndexConf[[]string]{},
	}

	smc, err := rmc.Filter(queryAtom)
	if err != nil {
		t.Error(err)
	}

	output, err := smc.Collect()
	t.Errorf("Returned %+v", output)
}
