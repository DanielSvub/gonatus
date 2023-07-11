package collection_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	. "github.com/SpongeData-cz/gonatus/collection"
)

func TestCollection(t *testing.T) {
	t.Run("serialization", func(t *testing.T) {
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

	})
	t.Run("notValidFields", func(t *testing.T) {
		// TODO: Solved? - should panic in future

		type FieldNotExistingConf struct {
			FielderConf
			Value string
		}

		rmCE := RamCollectionConf{
			SchemaConf: SchemaConf{
				Name:         "FooBarTable2",
				FieldsNaming: []string{"err"},
				Fields: []FielderConf{
					FieldNotExistingConf{},
				},
				Indexes: [][]IndexerConf{},
			},
			MaxMemory: 1024 * 1024 * 1024,
		}

		assertPanic := func() {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("The code did not panic")
				}
			}()
			NewRamCollection(rmCE)
		}

		assertPanic()
	})
	t.Run("error", func(t *testing.T) {
		// Test limit id
		rmc := prepareTable(true, false)

		err := testFilling(rmc)

		if err != nil {
			t.Error(err.Error())
		}

		rcrds := fillRecords([][]string{
			{"n@a.n", "n@n.y"},
		})

		rcrds[0].Id = CId(MaxUint)
		id, err := rmc.AddRecord(rcrds[0])
		if err == nil {
			t.Error("Shoud throw an error.")
		}

		if id.ValidP() {
			t.Errorf("Should return invalid CId but %d given", id)
		}

	})

	t.Run("removal", func(t *testing.T) {
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
		if err != nil {
			t.Error(err.Error())
		}
		if id != 1337 {
			t.Errorf("Expected Id 1337 but %d given", id)
		}

		// // TODO: Check mandatory fields
		// for i, l := range rmc.param.FieldsNaming {
		// 	println("Name: ", l, " fields: ", rmc.param.SchemaConf.Fields[i])
		// }

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
	})
	t.Run("nilQuery", func(t *testing.T) {
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
		if err != nil {
			t.Error(err.Error())
		}

		if len(output) != 2 {
			t.Errorf("Expected 2 got %d\n", len(output))
		}

		if err := testNthLine(output, 0); err != nil {
			t.Error(err)
		}
		if err := testNthLine(output, 1); err != nil {
			t.Error(err)
		}

	})

	t.Run("atom", func(t *testing.T) {
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
		if err != nil {
			t.Error(err.Error())
		}

		if len(output) != 1 {
			t.Errorf("Expected 1 got %d\n", len(output))
		}

		if err := testNthLine(output, 0); err != nil {
			t.Error(err)
		}
	})
	t.Run("and", func(t *testing.T) {
		output := testLogical(t, "and")
		if err := testNthLine(output, 0); err != nil {
			t.Error(err)
		}
	})
	t.Run("or", func(t *testing.T) {
		output := testLogical(t, "or")

		if len(output) != 2 {
			t.Errorf("Expected 2 lines but %d given", len(output))
			return
		}

		if err := testNthLine(output, 0); err != nil {
			t.Error(err)
		}

		if err := testNthLine(output, 1); err != nil {

			t.Error(err)
		}
	})
	t.Run("implication", func(t *testing.T) {
		output := testLogical(t, "implies")

		if len(output) != 2 {
			t.Errorf("Expected 2 lines but %d given", len(output))
		}

		if err := testNthLine(output, 0); err != nil {
			t.Error(err)
		}

		if err := testNthLine(output, 1); err != nil {
			t.Error(err)
		}
	})

	t.Run("index", func(t *testing.T) {
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
		if err != nil {
			t.Error(err)
		}

		if len(output) != 1 {
			t.Errorf("Expected 1 got %d\n", len(output))
		}

		if err := testNthLine(output, 0); err != nil {
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
	})

	t.Run("generalPrefix", func(t *testing.T) {
		// TODO
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

		// output, err := smc.Collect()
		_, err = smc.Collect()
		if err != nil {
			t.Error(err)
		}
		// t.Errorf("Returned %+v", output)
	})
	t.Run("stringPrefix", func(t *testing.T) {
		// TODO: ...
		// rmc := prepareTable(true, true)

		// err := testFilling(rmc)
		// if err != nil {
		// 	t.Error(err)
		// }

		// rmc.Inspect()
	})
}

func testNthLine(rc []RecordConf, n int) error {
	rclen := len(rc[n].Cols)

	if rclen != 2 {
		return fmt.Errorf("Wrong number of result columns %d", rclen)
	}

	// for i, col := range rc {
	// 	println("Cols: ", i)
	// 	for _, c := range col.Cols {
	// 		ss, _ := c.(FieldConf[string])
	// 		println("Value: ", ss.Value, ", fconf: ", ss.FielderConf)
	// 	}
	// }

	col1, ok1 := rc[n].Cols[0].(FieldConf[string])

	if !ok1 {
		return fmt.Errorf("Cannot cast to the original FieldStringConf.")
	}

	if col1.Value != "a@b.cz" && col1.Value != "x@y.tv" {
		return fmt.Errorf("Wrong order of values, got: %s", col1.Value)
	}

	col1_2, ok1_2 := rc[n].Cols[1].(FieldConf[string])

	if col1.Value == "a@b.cz" {
		if !ok1_2 {
			return fmt.Errorf("Cannot cast to the original FieldStringConf.")
		}
		if col1_2.Value != "c@d.com" {
			return fmt.Errorf("Wrong second value of column 1.")
		}
	} else {
		if !ok1_2 {
			return fmt.Errorf("Cannot cast to the original FieldStringConf.")
		}
		if col1_2.Value != "b@a.co.uk" {
			return fmt.Errorf("Wrong second value of column 2.")
		}
	}

	return nil
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
	if err != nil {
		t.Error(err)
	}
	rmc.Inspect()

	return output
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
