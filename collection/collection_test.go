package collection_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	. "github.com/SpongeData-cz/gonatus/collection"
)

func TestCollection(t *testing.T) {

	t.Run("fillingTypes", func(t *testing.T) {

		rmCInt := RamCollectionConf{
			SchemaConf: SchemaConf{
				Name:         "Vroom",
				FieldsNaming: []string{"car", "car1", "car2", "car3", "car4"},
				Fields: []FielderConf{
					FieldConf[int]{},
					FieldConf[float64]{},
					FieldConf[int64]{},
					FieldConf[uint64]{},
					FieldConf[time.Time]{},
				},
				Indexes: [][]IndexerConf{},
			},
			MaxMemory: 1024 * 1024 * 1024,
		}
		rmcInt := NewRamCollection(rmCInt)

		if err := testFillingType(rmcInt); err != nil {
			t.Error(err)
		}

	})

	t.Run("idxFullmatchTypes", func(t *testing.T) {

		rmCInt := RamCollectionConf{
			SchemaConf: SchemaConf{
				Name:         "Vroom",
				FieldsNaming: []string{"car"},
				Fields: []FielderConf{
					FieldConf[int]{},
				},
				Indexes: [][]IndexerConf{{
					FullmatchIndexConf[int]{Name: "who"},
					FullmatchIndexConf[float64]{Name: "whom"},
					FullmatchIndexConf[int64]{Name: "whom"},
					FullmatchIndexConf[uint64]{Name: "whom"},
					FullmatchIndexConf[time.Time]{Name: "whom"},
				}},
			},
			MaxMemory: 1024 * 1024 * 1024,
		}
		rmcInt := NewRamCollection(rmCInt)

		if err := testFillingType(rmcInt); err != nil {
			t.Error(err)
		}

	})

	t.Run("idxPrefixIdxTypes", func(t *testing.T) {

		rmCInt := RamCollectionConf{
			SchemaConf: SchemaConf{
				Name:         "Vroom",
				FieldsNaming: []string{"car"},
				Fields: []FielderConf{
					FieldConf[int]{},
				},
				Indexes: [][]IndexerConf{{
					PrefixIndexConf[[]int]{Name: "who"},
					PrefixIndexConf[[]float64]{Name: "whom"},
					PrefixIndexConf[[]int64]{Name: "whom"},
					PrefixIndexConf[[]uint64]{Name: "whom"},
				}},
			},
			MaxMemory: 1024 * 1024 * 1024,
		}
		rmcInt := NewRamCollection(rmCInt)

		if err := testFillingType(rmcInt); err != nil {
			t.Error(err)
		}

	})
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

		rmCL := RamCollectionConf{
			SchemaConf: SchemaConf{
				Name:         "FooBarTable2",
				FieldsNaming: []string{"err"},
				Fields: []FielderConf{
					FieldNotExistingConf{},
					FieldNotExistingConf{},
				},
				Indexes: [][]IndexerConf{},
			},
			MaxMemory: 1024 * 1024 * 1024,
		}

		output := NewRamCollection(rmCL)
		if output != nil {
			t.Error("Should return nil.")
		}

	})

	t.Run("notValidPrefix", func(t *testing.T) {

		type IndexNotExistingConf[T any] struct {
			IndexerConf
			Value string
		}

		rmCE := RamCollectionConf{
			SchemaConf: SchemaConf{
				Name:         "FooBarTable2",
				FieldsNaming: []string{"err"},
				Fields: []FielderConf{
					FieldConf[int]{},
				},
				Indexes: [][]IndexerConf{{
					IndexNotExistingConf[string]{Value: "ahuj"},
				}},
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

	t.Run("removalPrefixIndexer", func(t *testing.T) {
		rmC := RamCollectionConf{
			SchemaConf: SchemaConf{
				Name:         "PathTable",
				FieldsNaming: []string{"path"},
				Fields: []FielderConf{
					FieldConf[[]string]{},
				},
				Indexes: [][]IndexerConf{
					{
						PrefixIndexConf[[]string]{Name: "path"},
					},
				},
			},
		}

		rmc := NewRamCollection(rmC)

		if rmc == nil {
			t.Errorf("Should return valid instance of RamCollection.")
		}

		rs := make([][][]string, 3)
		rs[0] = [][]string{{"ab", "cdef", "ghi"}}
		rs[1] = [][]string{{"ab", "jkl"}}
		rs[2] = [][]string{{"mno", "pane", "pr"}}

		rcrds := fillRecords(rs)
		for _, r := range rcrds {
			_, err := rmc.AddRecord(r)
			if err != nil {
				t.Errorf("Addition cause error %s", err)
				return
			}
		}

		rmc.Inspect()

		if len(rmc.Rows()) != 3 {
			t.Errorf("Exptecting 3 rows.")
		}

		err := rmc.DeleteRecord(RecordConf{Id: 1})
		if err != nil {
			t.Errorf(err.Error())
		}

		if len(rmc.Rows()) != 2 {
			t.Errorf("Exptecting 2 rows.")
		}
		rmc.Inspect()

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

		output, err := smc.Collect()
		if err != nil {
			t.Error(err)
		}

		if len(output) != 3 {
			t.Errorf("The number of items in the output should be 3, but it is %d", len(output))
		}

		testingMap := map[int]bool{1: true, 2: true, 4: true}

		for _, elem := range output {
			if val, found := testingMap[int(elem.Id)]; !found || !val {
				t.Errorf("Found wrong element: %+v", elem)
			}
			col, _ := elem.Cols[0].(FieldConf[[]string])

			switch elem.Id {
			case 1:
				if !reflect.DeepEqual(col.Value, []string{"ab", "cdef", "ghi"}) {
					t.Errorf("Wrong Value, expected [ab, cdef, ghi], got: %+v", col.Value)
				}
			case 2:
				if !reflect.DeepEqual(col.Value, []string{"ab", "jkl"}) {
					t.Errorf("Wrong Value, expected [ab, jkl], got: %+v", col.Value)
				}
			case 4:
				if !reflect.DeepEqual(col.Value, []string{"ab", "pane", "pr"}) {
					t.Errorf("Wrong Value, expected [ab, pane, pr], got: %+v", col.Value)
				}
			}
			testingMap[int(elem.Id)] = false
		}
		fmt.Printf("Returned %+v", output)
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

	t.Run("filterFMindex", func(t *testing.T) {
		rmC := RamCollectionConf{
			SchemaConf: SchemaConf{
				Name:         "Vroom",
				FieldsNaming: []string{"who", "whom", "me", "you", "them"},
				Fields: []FielderConf{
					FieldConf[int]{},
					FieldConf[float64]{},
					FieldConf[int64]{},
					FieldConf[uint64]{},
					FieldConf[time.Time]{},
				},
				Indexes: [][]IndexerConf{{
					FullmatchIndexConf[int]{Name: "who"},
					FullmatchIndexConf[float64]{Name: "whom"},
					FullmatchIndexConf[int64]{Name: "me"},
					FullmatchIndexConf[uint64]{Name: "you"},
					FullmatchIndexConf[time.Time]{Name: "them"},
				}},
			},
			MaxMemory: 1024 * 1024 * 1024,
		}
		rmc := NewRamCollection(rmC)

		rcrds := fillFullmatch(rmc)

		for i := 0; i < 3; i++ {
			_, err := rmc.AddRecord(rcrds[i])
			if err != nil {
				t.Error(err)
			}
		}

		// rmc.Inspect()s

		timeT := time.Time{}

		query := QueryOrConf{
			QueryContextConf{
				Context: []QueryConf{
					QueryAtomConf{
						Name:      "who",
						Value:     14,
						MatchType: FullmatchIndexConf[int]{},
					},
					QueryAtomConf{
						Name:      "whom",
						Value:     float64(45),
						MatchType: FullmatchIndexConf[float64]{},
					},
					QueryAtomConf{
						Name:      "me",
						Value:     int64(34),
						MatchType: FullmatchIndexConf[int64]{},
					},
					QueryAtomConf{
						Name:      "you",
						Value:     uint64(45),
						MatchType: FullmatchIndexConf[uint64]{},
					},
					QueryAtomConf{
						Name:      "them",
						Value:     timeT.Add(time.Duration(4)),
						MatchType: FullmatchIndexConf[time.Time]{},
					},
				},
			},
		}

		smc, err := rmc.Filter(query)
		if err != nil {
			t.Error(err)
		}

		output, err := smc.Collect()
		if err != nil {
			t.Error(err)
		}

		if len(output) != 2 {
			t.Error("Found more or less results, then 2.")
		}

		val1, _ := output[0].Cols[0].(FieldConf[int])
		val2, _ := output[1].Cols[0].(FieldConf[int])

		if (val1.Value != 14 && val2.Value != 15) && (val1.Value != 15 && val2.Value != 14) {
			t.Errorf("Should be <<14, 15>> or <<15, 14>>, but got: %d, %d", val1.Value, val2.Value)
		}
	})

	t.Run("filterPrefixIndex", func(t *testing.T) {
		rmC := RamCollectionConf{
			SchemaConf: SchemaConf{
				Name:         "Vroom",
				FieldsNaming: []string{"who", "whom", "me", "you", "them"},
				Fields: []FielderConf{
					FieldConf[[]string]{},
					FieldConf[[]int]{},
					FieldConf[[]float64]{},
					FieldConf[[]int64]{},
					FieldConf[[]uint64]{},
				},
				Indexes: [][]IndexerConf{{
					PrefixIndexConf[[]string]{Name: "who"},
					PrefixIndexConf[[]int]{Name: "whom"},
					PrefixIndexConf[[]float64]{Name: "me"},
					PrefixIndexConf[[]int64]{Name: "you"},
					PrefixIndexConf[[]uint64]{Name: "them"},
				}},
			},
			MaxMemory: 1024 * 1024 * 1024,
		}
		rmc := NewRamCollection(rmC)

		rcrds := fillPrefix(rmc)

		for i := 0; i < 3; i++ {
			_, err := rmc.AddRecord(rcrds[i])
			if err != nil {
				t.Error(err)
			}
		}

		// rmc.Inspect()

		query := QueryOrConf{
			QueryContextConf{
				Context: []QueryConf{
					QueryAtomConf{
						Name:      "who",
						Value:     []string{"StrNmbr5"},
						MatchType: PrefixIndexConf[[]string]{},
					},
					QueryAtomConf{
						Name:      "whom",
						Value:     []int{14},
						MatchType: PrefixIndexConf[[]int]{},
					},
					QueryAtomConf{
						Name:      "me",
						Value:     []float64{float64(25)},
						MatchType: PrefixIndexConf[[]float64]{},
					},
					QueryAtomConf{
						Name:      "you",
						Value:     []int64{int64(34)},
						MatchType: PrefixIndexConf[[]int64]{},
					},
					QueryAtomConf{
						Name:      "them",
						Value:     []uint64{uint64(45)},
						MatchType: PrefixIndexConf[[]uint64]{},
					},
				},
			},
		}

		smc, err := rmc.Filter(query)
		if err != nil {
			t.Error(err)
		}

		output, err := smc.Collect()
		if err != nil {
			t.Error(err)
		}

		if len(output) != 2 {
			t.Error("Found more or less results, then 2.")
		}

		val1, _ := output[0].Cols[0].(FieldConf[[]string])
		val2, _ := output[1].Cols[0].(FieldConf[[]string])

		if (val1.Value[0] != "StrNmbr5" && val2.Value[0] != "StrNmbr54") && (val1.Value[0] != "StrNmbr4" && val2.Value[0] != "StrNmbr5") {
			t.Errorf("Should be <<StrNmbr4, StrNmbr5>> or <<StrNmbr5, StrNmbr4>>, but got: %s, %s", val1.Value[0], val2.Value[0])
		}

	})
}

func testNthLine(rc []RecordConf, n int) error {
	rclen := len(rc[n].Cols)

	if rclen != 2 {
		return fmt.Errorf("Wrong number of result columns %d", rclen)
	}

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

	// if prefixIndexP {
	// 	rmC.Indexes = append(rmC.Indexes, []IndexerConf{
	// 		PrefixIndexConf[[]rune]{Name: "who"},
	// 		PrefixIndexConf[[]rune]{Name: "whom"},
	// 	})
	// }

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

func testFillingType(rmc *RamCollection) error {
	rcrds := fillRecords([][]int{{111, 222}})

	_, err := rmc.AddRecord(rcrds[0])
	if err != nil {
		return errors.New("Adding record failed.")
	}

	rcrds = fillRecords([][]float64{{111.1, 222.1}})

	_, err = rmc.AddRecord(rcrds[0])
	if err != nil {
		return errors.New("Adding record failed.")
	}

	rcrds = fillRecords([][]int64{{111, 222}})

	_, err = rmc.AddRecord(rcrds[0])
	if err != nil {
		return errors.New("Adding record failed.")
	}

	rcrds = fillRecords([][]uint64{{111, 222}})

	_, err = rmc.AddRecord(rcrds[0])
	if err != nil {
		return errors.New("Adding record failed.")
	}

	rcrds = fillRecords([][]time.Time{{}})

	_, err = rmc.AddRecord(rcrds[0])
	if err != nil {
		return errors.New("Adding record failed.")
	}

	return nil
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

func fillPrefix(rmc *RamCollection) []RecordConf {

	out := make([]RecordConf, 3)

	var row RecordConf

	for r := 4; r < 7; r++ {
		row = RecordConf{
			Cols: make([]FielderConf, 5),
		}
		row.Cols[0] = FieldConf[[]string]{Value: []string{fmt.Sprintf("StrNmbr%d", 0+r), fmt.Sprintf("StrNmbr%d", 1000+r)}}
		row.Cols[1] = FieldConf[[]int]{Value: []int{10 + r, 100 + r}}
		row.Cols[2] = FieldConf[[]float64]{Value: []float64{float64(20.0 + r), float64(200.0 + r)}}
		row.Cols[3] = FieldConf[[]int64]{Value: []int64{int64(30 + r), int64(300 + r)}}
		row.Cols[4] = FieldConf[[]uint64]{Value: []uint64{uint64(40 + r), uint64(400 + r)}}
		out[r-4] = row
	}
	return out

}

func fillFullmatch(rmc *RamCollection) []RecordConf {

	out := make([]RecordConf, 3)

	var row RecordConf

	timeT := new(time.Time)

	for r := 4; r < 7; r++ {
		row = RecordConf{
			Cols: make([]FielderConf, 5),
		}
		row.Cols[0] = FieldConf[int]{Value: 10 + r}
		row.Cols[1] = FieldConf[float64]{Value: 20.0 + float64(r)}
		row.Cols[2] = FieldConf[int64]{Value: 30 + int64(r)}
		row.Cols[3] = FieldConf[uint64]{Value: 40 + uint64(r)}
		row.Cols[4] = FieldConf[time.Time]{Value: timeT.Add(time.Duration(r))}
		out[r-4] = row
	}
	return out

}

func consumeGPfxOutput(rcrds []RecordConf) error {

	return nil
}
