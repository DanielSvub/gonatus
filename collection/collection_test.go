package collection_test

import (
	"fmt"
	"testing"
	"time"

	. "github.com/SpongeData-cz/gonatus/collection"
)

var schemaConf = SchemaConf{
	Name:         "FooBarTable",
	FieldsNaming: []string{"who", "whom"},
	Fields: []FielderConf{
		FieldConf[string]{},
		FieldConf[string]{},
	},
	Indexes: [][]IndexerConf{{}},
}

var solrConnConf = NewSolrConnectionConf(map[string]string{"auth-type": "no", "url": "http://localhost:8983/solr"})

func prepareRAMCollection(fullmatchIndexP bool, prefixIndexP bool, stringPrefixIndexP bool) Collection {
	rmC := RamCollectionConf{
		SchemaConf: schemaConf,
		MaxMemory:  1024 * 1024 * 1024,
	}

	if fullmatchIndexP {

		rmC.Indexes = [][]IndexerConf{
			{
				FullmatchIndexConf[string]{Name: "who"},
				FullmatchIndexConf[string]{Name: "whom"},
			},
		}
	}

	if prefixIndexP {

		rmC.SchemaConf.Fields = []FielderConf{
			FieldConf[[]string]{},
			FieldConf[[]string]{},
		}

		rmC.Indexes = [][]IndexerConf{
			{
				PrefixIndexConf[[]string]{Name: "who"},
				PrefixIndexConf[[]string]{Name: "whom"},
			},
		}
	}

	if stringPrefixIndexP {

		rmC.Indexes = [][]IndexerConf{
			{
				PrefixIndexConf[string]{Name: "who"},
				PrefixIndexConf[string]{Name: "whom"},
			},
		}

	}

	return NewRamCollection(rmC)
}

var collections = []struct {
	name string
	col  Collection
}{}

func initCollections() {
	sc := NewSolrConnection(*solrConnConf)
	sc.DropCollection(schemaConf.Name)
	collections = []struct {
		name string
		col  Collection
	}{
		{"RAM nofullmatch_noprefix_nostringprefix", prepareRAMCollection(false, false, false)},
		{"RAM nofullmatch_noprefix_stringprefix", prepareRAMCollection(false, false, true)},
		//{"RAM nofullmatch_prefix_nostringprefix", prepareRAMCollection(false, true, false)},
		//	{"RAM nofullmatch_prefix_stringprefix", prepareRAMCollection(false, true, true)},
		{"RAM fullmatch_noprefix_nostringprefix", prepareRAMCollection(true, false, false)},
		{"RAM fullmatch_noprefix_stringprefix", prepareRAMCollection(true, false, true)},
		//{"RAM fullmatch_prefix_nostringprefix", prepareRAMCollection(true, true, false)},
		//{"RAM fullmatch_prefix_stringprefix", prepareRAMCollection(true, true, true)},
		{"SOLR", NewSolrCollection(*NewSolrCollectionConf(schemaConf, *solrConnConf, 1, 0))},
	}
}

//TODO continue here : the desired state is to eb able to add collection just to slice above and all test shuld then run automatically even for new collection.
// this means we need to specify a desired output for egeneral collection or the test shoudl have desired behaviour for general collection
// it is probalby not feasible (or maybe even possible to differentiate between different indexers in such tests.

func TestCollection(t *testing.T) {

	inspectOutput := func(output []RecordConf) {
		fmt.Print("\nID   who        whom")
		for _, o := range output {
			fmt.Printf("\n%d", o.Id)
			//	fmt.Printf("\n%+v", o)
			for _, j := range o.Cols {
				//	fmt.Printf("\n%+v", j)
				fmt.Printf(", %s", j.(FieldConf[string]).Value)
			}
		}
		println()
		println()
	}
	initCollections()
	for _, namedCol := range collections {

		t.Run("filterArgument"+namedCol.name, func(t *testing.T) {
			col := namedCol.col
			_, c, err := col.Filter(FilterArgument{
				Limit:     NO_LIMIT,
				QueryConf: new(QueryConf),
			})
			if c != 0 {
				t.Error("Collection expected to be empty!")
			}

			err = testFilling(col, 15, false)
			if err != nil {
				t.Error(err.Error())
			}

			for i := 0; i < 3; i++ {
				row := RecordConf{Cols: make([]FielderConf, 2)}
				row.Cols[0] = FieldConf[string]{Value: fmt.Sprintf("bah%dlol%d", i, (i+10)*11)}
				row.Cols[1] = FieldConf[string]{Value: fmt.Sprintf("ah%dnechapute%d", i, (i+15)*23)}

				col.AddRecord(row)
			}
			col.Commit()
			//col.Inspect()

			// Sort by CId, without Limit and Skip
			query := FilterArgument{
				Limit:     NO_LIMIT,
				QueryConf: new(QueryConf),
			}

			output, err := filterCollect(col, query)
			if err != nil {
				t.Error(err.Error())
			}

			fmt.Print("Only query")

			inspectOutput(output)

			if len(output) != 18 {
				t.Errorf("Expected 18 rows, got %d.", len(output))
			}

			// Sort by CId, with Limit and Skip
			query.Skip = 4
			query.Limit = 6

			output, err = filterCollect(col, query)
			if err != nil {
				t.Error(err.Error())
			}

			fmt.Print("Skip: 4 and Limit: 6")
			inspectOutput(output)

			if len(output) != 6 {
				t.Errorf("Expected 6 rows, got: %d", len(output))
			}
			if len(output) > 0 && (output[0].Id != 5 || output[len(output)-1].Id != 10) {
				t.Error("Wrong order of output.")
			}

			// Sort by "who", without Limit and Skip
			query.Sort = []string{"who"}
			query.Skip = 0
			query.Limit = NO_LIMIT
			output, err = filterCollect(col, query)
			if err != nil {
				t.Error(err.Error())
			}

			fmt.Printf("Sorted by \"who\"")
			inspectOutput(output)

			if len(output) > 0 && (output[0].Id != 16 || output[len(output)-1].Id != 10) {
				t.Error("Wrong order of output.")
			}

			// Sort by "who" DESC, with Limit and Skip
			query.SortOrder = DESC
			query.Skip = 4
			query.Limit = 6
			output, err = filterCollect(col, query)
			if err != nil {
				t.Error(err.Error())
			}

			fmt.Print("Sorted by \"who\" DESC with Limit and Skip")
			inspectOutput(output)

			if len(output) > 0 && (output[0].Id != 6 || output[len(output)-1].Id != 15) {
				t.Error("Wrong order of output.")
			}

		})
	}

	t.Run("usage", func(t *testing.T) {
		col := prepareRAMCollection(false, false, false)
		err := testFilling(col, 2, false)
		if err != nil {
			t.Error(err.Error())
		}

		// rmc.Inspect()

		query := FilterArgument{Limit: NO_LIMIT}
		query.QueryConf = QueryAndConf{
			QueryContextConf{
				Context: []QueryConf{
					QueryAtomConf{
						Name:      "who",
						Value:     "row0_str110",
						MatchType: FullmatchIndexConf[string]{},
					},
					QueryAtomConf{
						Name:      "whom",
						Value:     "row0_str345",
						MatchType: FullmatchIndexConf[string]{},
					},
				},
			},
		}

		output, err := filterCollect(col, query)
		if err != nil {
			t.Error(err.Error())
		}
		if len(output) != 1 {
			t.Error("Found more or less results, than 1.")
		}

		rcrds := fillRecords([][]string{{"kokos@jablko.cz", "okurek@lilek.com"}})
		record := rcrds[0]
		record.Id = 69

		id, err := col.AddRecord(record)
		if err != nil {
			t.Error(err.Error())
		}
		if id != 69 {
			t.Errorf("Expected Id 69 but %d given", id)
		}

		if recordCount(col) != 3 {
			t.Errorf("Exptecting 3 rows after add third.")
		}

		// rmc.Inspect()

		output, err = filterCollect(col, query)
		if err != nil {
			t.Error(err.Error())
		}

		if len(output) != 1 {
			t.Error("Found more or less results, than 1.")
		}

		err = col.DeleteRecord(RecordConf{Id: 1})
		if err != nil {
			t.Errorf(err.Error())
		}

		if recordCount(col) != 2 {
			t.Errorf("Exptecting 2 rows after remove third.")
		}

		// rmc.Inspect()

		query.QueryConf = QueryOrConf{
			QueryContextConf{
				Context: []QueryConf{
					QueryAtomConf{
						Name:      "who",
						Value:     "row1_str121",
						MatchType: FullmatchIndexConf[string]{},
					},
					QueryAtomConf{
						Name:      "whom",
						Value:     "okurek@lilek.com",
						MatchType: FullmatchIndexConf[string]{},
					},
				},
			},
		}

		output, err = filterCollect(col, query)
		if err != nil {
			t.Error(err.Error())
		}
		if len(output) != 2 {
			t.Error("Found more or less results, than 2.")
		}

		err = col.DeleteRecord(RecordConf{Id: 2})
		if err != nil {
			t.Errorf(err.Error())
		}

		if recordCount(col) != 1 {
			t.Errorf("Exptecting 1 rows after remove second.")
		}

		// rmc.Inspect()

		err = col.DeleteRecord(RecordConf{Id: 69})
		if err != nil {
			t.Errorf(err.Error())
		}

		if recordCount(col) != 0 {
			t.Errorf("Exptecting 0 rows after remove first.")
		}

		// rmc.Inspect()

		output, err = filterCollect(col, query)
		if err != nil {
			t.Error(err.Error())
		}
		if len(output) != 0 {
			t.Error("Found more or less results, than 2.")
		}

		rcrds = fillRecords([][]string{{"pata", ""}})
		record = rcrds[0]
		record.Id = 32

		id, err = col.AddRecord(record)
		if err != nil {
			t.Error(err.Error())
		}
		if id != 32 {
			t.Errorf("Expected Id 32 but %d given", id)
		}

		if recordCount(col) != 1 {
			t.Errorf("Exptecting 1 rows after adding one.")
		}

		// rmc.Inspect()

	})

	// BASIC
	t.Run("serialization", func(t *testing.T) {
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

		rmc := NewRamCollection(rmC)
		rmCT := rmc.Serialize().(RamCollectionConf)

		if !(len(rmCT.Fields) == len(rmC.Fields) && len(rmCT.FieldsNaming) == len(rmC.FieldsNaming)) {
			t.Error("Not equal headers.")
		}

		if !(rmCT.FieldsNaming[0] == "who" && rmCT.FieldsNaming[1] == "whom") {
			t.Error("Field not named correctly.")
		}

	})
	t.Run("basicWithoutIndexer", func(t *testing.T) {

		rmc := prepareRAMCollection(false, false, false)
		if rmc == nil {
			t.Errorf("Should return valid instance of RamCollection.")
		}
		err := testFilling(rmc, 2, false)
		if err != nil {
			t.Error(err.Error())
		}

		// rmc.Inspect()

		// Query to return all results
		query := FilterArgument{Limit: NO_LIMIT, QueryConf: new(QueryConf)}
		output, err := filterCollect(rmc, query)
		if err != nil {
			t.Error(err.Error())
		}

		if len(output) != 2 {
			t.Errorf("Found more or less results, expected <<2>>, got: %d", len(output))
		}

		row1 := output[0]
		row2 := output[1]
		r1c1 := output[0].Cols[0].(FieldConf[string])
		r1c2 := output[0].Cols[1].(FieldConf[string])
		r2c1 := output[1].Cols[0].(FieldConf[string])
		r2c2 := output[1].Cols[1].(FieldConf[string])

		if (row1.Id == 1 && row2.Id == 2) || (row1.Id == 2 && row2.Id == 1) {
			if !((r1c1.Value == "row0_str110" && r1c2.Value == "row0_str345" && r2c1.Value == "row1_str121" && r2c2.Value == "row1_str368") ||
				(r1c1.Value == "row1_str121" && r1c2.Value == "row1_str368" && r2c1.Value == "row0_str110" && r2c2.Value == "row0_str345")) {
				t.Error("Wrong values.")
			}
		} else {
			t.Error("Wrong ids.")
		}

	})
	t.Run("basicFullmatchIndexer", func(t *testing.T) {
		rmc := prepareRAMCollection(true, false, false)
		if rmc == nil {
			t.Errorf("Should return valid instance of RamCollection.")
		}

		err := testFilling(rmc, 2, false)
		if err != nil {
			t.Error(err.Error())
		}

		// rmc.Inspect()

		// Query to return one row
		query := FilterArgument{
			Limit: NO_LIMIT,
			QueryConf: QueryAtomConf{
				Name:      "who",
				Value:     "row0_str110",
				MatchType: FullmatchIndexConf[string]{},
			}}

		output, err := filterCollect(rmc, query)
		if err != nil {
			t.Error(err.Error())
		}

		if len(output) != 1 {
			t.Error("Found more or less results, then 1.")
		}

		r1c1 := output[0].Cols[0].(FieldConf[string])
		r1c2 := output[0].Cols[1].(FieldConf[string])
		if !(r1c1.Value == "row0_str110" && r1c2.Value == "row0_str345") {
			t.Error("Wrong values.")
		}
	})
	t.Run("basicPrefixIndexer", func(t *testing.T) {
		rmc := prepareRAMCollection(false, true, false)
		if rmc == nil {
			t.Errorf("Should return valid instance of RamCollection.")
		}

		err := testFilling(rmc, 2, true)
		if err != nil {
			t.Error(err.Error())
		}

		// rmc.Inspect()

		// Query to return all results
		query := FilterArgument{
			Limit: NO_LIMIT,
			QueryConf: QueryAtomConf{
				Name:      "who",
				Value:     []string{},
				MatchType: PrefixIndexConf[[]string]{},
			}}

		output, err := filterCollect(rmc, query)
		if err != nil {
			t.Error(err.Error())
		}

		if len(output) != 2 {
			t.Error("Found more or less results, then 2.")
		}

		row1 := output[0]
		row2 := output[1]
		r1c1 := output[0].Cols[0].(FieldConf[[]string])
		r1c2 := output[0].Cols[1].(FieldConf[[]string])
		r2c1 := output[1].Cols[0].(FieldConf[[]string])
		r2c2 := output[1].Cols[1].(FieldConf[[]string])

		if row1.Id == 1 {
			if r1c1.Value[0] != "row0_str110" || r2c2.Value[0] != "row1_str368" {
				t.Error("Wrong values.")
			}
		} else if row2.Id == 1 {
			if r1c2.Value[0] != "row1_str368" || r2c1.Value[1] != "row0_str252" {
				t.Error("Wrong values.")
			}
		} else {
			t.Error("Wrong ids.")
		}
	})
	t.Run("basicStringPrefix", func(t *testing.T) {

		rmc := prepareRAMCollection(false, false, true)
		if rmc == nil {
			t.Errorf("Should return valid instance of RamCollection.")
		}

		err := testFilling(rmc, 6, false)
		if err != nil {
			t.Error(err.Error())
		}

		// rmc.Inspect()

		query := FilterArgument{
			Limit: NO_LIMIT,
			QueryConf: QueryAtomConf{
				Name:      "who",
				Value:     "row1",
				MatchType: PrefixIndexConf[string]{},
			}}

		output, err := filterCollect(rmc, query)
		if err != nil {
			t.Error(err.Error())
		}

		if len(output) != 1 {
			t.Errorf("Expecting 1 result, got: %d", len(output))
		}

		err = rmc.DeleteRecord(RecordConf{Id: 4})
		if err != nil {
			t.Error(err.Error())
		}

		// rmc.Inspect()

		query.QueryConf = QueryAtomConf{
			Name:      "who",
			Value:     "row",
			MatchType: PrefixIndexConf[string]{},
		}

		output, err = filterCollect(rmc, query)
		if err != nil {
			t.Error(err.Error())
		}
		if len(output) != 5 {
			t.Errorf("Expecting 5 result, got: %d", len(output))
		}

	})
	t.Run("basicPrefixLookupWithoutIndexer", func(t *testing.T) {
		rmC := RamCollectionConf{
			SchemaConf: SchemaConf{
				Name:         "FooBarTable",
				FieldsNaming: []string{"who", "whom"},
				Fields: []FielderConf{
					FieldConf[[]string]{},
					FieldConf[[]string]{},
				},
				Indexes: [][]IndexerConf{},
			},
			MaxMemory: 1024 * 1024 * 1024,
		}
		rmc := NewRamCollection(rmC)

		err := testFilling(rmc, 6, true)
		if err != nil {
			t.Error(err.Error())
		}

		// rmc.Inspect()
		query := FilterArgument{
			Limit: NO_LIMIT,
			QueryConf: QueryAtomConf{
				Name:      "who",
				Value:     []string{"row5_str165", "row5_str312"},
				MatchType: PrefixIndexConf[[]string]{},
			}}

		output, err := filterCollect(rmc, query)
		if err != nil {
			t.Error(err.Error())
		}

		if len(output) != 1 {
			t.Errorf("Expect 1 result, got: %d", len(output))
		}

		query.QueryConf = QueryAtomConf{
			Name:      "who",
			Value:     []string{"row5_str165", "row15_str312"},
			MatchType: PrefixIndexConf[[]string]{},
		}

		output, err = filterCollect(rmc, query)
		if err != nil {
			t.Error(err.Error())
		}

		if len(output) != 0 {
			t.Errorf("Expect 0 result, got: %d", len(output))
		}

	})

	// EDIT
	t.Run("editRecord", func(t *testing.T) {
		col := prepareRAMCollection(true, false, false)
		err := testFilling(col, 2, false)
		if err != nil {
			t.Error(err.Error())
		}

		//col.Inspect()

		rc := RecordConf{Id: 1, Cols: make([]FielderConf, 2)}
		rc.Cols[0] = FieldConf[string]{Value: "imChanged@text.me"}
		rc.Cols[1] = FieldConf[string]{Value: "row0_str345"}

		err = col.EditRecord(rc)
		if err != nil {
			t.Error(err.Error())
		}

		//col.Inspect()
		rcN := RecordConf{Id: 42, Cols: make([]FielderConf, 3)}
		rcN.Cols[0] = FieldConf[string]{Value: "imChanged@text.me"}
		rcN.Cols[1] = FieldConf[string]{Value: "nowhereToSee@msg.me"}
		rcN.Cols[2] = FieldConf[string]{Value: "ImDolphin@eueu.me"}
		err = col.EditRecord(rcN)
		if err == nil {
			t.Error("No record with id 42 shoud be found.")
		}

		rcN.Id = 2
		err = col.EditRecord(rcN)
		if err == nil {
			t.Error("Should throw an error \"wrong number of columns.\"")
		}

	})

	// TYPES
	t.Run("typesPrimaryPrefix", func(t *testing.T) {
		rmC := RamCollectionConf{
			SchemaConf: SchemaConf{
				Name:         "Vroom",
				FieldsNaming: []string{"col0", "col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10", "col11", "col12", "col13"},
				Fields: []FielderConf{
					FieldConf[string]{},
					FieldConf[[]string]{},
					FieldConf[[]int]{},
					FieldConf[[]int8]{},
					FieldConf[[]int16]{},
					FieldConf[[]int32]{},
					FieldConf[[]int64]{},
					FieldConf[[]uint]{},
					FieldConf[[]uint8]{},
					FieldConf[[]uint16]{},
					FieldConf[[]uint32]{},
					FieldConf[[]uint64]{},
					FieldConf[[]float32]{},
					FieldConf[[]float64]{},
				},
				Indexes: [][]IndexerConf{},
			},
			MaxMemory: 1024 * 1024 * 1024,
		}
		rmc := NewRamCollection(rmC)
		if rmc == nil {
			t.Errorf("Should return valid instance of RamCollection.")
		}

		rcrds := fillPrefix(rmc)

		for i := 0; i < 3; i++ {
			_, err := rmc.AddRecord(rcrds[i])
			if err != nil {
				t.Error(err)
			}
		}

		// rmc.Inspect()

		output, err := getOut(true, rmc)
		if err != nil {
			t.Error(err)
		}

		if len(output) != 2 {
			t.Error("Found more or less results, then 2.")
		}

		val1, _ := output[0].Cols[1].(FieldConf[[]string])
		val2, _ := output[1].Cols[1].(FieldConf[[]string])

		if (val1.Value[1] != "StrNmbr1004" && val2.Value[1] != "StrNmbr1005") && (val1.Value[1] != "StrNmbr1005" && val2.Value[1] != "StrNmbr1004") {
			t.Errorf("Should be <<StrNmbr1004, StrNmbr1005>> or <<StrNmbr1005, StrNmbr1004>>, but got: %s, %s", val1.Value[1], val2.Value[1])
		}

	})
	t.Run("typesPrimaryFM", func(t *testing.T) {
		rmC := RamCollectionConf{
			SchemaConf: SchemaConf{
				Name:         "Vroom",
				FieldsNaming: []string{"col0", "col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10", "col11", "col12", "col13", "col14", "col15", "col16", "col17", "col18", "col19", "col20", "col21", "col22", "col23", "col24", "col25", "col26"},
				Fields: []FielderConf{
					FieldConf[string]{},
					FieldConf[int]{},
					FieldConf[int8]{},
					FieldConf[int16]{},
					FieldConf[int32]{},
					FieldConf[int64]{},
					FieldConf[uint]{},
					FieldConf[uint8]{},
					FieldConf[uint16]{},
					FieldConf[uint32]{},
					FieldConf[uint64]{},
					FieldConf[float32]{},
					FieldConf[float64]{},
					FieldConf[time.Time]{},
					FieldConf[[]string]{},
					FieldConf[[]int]{},
					FieldConf[[]int8]{},
					FieldConf[[]int16]{},
					FieldConf[[]int32]{},
					FieldConf[[]int64]{},
					FieldConf[[]uint]{},
					FieldConf[[]uint8]{},
					FieldConf[[]uint16]{},
					FieldConf[[]uint32]{},
					FieldConf[[]uint64]{},
					FieldConf[[]float32]{},
					FieldConf[[]float64]{},
				},
				Indexes: [][]IndexerConf{},
			},
			MaxMemory: 1024 * 1024 * 1024,
		}
		rmc := NewRamCollection(rmC)
		if rmc == nil {
			t.Errorf("Should return valid instance of RamCollection.")
		}

		rcrds := fillFullmatch(rmc)

		for i := 0; i < 3; i++ {
			_, err := rmc.AddRecord(rcrds[i])
			if err != nil {
				t.Error(err)
			}
		}

		// rmc.Inspect()

		output, err := getOut(false, rmc)
		if err != nil {
			t.Error(err)
		}

		if len(output) != 2 {
			t.Error("Found more or less results, then 2.")
		}

		val1, _ := output[0].Cols[0].(FieldConf[string])
		val2, _ := output[1].Cols[0].(FieldConf[string])

		if (val1.Value != "str4" && val2.Value != "str5") && (val1.Value != "str5" && val2.Value != "str4") {
			t.Errorf("Should be <<str14, str15>> or <<str15, str14>>, but got: %s, %s", val1.Value, val2.Value)
		}

	})
	t.Run("typesWfullmatch", func(t *testing.T) {
		rmC := RamCollectionConf{
			SchemaConf: SchemaConf{
				Name:         "Vroom",
				FieldsNaming: []string{"col0", "col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10", "col11", "col12", "col13", "col14", "col15", "col16", "col17", "col18", "col19", "col20", "col21", "col22", "col23", "col24", "col25", "col26"},
				Fields: []FielderConf{
					FieldConf[string]{},
					FieldConf[int]{},
					FieldConf[int8]{},
					FieldConf[int16]{},
					FieldConf[int32]{},
					FieldConf[int64]{},
					FieldConf[uint]{},
					FieldConf[uint8]{},
					FieldConf[uint16]{},
					FieldConf[uint32]{},
					FieldConf[uint64]{},
					FieldConf[float32]{},
					FieldConf[float64]{},
					FieldConf[time.Time]{},
					FieldConf[[]string]{},
					FieldConf[[]int]{},
					FieldConf[[]int8]{},
					FieldConf[[]int16]{},
					FieldConf[[]int32]{},
					FieldConf[[]int64]{},
					FieldConf[[]uint]{},
					FieldConf[[]uint8]{},
					FieldConf[[]uint16]{},
					FieldConf[[]uint32]{},
					FieldConf[[]uint64]{},
					FieldConf[[]float32]{},
					FieldConf[[]float64]{},
				},
				Indexes: [][]IndexerConf{{
					FullmatchIndexConf[string]{Name: "col0"},
					FullmatchIndexConf[int]{Name: "col1"},
					FullmatchIndexConf[int8]{Name: "col2"},
					FullmatchIndexConf[int16]{Name: "col3"},
					FullmatchIndexConf[int32]{Name: "col4"},
					FullmatchIndexConf[int64]{Name: "col5"},
					FullmatchIndexConf[uint]{Name: "col6"},
					FullmatchIndexConf[uint8]{Name: "col7"},
					FullmatchIndexConf[uint16]{Name: "col8"},
					FullmatchIndexConf[uint32]{Name: "col9"},
					FullmatchIndexConf[uint64]{Name: "col10"},
					FullmatchIndexConf[float32]{Name: "col11"},
					FullmatchIndexConf[float64]{Name: "col12"},
					FullmatchIndexConf[time.Time]{Name: "col13"},
					FullmatchIndexConf[[]string]{Name: "col14"},
					FullmatchIndexConf[[]int]{Name: "col15"},
					FullmatchIndexConf[[]int8]{Name: "col16"},
					FullmatchIndexConf[[]int16]{Name: "col17"},
					FullmatchIndexConf[[]int32]{Name: "col18"},
					FullmatchIndexConf[[]int64]{Name: "col19"},
					FullmatchIndexConf[[]uint]{Name: "col20"},
					FullmatchIndexConf[[]uint8]{Name: "col21"},
					FullmatchIndexConf[[]uint16]{Name: "col22"},
					FullmatchIndexConf[[]uint32]{Name: "col23"},
					FullmatchIndexConf[[]uint64]{Name: "col24"},
					FullmatchIndexConf[[]float32]{Name: "col25"},
					FullmatchIndexConf[[]float64]{Name: "col26"},
				}},
			},
			MaxMemory: 1024 * 1024 * 1024,
		}
		rmc := NewRamCollection(rmC)
		if rmc == nil {
			t.Errorf("Should return valid instance of RamCollection.")
		}

		rcrds := fillFullmatch(rmc)

		for i := 0; i < 3; i++ {
			_, err := rmc.AddRecord(rcrds[i])
			if err != nil {
				t.Error(err)
			}
		}
		// rmc.Inspect()

		output, err := getOut(false, rmc)
		if err != nil {
			t.Error(err)
		}

		if len(output) != 2 {
			t.Error("Found more or less results, then 2.")
		}

		val1, _ := output[0].Cols[0].(FieldConf[string])
		val2, _ := output[1].Cols[0].(FieldConf[string])

		if (val1.Value != "str4" && val2.Value != "str5") && (val1.Value != "str5" && val2.Value != "str4") {
			t.Errorf("Should be <<str14, str15>> or <<str15, str14>>, but got: %s, %s", val1.Value, val2.Value)
		}

	})
	t.Run("typesWprefix", func(t *testing.T) {
		rmC := RamCollectionConf{
			SchemaConf: SchemaConf{
				Name:         "Vroom",
				FieldsNaming: []string{"col0", "col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10", "col11", "col12", "col13"},
				Fields: []FielderConf{
					FieldConf[string]{},
					FieldConf[[]string]{},
					FieldConf[[]int]{},
					FieldConf[[]int8]{},
					FieldConf[[]int16]{},
					FieldConf[[]int32]{},
					FieldConf[[]int64]{},
					FieldConf[[]uint]{},
					FieldConf[[]uint8]{},
					FieldConf[[]uint16]{},
					FieldConf[[]uint32]{},
					FieldConf[[]uint64]{},
					FieldConf[[]float32]{},
					FieldConf[[]float64]{},
				},
				Indexes: [][]IndexerConf{{
					PrefixIndexConf[string]{Name: "col0"},
					PrefixIndexConf[[]string]{Name: "col1"},
					PrefixIndexConf[[]int]{Name: "col2"},
					PrefixIndexConf[[]int8]{Name: "col3"},
					PrefixIndexConf[[]int16]{Name: "col4"},
					PrefixIndexConf[[]int32]{Name: "col5"},
					PrefixIndexConf[[]int64]{Name: "col6"},
					PrefixIndexConf[[]uint]{Name: "col7"},
					PrefixIndexConf[[]uint8]{Name: "col8"},
					PrefixIndexConf[[]uint16]{Name: "col9"},
					PrefixIndexConf[[]uint32]{Name: "col10"},
					PrefixIndexConf[[]uint64]{Name: "col11"},
					PrefixIndexConf[[]float32]{Name: "col12"},
					PrefixIndexConf[[]float64]{Name: "col13"},
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

		output, err := getOut(true, rmc)
		if err != nil {
			t.Error(err)
		}

		if len(output) != 2 {
			t.Error("Found more or less results, then 2.")
		}

		val1, _ := output[0].Cols[1].(FieldConf[[]string])
		val2, _ := output[1].Cols[1].(FieldConf[[]string])

		if (val1.Value[1] != "StrNmbr1004" && val2.Value[1] != "StrNmbr1005") && (val1.Value[1] != "StrNmbr1005" && val2.Value[1] != "StrNmbr1004") {
			t.Errorf("Should be <<StrNmbr1004, StrNmbr1005>> or <<StrNmbr1005, StrNmbr1004>>, but got: %s, %s", val1.Value[1], val2.Value[1])
		}

	})

	// REMOVAL
	t.Run("removalWithoutIndexer", func(t *testing.T) {
		col := prepareRAMCollection(false, false, false)
		err := testFilling(col, 2, false)
		if err != nil {
			t.Error(err.Error())
		}
		rcrds := fillRecords([][]string{{"ba@cd.tv", "gt@go.co.uk"}})
		record := rcrds[0]
		record.Id = 1337

		id, err := col.AddRecord(record)
		if err != nil {
			t.Error(err.Error())
		}
		if id != 1337 {
			t.Errorf("Expected Id 1337 but %d given", id)
		}

		id, err = col.AddRecord(record)
		if err == nil {
			t.Errorf("Repeating Id should lead to error but got nil.")
		}
		if id.ValidP() {
			t.Errorf("Repeating Id should lead to invalid Id got %d.", id)
		}

		err = col.DeleteRecord(RecordConf{Id: 1})
		if err != nil {
			t.Errorf(err.Error())
		}

		if recordCount(col) != 2 {
			t.Errorf("Exptecting 2 rows after remove third.")
		}

		rcr := RecordConf{Id: 2222}
		err = col.DeleteRecord(rcr)
		if err == nil {
			t.Errorf("Removal of not existing record should lead to error.")
		}
	})
	t.Run("removalFMindexer", func(t *testing.T) {
		col := prepareRAMCollection(true, false, false)

		err := testFilling(col, 2, false)
		if err != nil {
			t.Error(err)
		}

		rcrds := fillRecords([][]string{{"ba@cd.tv", "gt@go.co.uk"}})
		record := rcrds[0]
		record.Id = 1337

		id, err := col.AddRecord(record)
		if err != nil {
			t.Error(err.Error())
		}
		if id != 1337 {
			t.Errorf("Expected Id 1337 but %d given", id)
		}

		id, err = col.AddRecord(record)
		if err == nil {
			t.Errorf("Repeating Id should lead to error but got nil.")
		}

		if id.ValidP() {
			t.Errorf("Repeating Id should lead to invalid Id got %d.", id)
		}

		err = col.DeleteRecord(RecordConf{Id: 1})
		if err != nil {
			t.Errorf(err.Error())
		}

		if recordCount(col) != 2 {
			t.Errorf("Exptecting 2 rows after remove third.")
		}

		rcr := RecordConf{Id: 2222}
		err = col.DeleteRecord(rcr)
		if err == nil {
			t.Errorf("Removal of not existing record should lead to error.")
		}
	})
	t.Run("removalPrefixIndexer", func(t *testing.T) {
		col := prepareRAMCollection(false, true, false)
		err := testFilling(col, 2, true)
		if err != nil {
			t.Error(err.Error())
		}

		rcrds := fillRecords([][][]string{{{"a@b.cz", "gt@go.co.uk"}, {"uho@fr.kj"}}})
		record := rcrds[0]
		record.Id = 1337

		id, err := col.AddRecord(record)
		if err != nil {
			t.Error(err.Error())
		}
		if id != 1337 {
			t.Errorf("Expected Id 1337 but %d given", id)
		}

		id, err = col.AddRecord(record)
		if err == nil {
			t.Errorf("Repeating Id should lead to error but got nil.")
		}
		if id.ValidP() {
			t.Errorf("Repeating Id should lead to invalid Id got %d.", id)
		}

		err = col.DeleteRecord(RecordConf{Id: 1})
		if err != nil {
			t.Errorf(err.Error())
		}

		if recordCount(col) != 2 {
			t.Errorf("Exptecting 2 rows after remove third.")
		}

		rcr := RecordConf{Id: 2222}
		err = col.DeleteRecord(rcr)
		if err == nil {
			t.Errorf("Removal of not existing record should lead to error.")
		}

	})
	t.Run("removalByFilter", func(t *testing.T) {
		col := prepareRAMCollection(true, false, false)

		err := testFilling(col, 2, false)
		if err != nil {
			t.Error(err.Error())
		}

		// rmc.Inspect()
		query := FilterArgument{
			Limit: NO_LIMIT,
			QueryConf: QueryAtomConf{
				Name:      "who",
				Value:     "row1_str121",
				MatchType: FullmatchIndexConf[string]{},
			}}

		_, err = col.DeleteByFilter(query) //TODO _ -> count check
		if err != nil {
			t.Error(err.Error())
		}

		// rmc.Inspect()

		if recordCount(col) != 1 {
			t.Error("Expecting 1 row after remove second.")
		}

		// Non-valid
		query.QueryConf = QueryAtomConf{
			Name:      "nonexisting",
			Value:     "row1_str121",
			MatchType: FullmatchIndexConf[string]{},
		}

		_, err = col.DeleteByFilter(query) //TODO _ -> count check
		if err == nil {
			t.Error("Should throw an error")
		}

		// rmc.Inspect()

		if recordCount(col) != 1 {
			t.Error("Expecting 1 row after remove second.")
		}

		// Valid and-conf

		query.QueryConf = QueryAndConf{
			QueryContextConf{
				Context: []QueryConf{
					QueryAtomConf{
						Name:      "who",
						Value:     "row0_str110",
						MatchType: FullmatchIndexConf[string]{},
					},
					QueryAtomConf{
						Name:      "whom",
						Value:     "row0_str345",
						MatchType: FullmatchIndexConf[string]{},
					},
				},
			},
		}

		_, err = col.DeleteByFilter(query) //TODO _ -> count check
		if err != nil {
			t.Error(err.Error())
		}

		if recordCount(col) != 0 {
			t.Error("Expecting 0 row after remove second.")
		}

		// Valid and-conf
		query.QueryConf = QueryAndConf{QueryContextConf{Context: []QueryConf{}}}

		_, err = col.DeleteByFilter(query) //TODO _ -> count check
		if err != nil {
			t.Error(err.Error())
		}

		if recordCount(col) != 0 {
			t.Error("Expecting 0 row after remove second.")
		}

	})

	// QUERY
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

	// ERRORS / PANICS
	t.Run("error", func(t *testing.T) {
		// Test limit id
		rmc := prepareRAMCollection(true, false, false)

		err := testFilling(rmc, 2, false)

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
					FieldConf[string]{},
					FieldConf[string]{},
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
	t.Run("notValidIndex", func(t *testing.T) {

		// Non-valid indexer
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
					IndexNotExistingConf[int]{Value: "err"},
				}},
			},
			MaxMemory: 1024 * 1024 * 1024,
		}

		rmc := NewRamCollection(rmCE)
		if rmc != nil {
			t.Error("Shoud return nil RamCollection")
		}

		// Valid indexer, but with non-valid column Name
		rmCE = RamCollectionConf{
			SchemaConf: SchemaConf{
				Name:         "FooBarTable2",
				FieldsNaming: []string{"err"},
				Fields: []FielderConf{
					FieldConf[string]{},
				},
				Indexes: [][]IndexerConf{{
					FullmatchIndexConf[string]{Name: "ImNotHere"},
				}},
			},
			MaxMemory: 1024 * 1024 * 1024,
		}

		rmc = NewRamCollection(rmCE)
		if rmc != nil {
			t.Error("Shoud return nil RamCollection")
		}

		// duplicate valid indexer
		rmCE = RamCollectionConf{
			SchemaConf: SchemaConf{
				Name:         "FooBarTable2",
				FieldsNaming: []string{"err"},
				Fields: []FielderConf{
					FieldConf[string]{},
				},
				Indexes: [][]IndexerConf{{
					FullmatchIndexConf[string]{Name: "err"},
					FullmatchIndexConf[string]{Name: "err"},
				}},
			},
			MaxMemory: 1024 * 1024 * 1024,
		}

		rmc = NewRamCollection(rmCE)
		if rmc != nil {
			t.Error("Shoud return nil RamCollection")
		}

	})
	t.Run("notValidQuery", func(t *testing.T) {
		rmc := prepareRAMCollection(false, false, false)
		err := testFilling(rmc, 2, false)
		if err != nil {
			t.Error(err.Error())
		}
		query := FilterArgument{
			Limit: NO_LIMIT,
			QueryConf: QueryAtomConf{
				Name:  "noExisting",
				Value: "string",
			}}
		_, _, err = rmc.Filter(query) //TODO _ -> count check
		if err == nil {
			t.Error("Should throw en error column not found.")
		}

		query.QueryConf = QueryAtomConf{Name: "who", Value: "row0_str110"}

		_, _, err = rmc.Filter(query) //TODO _ -> count check
		if err == nil {
			t.Error("Should throw en error: not valid prefix in query")
		}

		// Returns all rows
		query.QueryConf = QueryAndConf{QueryContextConf{}}

		output, _ := filterCollect(rmc, query)
		if len(output) != 2 {
			t.Error("Should return 2 rows.")
		}

		// Returns empty rows
		query.QueryConf = QueryOrConf{QueryContextConf{}}

		output, _ = filterCollect(rmc, query)
		if len(output) != 0 {
			t.Error("Should return 0 rows.")
		}

	})

	// TODO
	t.Run("commit", func(t *testing.T) {
		// TODO: ...
		rmc := prepareRAMCollection(true, true, false)
		err := rmc.Commit()
		if err != nil {
			t.Error(err.Error())
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

	if col1.Value != "row0_str110" && col1.Value != "row1_str121" {
		return fmt.Errorf("Wrong order of values, got: %s", col1.Value)
	}

	col1_2, ok1_2 := rc[n].Cols[1].(FieldConf[string])

	if col1.Value == "row0_str110" {
		if !ok1_2 {
			return fmt.Errorf("Cannot cast to the original FieldStringConf.")
		}
		if col1_2.Value != "row0_str345" {
			return fmt.Errorf("Wrong second value of column 1.")
		}
	} else {
		if !ok1_2 {
			return fmt.Errorf("Cannot cast to the original FieldStringConf.")
		}
		if col1_2.Value != "row1_str368" {
			return fmt.Errorf("Wrong second value of column 2.")
		}
	}

	return nil
}

func testFilling(col Collection, iteration uint64, prefixI bool) error {
	var i uint64
	for i = 0; i < iteration; i++ {
		row := RecordConf{Cols: make([]FielderConf, 2)}
		if prefixI {
			row.Cols[0] = FieldConf[[]string]{Value: []string{fmt.Sprintf("row%d_str%d", i, (i+10)*11), fmt.Sprintf("row%d_str%d", i, (i+21)*12)}}
			row.Cols[1] = FieldConf[[]string]{Value: []string{fmt.Sprintf("row%d_str%d", i, (i+15)*23), fmt.Sprintf("row%d_str%d", i, (i+51)*48)}}
		} else {
			row.Cols[0] = FieldConf[string]{Value: fmt.Sprintf("row%d_str%d", i, (i+10)*11)}
			row.Cols[1] = FieldConf[string]{Value: fmt.Sprintf("row%d_str%d", i, (i+15)*23)}
		}
		id, err := col.AddRecord(row)
		if id != CId(i+1) {
			return fmt.Errorf("Expecting: %d, got: %d\n", i+1, id)
		} else if err != nil {
			return err
		}
	}
	col.Commit()

	rCount := recordCount(col)
	if rCount != iteration {
		return fmt.Errorf("Expecting %d rows, got: %d\n", iteration, rCount)
	}

	return nil
}

func filterCollect(col Collection, fa FilterArgument) ([]RecordConf, error) {
	smc, _, err := col.Filter(fa) //TODO _ -> count check
	if err != nil {
		return nil, err
	}

	output, err := smc.Collect()
	if err != nil {
		return nil, err
	}
	return output, err
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

func testLogical(t *testing.T, op string) []RecordConf {
	rmc := prepareRAMCollection(false, false, false)

	err := testFilling(rmc, 2, false)
	if err != nil {
		t.Error(err)
	}

	// rmc.Inspect()

	query := FilterArgument{Limit: NO_LIMIT}

	if op == "or" {
		query.QueryConf = QueryOrConf{
			QueryContextConf{
				Context: []QueryConf{
					QueryAtomConf{
						Name:      "who",
						Value:     "row0_str110",
						MatchType: FullmatchIndexConf[string]{},
					},
					QueryAtomConf{
						Name:      "whom",
						Value:     "row1_str368",
						MatchType: FullmatchIndexConf[string]{},
					},
				},
			},
		}
	} else if op == "and" {
		query.QueryConf = QueryAndConf{
			QueryContextConf{
				Context: []QueryConf{
					QueryAtomConf{
						Name:      "who",
						Value:     "row0_str110",
						MatchType: FullmatchIndexConf[string]{},
					},
					QueryAtomConf{
						Name:      "whom",
						Value:     "row0_str345",
						MatchType: FullmatchIndexConf[string]{},
					},
				},
			},
		}
	} else if op == "implies" {
		query.QueryConf = QueryImplicationConf{
			Left: QueryAtomConf{
				Name:      "who",
				Value:     "row0_str110",
				MatchType: FullmatchIndexConf[string]{},
			},
			Right: QueryAtomConf{
				Name:      "whom",
				Value:     "row0_str345",
				MatchType: FullmatchIndexConf[string]{},
			},
		}
	} else {
		t.Errorf("Unknown op: %s", op)
	}

	output, err := filterCollect(rmc, query)
	if err != nil {
		t.Error(err)
	}
	return output
}

func fillPrefix(rmc *RamCollection) []RecordConf {

	out := make([]RecordConf, 3)

	var row RecordConf

	for r := 4; r < 7; r++ {
		row = RecordConf{
			Cols: make([]FielderConf, 14),
		}
		row.Cols[0] = FieldConf[string]{Value: fmt.Sprintf("OnlyStrNmbr%d", 100+r)}
		row.Cols[1] = FieldConf[[]string]{Value: []string{fmt.Sprintf("StrNmbr%d", 0+r), fmt.Sprintf("StrNmbr%d", 1000+r)}}
		row.Cols[2] = FieldConf[[]int]{Value: []int{10 + r, 100 + r}}
		row.Cols[3] = FieldConf[[]int8]{Value: []int8{int8(10 + 2*r), int8(100 + 2*r)}}
		row.Cols[4] = FieldConf[[]int16]{Value: []int16{int16(10 + 3*r), int16(100 + 3*r)}}
		row.Cols[5] = FieldConf[[]int32]{Value: []int32{int32(10 + 4*r), int32(100 + 4*r)}}
		row.Cols[6] = FieldConf[[]int64]{Value: []int64{int64(10 + 5*r), int64(100 + 5*r)}}
		row.Cols[7] = FieldConf[[]uint]{Value: []uint{uint(10 + 6*r), uint(100 + 6*r)}}
		row.Cols[8] = FieldConf[[]uint8]{Value: []uint8{uint8(10 + 7*r), uint8(100 + 7*r)}}
		row.Cols[9] = FieldConf[[]uint16]{Value: []uint16{uint16(10 + 8*r), uint16(100 + 8*r)}}
		row.Cols[10] = FieldConf[[]uint32]{Value: []uint32{uint32(10 + 9*r), uint32(100 + 9*r)}}
		row.Cols[11] = FieldConf[[]uint64]{Value: []uint64{uint64(10 + 10*r), uint64(100 + 10*r)}}
		row.Cols[12] = FieldConf[[]float32]{Value: []float32{float32(20.0 + 11*r), float32(200.0 + 11*r)}}
		row.Cols[13] = FieldConf[[]float64]{Value: []float64{float64(30 + 12*r), float64(300 + 12*r)}}
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
			Cols: make([]FielderConf, 27),
		}
		row.Cols[0] = FieldConf[string]{Value: fmt.Sprintf("str%d", r)}
		row.Cols[1] = FieldConf[int]{Value: 10 + r}
		row.Cols[2] = FieldConf[int8]{Value: int8(11 + r)}
		row.Cols[3] = FieldConf[int16]{Value: int16(12 + r)}
		row.Cols[4] = FieldConf[int32]{Value: int32(13 + r)}
		row.Cols[5] = FieldConf[int64]{Value: int64(14 + r)}
		row.Cols[6] = FieldConf[uint]{Value: uint(20 + r)}
		row.Cols[7] = FieldConf[uint8]{Value: uint8(21 + r)}
		row.Cols[8] = FieldConf[uint16]{Value: uint16(22 + r)}
		row.Cols[9] = FieldConf[uint32]{Value: uint32(23 + r)}
		row.Cols[10] = FieldConf[uint64]{Value: uint64(24 + r)}
		row.Cols[11] = FieldConf[float32]{Value: float32(30.0 + r)}
		row.Cols[12] = FieldConf[float64]{Value: float64(31.0 + r)}
		row.Cols[13] = FieldConf[time.Time]{Value: timeT.Add(time.Duration(r))}
		row.Cols[14] = FieldConf[[]string]{Value: []string{fmt.Sprintf("strA%d", r)}}
		row.Cols[15] = FieldConf[[]int]{Value: []int{10 + r}}
		row.Cols[16] = FieldConf[[]int8]{Value: []int8{int8(11 + r)}}
		row.Cols[17] = FieldConf[[]int16]{Value: []int16{int16(12 + r)}}
		row.Cols[18] = FieldConf[[]int32]{Value: []int32{int32(13 + r)}}
		row.Cols[19] = FieldConf[[]int64]{Value: []int64{int64(14 + r)}}
		row.Cols[20] = FieldConf[[]uint]{Value: []uint{uint(20 + r)}}
		row.Cols[21] = FieldConf[[]uint8]{Value: []uint8{uint8(21 + r)}}
		row.Cols[22] = FieldConf[[]uint16]{Value: []uint16{uint16(22 + r)}}
		row.Cols[23] = FieldConf[[]uint32]{Value: []uint32{uint32(23 + r)}}
		row.Cols[24] = FieldConf[[]uint64]{Value: []uint64{uint64(24 + r)}}
		row.Cols[25] = FieldConf[[]float32]{Value: []float32{float32(30.0 + r)}}
		row.Cols[26] = FieldConf[[]float64]{Value: []float64{float64(31.0 + r)}}
		out[r-4] = row
	}
	return out
}

func getOut(prefixI bool, rmc *RamCollection) ([]RecordConf, error) {
	query := FilterArgument{Limit: NO_LIMIT}
	if prefixI {
		query.QueryConf = QueryOrConf{
			QueryContextConf{
				Context: []QueryConf{
					QueryAtomConf{
						Name:      "col0",
						Value:     "OnlyStrNmbr105",
						MatchType: PrefixIndexConf[string]{},
					},
					QueryAtomConf{
						Name:      "col1",
						Value:     []string{"StrNmbr4"},
						MatchType: PrefixIndexConf[[]string]{},
					},
					QueryAtomConf{
						Name:      "col2",
						Value:     []int{15},
						MatchType: PrefixIndexConf[[]int]{},
					},
					QueryAtomConf{
						Name:      "col3",
						Value:     []int8{20},
						MatchType: PrefixIndexConf[[]int8]{},
					},
					QueryAtomConf{
						Name:      "col4",
						Value:     []int16{22},
						MatchType: PrefixIndexConf[[]int16]{},
					},
					QueryAtomConf{
						Name:      "col5",
						Value:     []int32{30},
						MatchType: PrefixIndexConf[[]int32]{},
					},
					QueryAtomConf{
						Name:      "col6",
						Value:     []int64{30},
						MatchType: PrefixIndexConf[[]int64]{},
					},
					QueryAtomConf{
						Name:      "col7",
						Value:     []uint{40},
						MatchType: PrefixIndexConf[[]uint]{},
					},
					QueryAtomConf{
						Name:      "col8",
						Value:     []uint8{38},
						MatchType: PrefixIndexConf[[]uint8]{},
					},
					QueryAtomConf{
						Name:      "col9",
						Value:     []uint16{50},
						MatchType: PrefixIndexConf[[]uint16]{},
					},
					QueryAtomConf{
						Name:      "col10",
						Value:     []uint32{46},
						MatchType: PrefixIndexConf[[]uint32]{},
					},
					QueryAtomConf{
						Name:      "col11",
						Value:     []uint64{60},
						MatchType: PrefixIndexConf[[]uint64]{},
					},
					QueryAtomConf{
						Name:      "col12",
						Value:     []float32{float32(64)},
						MatchType: PrefixIndexConf[[]float32]{},
					},
					QueryAtomConf{
						Name:      "col13",
						Value:     []float64{float64(90)},
						MatchType: PrefixIndexConf[[]float64]{},
					},
				},
			},
		}
		return filterCollect(rmc, query)
	}
	timeT := time.Time{}
	query.QueryConf = QueryOrConf{
		QueryContextConf{
			Context: []QueryConf{
				QueryAtomConf{
					Name:      "col0",
					Value:     "str4",
					MatchType: FullmatchIndexConf[string]{},
				},
				QueryAtomConf{
					Name:      "col1",
					Value:     15,
					MatchType: FullmatchIndexConf[int]{},
				},
				QueryAtomConf{
					Name:      "col2",
					Value:     int8(15),
					MatchType: FullmatchIndexConf[int8]{},
				},
				QueryAtomConf{
					Name:      "col3",
					Value:     int16(16),
					MatchType: FullmatchIndexConf[int16]{},
				},
				QueryAtomConf{
					Name:      "col4",
					Value:     int32(17),
					MatchType: FullmatchIndexConf[int32]{},
				},
				QueryAtomConf{
					Name:      "col5",
					Value:     int64(18),
					MatchType: FullmatchIndexConf[int64]{},
				},
				QueryAtomConf{
					Name:      "col6",
					Value:     uint(24),
					MatchType: FullmatchIndexConf[uint]{},
				},
				QueryAtomConf{
					Name:      "col7",
					Value:     uint8(25),
					MatchType: FullmatchIndexConf[uint8]{},
				},
				QueryAtomConf{
					Name:      "col8",
					Value:     uint16(26),
					MatchType: FullmatchIndexConf[uint16]{},
				},
				QueryAtomConf{
					Name:      "col9",
					Value:     uint32(27),
					MatchType: FullmatchIndexConf[uint32]{},
				},
				QueryAtomConf{
					Name:      "col10",
					Value:     uint64(28),
					MatchType: FullmatchIndexConf[uint64]{},
				},
				QueryAtomConf{
					Name:      "col11",
					Value:     float32(34),
					MatchType: FullmatchIndexConf[float32]{},
				},
				QueryAtomConf{
					Name:      "col12",
					Value:     float64(35),
					MatchType: FullmatchIndexConf[float64]{},
				},
				QueryAtomConf{
					Name:      "col13",
					Value:     timeT.Add(time.Duration(5)),
					MatchType: FullmatchIndexConf[time.Time]{},
				},
				QueryAtomConf{
					Name:      "col14",
					Value:     []string{"strA4"},
					MatchType: FullmatchIndexConf[[]string]{},
				},
				QueryAtomConf{
					Name:      "col15",
					Value:     []int{14},
					MatchType: FullmatchIndexConf[[]int]{},
				},
				QueryAtomConf{
					Name:      "col16",
					Value:     []int8{int8(15)},
					MatchType: FullmatchIndexConf[[]int8]{},
				},
				QueryAtomConf{
					Name:      "col17",
					Value:     []int16{int16(16)},
					MatchType: FullmatchIndexConf[[]int16]{},
				},
				QueryAtomConf{
					Name:      "col18",
					Value:     []int32{int32(17)},
					MatchType: FullmatchIndexConf[[]int32]{},
				},
				QueryAtomConf{
					Name:      "col19",
					Value:     []int64{int64(18)},
					MatchType: FullmatchIndexConf[[]int64]{},
				},
				QueryAtomConf{
					Name:      "col20",
					Value:     []uint{uint(24)},
					MatchType: FullmatchIndexConf[[]uint]{},
				},
				QueryAtomConf{
					Name:      "col21",
					Value:     []uint8{uint8(25)},
					MatchType: FullmatchIndexConf[[]uint8]{},
				},
				QueryAtomConf{
					Name:      "col22",
					Value:     []uint16{uint16(26)},
					MatchType: FullmatchIndexConf[[]uint16]{},
				},
				QueryAtomConf{
					Name:      "col23",
					Value:     []uint32{uint32(27)},
					MatchType: FullmatchIndexConf[[]uint32]{},
				},
				QueryAtomConf{
					Name:      "col24",
					Value:     []uint64{uint64(28)},
					MatchType: FullmatchIndexConf[[]uint64]{},
				},
				QueryAtomConf{
					Name:      "col25",
					Value:     []float32{float32(34)},
					MatchType: FullmatchIndexConf[[]float32]{},
				},
				QueryAtomConf{
					Name:      "col26",
					Value:     []float64{float64(35)},
					MatchType: FullmatchIndexConf[[]float64]{},
				},
			},
		},
	}
	return filterCollect(rmc, query)
}

func inspectCollection(col Collection) {
	switch tc := col.(type) {
	case *RamCollection:
		tc.Inspect()
		break
	case *SolrCollection:
		break
	default:
		panic("You should implement this testing related behaviour for your great new collection")
	}
}

func recordCount(col Collection) uint64 {
	_, count, _ := col.Filter(FilterArgument{
		Limit:     NO_LIMIT,
		QueryConf: new(QueryConf),
	})
	return count
}

// func consumeGPfxOutput(rcrds []RecordConf) error {

// 	return nil
// }
