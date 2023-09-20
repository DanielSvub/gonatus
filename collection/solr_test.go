package collection_test

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	. "github.com/SpongeData-cz/gonatus/collection"
	log "github.com/SpongeData-cz/gonatus/logging"
)

var connConf *SolrConnectionConf
var conn SolrConnection

func SetUp() error {
	file, err := os.Create("test.log")
	if err != nil {
		return err
	}
	log.SetDefaultLogger(log.NewJSONLogger(file, slog.LevelDebug))

	solrConnMap := map[string]string{}
	solrConnMap["auth-type"] = "no"
	solrConnMap["url"] = "http://localhost:8983/solr"
	connConf = NewSolrConnectionConf(solrConnMap)
	conn = NewSolrConnection(*connConf)
	if conn == nil {
		return errors.New("Cannot create connection to solr")
	}
	return nil
}

func TestSolrBasics(t *testing.T) {
	if conn == nil {
		err := SetUp()
		if err != nil {
			t.Error(err)
		}
	}

	t.Run("drop, create, drop collection", func(t *testing.T) {
		schema := SchemaConf{
			Name:         "test",
			FieldsNaming: []string{"name", "count", "time"},
			Fields: []FielderConf{
				FieldConf[string]{},
				FieldConf[int32]{},
				FieldConf[time.Time]{},
			},
			Indexes: nil,
		}

		conn.DropCollection("test")

		err := conn.CreateCollection(schema, 2)
		if err != nil {
			t.Error(err)
		}
		err = conn.DropCollection("test")
		if err != nil {
			t.Error(err)
		}

	})

	t.Run("create with already existing collection (same schema)", func(t *testing.T) {
		schema := SchemaConf{
			Name:         "test",
			FieldsNaming: []string{"name", "count", "time"},
			Fields: []FielderConf{
				FieldConf[string]{},
				FieldConf[int32]{},
				FieldConf[time.Time]{},
			},
			Indexes: nil,
		}

		err := conn.CreateCollection(schema, 2)
		if err != nil {
			t.Error("Unexpected error in test setup.", err)
		}

		err = conn.CreateCollection(schema, 2)
		if err == nil {
			t.Error("Error expected but not received")
		}
		conn.DropCollection("test")

	})

	t.Run("connect to existing collection", func(t *testing.T) {
		schema := SchemaConf{
			Name:         "test",
			FieldsNaming: []string{"name", "count", "time"},
			Fields: []FielderConf{
				FieldConf[string]{},
				FieldConf[int32]{},
				FieldConf[time.Time]{},
			},
			Indexes: nil,
		}
		conn.DropCollection("test")

		err := conn.CreateCollection(schema, 4)
		if err != nil {
			t.Error("Unexpected error in test setup.", err)
		}

		collConf := NewSolrCollectionConf(schema, *connConf, 4, 0)
		coll := NewSolrCollection(*collConf)
		if coll == nil {
			t.Error("Cannot connect to existing collection")
		}
		conn.DropCollection("test")
	})

	t.Run("implicit creation of collection in solr", func(t *testing.T) {
		schema := SchemaConf{
			Name:         "test",
			FieldsNaming: []string{"name", "count", "time"},
			Fields: []FielderConf{
				FieldConf[string]{},
				FieldConf[int32]{},
				FieldConf[time.Time]{},
			},
			Indexes: nil,
		}

		solrConnMap := map[string]string{}
		solrConnMap["auth-type"] = "no"
		solrConnMap["url"] = "http://localhost:8983/solr"
		solrConnConf := NewSolrConnectionConf(solrConnMap)
		conn := NewSolrConnection(*solrConnConf)

		conn.DropCollection("test")

		collConf := NewSolrCollectionConf(schema, *solrConnConf, 4, 0)
		coll := NewSolrCollection(*collConf)
		if coll == nil {
			t.Error("Cannot create collection implicitly")
		}
		conn.DropCollection("test")
	})

	t.Run("connect to existing collection with wrong schema", func(t *testing.T) {
		schema := SchemaConf{
			Name:         "test",
			FieldsNaming: []string{"name", "count", "time"},
			Fields: []FielderConf{
				FieldConf[string]{},
				FieldConf[int32]{},
				FieldConf[time.Time]{},
			},
			Indexes: nil,
		}

		conn.DropCollection("test")

		//create collection with orginal schema
		collConf := NewSolrCollectionConf(schema, *connConf, 4, 0)
		coll := NewSolrCollection(*collConf)
		if coll == nil {
			t.Error("Cannot create collection implicitly")
		}

		//try various kind of different schemas
		//same types different names
		schema2 := SchemaConf{
			Name:         "test",
			FieldsNaming: []string{"surname", "count", "date"},
			Fields: []FielderConf{
				FieldConf[string]{},
				FieldConf[int32]{},
				FieldConf[time.Time]{},
			},
			Indexes: nil,
		}

		collConf = NewSolrCollectionConf(schema2, *connConf, 4, 0)
		coll = NewSolrCollection(*collConf)
		if coll != nil {
			t.Error("Somehow connected to incompatible collection, or destroyed original data silently")
		}

		//same names different types
		schema2 = SchemaConf{
			Name:         "test",
			FieldsNaming: []string{"name", "count", "time"},
			Fields: []FielderConf{
				FieldConf[string]{},
				FieldConf[int64]{},
				FieldConf[time.Time]{},
			},
			Indexes: nil,
		}

		collConf = NewSolrCollectionConf(schema2, *connConf, 4, 0)
		coll = NewSolrCollection(*collConf)
		if coll != nil {
			t.Error("Somehow connected to incompatible collection, or destroyed original data silently")
		}

		//extra field
		schema2 = SchemaConf{
			Name:         "test",
			FieldsNaming: []string{"name", "count", "time", "extra"},
			Fields: []FielderConf{
				FieldConf[string]{},
				FieldConf[int64]{},
				FieldConf[time.Time]{},
				FieldConf[string]{},
			},
			Indexes: nil,
		}

		//TODO if there are more fields in solr than in the schema, it is ok (because of default and computed solr fields). Is it ok or do we want exactly the same fields in solr and in schema?

		collConf = NewSolrCollectionConf(schema2, *connConf, 4, 0)
		coll = NewSolrCollection(*collConf)
		if coll != nil {
			t.Error("Somehow connected to incompatible collection, or destroyed original data silently")
		}
		conn.DropCollection("test")
	})
}

func TestSolrIds(t *testing.T) {
	if conn == nil {
		err := SetUp()
		if err != nil {
			t.Error(err)
		}
	}
	t.Run("get last id", func(t *testing.T) {
		schema := SchemaConf{
			Name:         "test",
			FieldsNaming: []string{"name", "count", "time", "texts"},
			Fields: []FielderConf{
				FieldConf[string]{},
				FieldConf[[]int32]{},
				FieldConf[time.Time]{},
				FieldConf[[]string]{},
			},
			Indexes: nil,
		}

		conn.DropCollection("test")

		collConf := NewSolrCollectionConf(schema, *connConf, 4, 0)
		coll := NewSolrCollection(*collConf)
		if coll == nil {
			t.Error("Can not create collection.")
		}

		rec := RecordConf{
			Id: 9,
			Cols: []FielderConf{
				FieldConf[string]{
					FielderConf: nil,
					Value:       "Jméno",
				},
				FieldConf[[]int32]{
					FielderConf: nil,
					Value:       []int32{1, 2, 3},
				},
				FieldConf[time.Time]{
					FielderConf: nil,
					Value:       time.Now(),
				},
				FieldConf[[]string]{
					FielderConf: nil,
					Value:       []string{"Testovací", "data", "áýžřčšě+"},
				},
			},
		}
		_, err := coll.AddRecord(rec)

		if err != nil {
			t.Error(err)
		}

		rec = RecordConf{
			Id: 3333333,
			Cols: []FielderConf{
				FieldConf[string]{
					FielderConf: nil,
					Value:       "Jméno",
				},
				FieldConf[[]int32]{
					FielderConf: nil,
					Value:       []int32{1, 2, 3},
				},
				FieldConf[time.Time]{
					FielderConf: nil,
					Value:       time.Now(),
				},
				FieldConf[[]string]{
					FielderConf: nil,
					Value:       []string{"B"},
				},
			},
		}
		_, err = coll.AddRecord(rec)

		if err != nil {
			t.Error(err)
		}

		rec = RecordConf{
			Id: 3,
			Cols: []FielderConf{
				FieldConf[string]{
					FielderConf: nil,
					Value:       "SDSDA",
				},
				FieldConf[[]int32]{
					FielderConf: nil,
					Value:       []int32{1, 5, 6},
				},
				FieldConf[time.Time]{
					FielderConf: nil,
					Value:       time.Now().Add(time.Hour * 10),
				},
				FieldConf[[]string]{
					FielderConf: nil,
					Value:       []string{"Testovací", "data", "1234567890"},
				},
			},
		}
		_, err = coll.AddRecord(rec)

		if err != nil {
			t.Error(err)
		}
		err = coll.Commit()
		if err != nil {
			t.Error(err)
		}

		coll = NewSolrCollection(*collConf)
		if coll == nil {
			t.Error("Can not create collection.")
		}

	})
}

func TestSolrAdd(t *testing.T) {
	if conn == nil {
		err := SetUp()
		if err != nil {
			t.Error(err)
		}
	}
	t.Run("add record", func(t *testing.T) {
		schema := SchemaConf{
			Name:         "test",
			FieldsNaming: []string{"name", "count", "time", "texts"},
			Fields: []FielderConf{
				FieldConf[string]{},
				FieldConf[[]int32]{},
				FieldConf[time.Time]{},
				FieldConf[[]string]{},
			},
			Indexes: nil,
		}

		conn.DropCollection("test")
		collConf := NewSolrCollectionConf(schema, *connConf, 4, 0)
		coll := NewSolrCollection(*collConf)
		if coll == nil {
			t.Error("Can not create collection.")
		}

		rec := RecordConf{
			Id: 1,
			Cols: []FielderConf{
				FieldConf[string]{
					FielderConf: nil,
					Value:       "Jméno",
				},
				FieldConf[[]int32]{
					FielderConf: nil,
					Value:       []int32{1, 2, 3},
				},
				FieldConf[time.Time]{
					FielderConf: nil,
					Value:       time.Now(),
				},
				FieldConf[[]string]{
					FielderConf: nil,
					Value:       []string{"Testovací", "data", "áýžřčšě+"},
				},
			},
		}
		_, err := coll.AddRecord(rec)

		if err != nil {
			t.Error(err)
		}
		err = coll.Commit()
		if err != nil {
			t.Error(err)
		}
		coll.Filter(FilterArgument{})

	})

	t.Run("add record (uint limit id)", func(t *testing.T) {
		schema := SchemaConf{
			Name:         "test",
			FieldsNaming: []string{"name", "count", "time", "texts"},
			Fields: []FielderConf{
				FieldConf[string]{},
				FieldConf[[]int32]{},
				FieldConf[time.Time]{},
				FieldConf[[]string]{},
			},
			Indexes: nil,
		}

		conn.DropCollection("test")

		collConf := NewSolrCollectionConf(schema, *connConf, 4, 0)
		coll := NewSolrCollection(*collConf)
		if coll == nil {
			t.Error("Can not create collection.")
		}

		rec := RecordConf{
			Id: CId(MaxUint),
			Cols: []FielderConf{
				FieldConf[string]{
					FielderConf: nil,
					Value:       "Jméno",
				},
				FieldConf[[]int32]{
					FielderConf: nil,
					Value:       []int32{1, 2, 3},
				},
				FieldConf[time.Time]{
					FielderConf: nil,
					Value:       time.Now(),
				},
				FieldConf[[]string]{
					FielderConf: nil,
					Value:       []string{"Testovací", "data", "áýžřčšě+"},
				},
			},
		}
		cId, err := coll.AddRecord(rec)

		if err != nil {
			t.Error(err)
		}

		if cId != CId(MaxUint) {
			t.Error("Used different id than expected")
		}
		err = coll.Commit()
		if err != nil {
			t.Error(err)
		}

		found, err := coll.Filter(FilterArgument{
			QueryConf: QueryAtomConf{
				QueryConf: nil,
				MatchType: FullmatchIndexConf[string]{},
				Name:      "id",
				Value:     fmt.Sprint(cId),
			},
			Sort:      []string{},
			SortOrder: 0,
			Skip:      0,
			Limit:     0,
		})
		if err != nil {
			t.Error("unexpected error", err)
		}

		res, valid, err := found.Get()
		if err != nil {
			t.Error("unexpected error", err)
		}
		if !valid {
			t.Error("Not found ", res, " rigth after it has been added.")
		}

	})

}

func TestSolrDel(t *testing.T) {
	if conn == nil {
		err := SetUp()
		if err != nil {
			t.Error(err)
		}
	}
	t.Run("add and delete record", func(t *testing.T) {
		schema := SchemaConf{
			Name:         "test",
			FieldsNaming: []string{"name", "count", "time", "texts"},
			Fields: []FielderConf{
				FieldConf[string]{},
				FieldConf[[]int32]{},
				FieldConf[time.Time]{},
				FieldConf[[]string]{},
			},
			Indexes: nil,
		}

		conn.DropCollection("test")

		collConf := NewSolrCollectionConf(schema, *connConf, 4, 0)
		coll := NewSolrCollection(*collConf)
		if coll == nil {
			t.Error("Can not create collection.")
		}

		rec := RecordConf{
			Id: 1,
			Cols: []FielderConf{
				FieldConf[string]{
					FielderConf: nil,
					Value:       "Jméno",
				},
				FieldConf[[]int32]{
					FielderConf: nil,
					Value:       []int32{1, 2, 3},
				},
				FieldConf[time.Time]{
					FielderConf: nil,
					Value:       time.Now(),
				},
				FieldConf[[]string]{
					FielderConf: nil,
					Value:       []string{"Testovací", "data", "áýžřčšě+"},
				},
			},
		}
		_, err := coll.AddRecord(rec)

		err = coll.Commit()
		if err != nil {
			t.Error(err)
		}

		found, err := coll.Filter(FilterArgument{
			QueryConf: QueryAtomConf{
				QueryConf: nil,
				MatchType: FullmatchIndexConf[string]{},
				Name:      "id",
				Value:     fmt.Sprint(rec.Id),
			},
			Sort:      []string{},
			SortOrder: 0,
			Skip:      0,
			Limit:     0,
		})
		if err != nil {
			t.Error("unexpected error", err)
		}

		res, valid, err := found.Get()
		if err != nil {
			t.Error("unexpected error", err)
		}
		if !valid {
			t.Error("Not found ", res, " rigth after it has been added.")
		}

		err = coll.DeleteRecord(rec)
		if err != nil {
			t.Error(err)
		}

		err = coll.Commit()
		if err != nil {
			t.Error(err)
		}

		found, err = coll.Filter(FilterArgument{
			QueryConf: QueryAtomConf{
				QueryConf: nil,
				MatchType: FullmatchIndexConf[uint64]{},
				Name:      "id",
				Value:     rec.Id,
			},
			Sort:      []string{},
			SortOrder: 0,
			Skip:      0,
			Limit:     0,
		})
		if err != nil {
			t.Error("unexpected error", err)
		}
		res, valid, err = found.Get()
		if err != nil {
			t.Error("unexpected error", err)
		}
		if valid {
			t.Error("Found ", res, " which should be deleted")
		}
	})
}
