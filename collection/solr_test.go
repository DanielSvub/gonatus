package collection_test

import (
	"fmt"
	"testing"
	"time"

	. "github.com/SpongeData-cz/gonatus/collection"
)

func TestSolr(t *testing.T) {

	schema := SchemaConf{
		Name:         "demo",
		FieldsNaming: []string{"name"},
		Fields: []FielderConf{
			FieldConf[string]{},
		},
		Indexes: nil,
	}

	solrConnMap := map[string]string{}
	solrConnMap["auth-type"] = "no"
	solrConnMap["url"] = "http://localhost:8983/solr"
	solrConnMap["core"] = "gonatus_collection"
	solrConnConf := NewSolrConnectionConf(solrConnMap)
	solrCollConf := NewSolrCollectionConf(schema, *solrConnConf)
	solrColl := NewSolrCollection(*solrCollConf)

	query := FilterArgument{
		QueryConf: QueryAtomConf{
			QueryConf: nil,
			MatchType: FullmatchIndexConf[string]{
				IndexerConf: nil,
				Name:        "*",
			},
			Name:  "*",
			Value: "*",
		},
		Sort:      []string{},
		SortOrder: 0,
		Skip:      0,
		Limit:     0,
	}

	//solrColl.Filter(query)
	//	res, err := solrColl.Filter(query)
	//fmt.Printf("res: %+v\n", res)
	//for !res.Closed() {
	//	data, valid, err := res.Get()
	//	fmt.Println("data", data, "(", valid, err, ")")
	//}
	//fmt.Printf("err: %+v\n", err)

	query = FilterArgument{Limit: NO_LIMIT}
	query.QueryConf = QueryAndConf{
		QueryContextConf{
			Context: []QueryConf{
				QueryAtomConf{
					Name:      "cat",
					Value:     "electronics",
					MatchType: FullmatchIndexConf[string]{},
				},
				QueryAtomConf{
					Name:      "cat",
					Value:     "hard",
					MatchType: PrefixIndexConf[string]{},
				},
			},
		},
	}

	//solrColl.Filter(query)

	//	res, err = solrColl.Filter(query)
	//fmt.Printf("res: %+v\n", res)
	//fmt.Printf("err: %+v\n", err)

	query = FilterArgument{Limit: NO_LIMIT}
	query.QueryConf = QueryAndConf{
		QueryContextConf{
			Context: []QueryConf{
				QueryAtomConf{
					Name:      "cat",
					Value:     "electronics",
					MatchType: FullmatchIndexConf[string]{},
				},
				QueryAtomConf{
					Name:      "cat",
					Value:     "hard",
					MatchType: PrefixIndexConf[string]{},
				},
				QueryRange[time.Time]{
					Name:   "manufacturedate_dt",
					Lower:  time.Date(2006, 02, 12, 0, 0, 0, 0, time.Now().Local().Location()), //"2005-01-01T00:00:00Z",
					Higher: time.Date(2006, 02, 16, 0, 0, 0, 0, time.Now().Local().Location()),
				},
			},
		},
	}
	//	solrColl.Filter(query)
	res, err := solrColl.Filter(query)
	fmt.Printf("res: %+v\n", res)
	for !res.Closed() {
		data, valid, err := res.Get()
		fmt.Println("data", data, "(", valid, err, ")")
	}
	fmt.Printf("err: %+v\n", err)

	//	res, err = solrColl.Filter(query)

	//fmt.Printf("res: %+v\n", res)
	//fmt.Printf("err: %+v\n", err)

	// query = FilterArgument{Limit: NO_LIMIT}
	// query.QueryConf = QueryOrConf{
	// 	QueryContextConf{
	// 		Context: []QueryConf{
	// 			QueryAtomConf{
	// 				Name:      "cat",
	// 				Value:     "electronics",
	// 				MatchType: FullmatchIndexConf[string]{},
	// 			},
	// 			QueryAtomConf{
	// 				Name:      "cat",
	// 				Value:     "hard",
	// 				MatchType: PrefixIndexConf[string]{},
	// 			},
	// 		},
	// 	},
	// }

	// res, err = solrColl.Filter(query)

	// fmt.Printf("res: %+v\n", res)
	// fmt.Printf("err: %+v\n", err)

	// query = FilterArgument{Limit: NO_LIMIT}
	// query.QueryConf =
	// 	QueryContextConf{
	// 		Context: []QueryConf{
	// 			QueryAtomConf{
	// 				Name:      "cat",
	// 				Value:     "electronics",
	// 				MatchType: FullmatchIndexConf[string]{},
	// 			},
	// 			QueryAtomConf{
	// 				Name:      "cat",
	// 				Value:     "hard",
	// 				MatchType: PrefixIndexConf[string]{},
	// 			},
	// 		},
	// 	}

	// res, err = solrColl.Filter(query)

	// fmt.Printf("res: %+v\n", res)
	// fmt.Printf("err: %+v\n", err)

}
