package collection

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/SpongeData-cz/gonatus"
	"github.com/SpongeData-cz/gonatus/errors"
	"github.com/SpongeData-cz/stream"
)

type SolrConnectionConf struct {
	connectionData map[string]string
}

func NewSolrConnectionConf(conData map[string]string) *SolrConnectionConf {
	return &SolrConnectionConf{connectionData: conData}
}

// SolrConnection is a query/request middleware to solr database.
// It mediates the user authentication and request authorization processes.
type SolrConnection interface {
	gonatus.Gobjecter
	//Authenticate authenticates user and prepares everything for requests' authorization.
	Authenticate() error
	//Request prepares and sends specified request to the server.
	//Namely it appends authorization headers (tokens, passwords, ...), if they are needed.
	Query(string) (io.ReadCloser, error)
	//Test connection and authentication
	Test() error
}

func NewSolrConnection(conf SolrConnectionConf) SolrConnection {
	switch conf.connectionData["auth-type"] {
	case "no":
		return NewSimpleSolrConnection(conf)
	case "user-password":
		return NewUserPassSolrConnection(conf)
	default:
		slog.Default().Warn("Unknown solr connection auhtentication type", "auth-type", conf.connectionData["auth-type"])
		return nil
	}
}

type SimpleSolrConnection struct {
	gonatus.Gobject
	baseUrl  string
	solrCore string
}

func (ego *SimpleSolrConnection) Authenticate() error {
	err := ego.Test()
	if err == nil {
		ego.Log().Info("Auhtenticated")
	}
	return err
}

func (ego *SimpleSolrConnection) Query(query string) (io.ReadCloser, error) {
	ego.Log().Info("Request sent", "query", query)
	fullRequest := ego.baseUrl + "/" + ego.solrCore + "/query?q=" + query
	fmt.Println(fullRequest)
	resp, err := http.Get(fullRequest)
	fmt.Printf("Query response: %+v\n", resp)
	//TODO check response content, possibly transfomr it to error
	if err != nil {
		return nil, err
	}
	return resp.Body, err

}

func (ego *SimpleSolrConnection) Test() error {
	res, err := ego.Query("")
	if err != nil {
		return errors.NewNotImplError(ego)
	}
	println(res)
	return nil
}

func (ego *SimpleSolrConnection) Serialize() gonatus.Conf {
	confMap := map[string]string{}
	confMap["auth-type"] = "no"
	confMap["url"] = ego.baseUrl
	confMap["core"] = ego.solrCore
	return &SolrConnectionConf{connectionData: confMap}
}

func NewSimpleSolrConnection(param SolrConnectionConf) *SimpleSolrConnection {
	baseUrl, ok := param.connectionData["url"]
	if !ok {
		slog.Default().Warn("solr server address not specified in the conf (url)")
		return nil
	}
	core, ok := param.connectionData["core"]
	if !ok {
		slog.Default().Warn("solr core name not specified in the conf (core)")
		return nil
	}

	res := SimpleSolrConnection{
		Gobject:  gonatus.Gobject{},
		baseUrl:  baseUrl,
		solrCore: core,
	}
	res.SetLog(res.Log().WithGroup("Solr").With("address", res.baseUrl, "core", res.solrCore, "auth-type", "no"))
	res.Log().Info("Simple solr connection object created.")
	return &res
}

type UserPassSolrConnection struct {
	gonatus.Gobject
	baseUrl  string
	solrCore string
	username string
	password string
}

// Test implements SolrConnection
func (*UserPassSolrConnection) Test() error {
	panic("unimplemented")
}

func NewUserPassSolrConnection(param SolrConnectionConf) *UserPassSolrConnection {
	baseUrl, ok := param.connectionData["url"]
	if !ok {
		slog.Default().Warn("solr server address not specified in the conf (url)")
		return nil
	}
	core, ok := param.connectionData["core"]
	if !ok {
		slog.Default().Warn("solr core name not specified in the conf (core)")
		return nil
	}
	user, ok := param.connectionData["user"]
	if !ok {
		slog.Default().Warn("solr core name not specified in the conf (core)")
		return nil
	}
	password, ok := param.connectionData["password"]
	if !ok {
		slog.Default().Warn("solr user's password not specified in the conf (password)")
		return nil
	}

	res := UserPassSolrConnection{
		Gobject:  gonatus.Gobject{},
		baseUrl:  baseUrl,
		solrCore: core,
		username: user,
		password: password,
	}
	res.SetLog(res.Log().WithGroup("Solr").With("address", res.baseUrl, "core", res.solrCore, "auth-type", "user-password", "user", res.username))
	res.Log().Info("User password solr connection object created.")
	return &res
}

func (ego *UserPassSolrConnection) Authenticate() error {
	resp, err := http.Get(ego.username + ":" + ego.password + "@" + ego.baseUrl + "/" + ego.solrCore)
	print(resp)
	//TODO check content of reponse, possibly transform it to error
	return err
}

func (ego *UserPassSolrConnection) Serialize() gonatus.Conf {
	confMap := map[string]string{}
	confMap["auth-type"] = "user-password"
	confMap["url"] = ego.baseUrl
	confMap["core"] = ego.solrCore
	confMap["user"] = ego.username
	confMap["password"] = ego.password
	return &SolrConnectionConf{connectionData: confMap}
}

func (ego *UserPassSolrConnection) Query(query string) (io.ReadCloser, error) {
	panic("NIY")
}

//---------COLLECTION API

type SolrCollectionConf struct {
	connection SolrConnectionConf
}

func NewSolrCollectionConf(solrConnectionConf SolrConnectionConf) *SolrCollectionConf {
	return &SolrCollectionConf{
		connection: solrConnectionConf,
	}
}

type SolrCollection struct {
	gonatus.Gobject
	con SolrConnection
	//solrCollection string  //TODO collection name as part of connection? i.e. do we want to manage connection per collection or share connections among collections stored in the same solr instance
}

func NewSolrCollection(conf SolrCollectionConf) *SolrCollection {
	con := NewSolrConnection(conf.connection)
	if con == nil {
		return nil
	}
	return &SolrCollection{
		Gobject: gonatus.Gobject{},
		con:     con,
	}

}

func (ego *SolrCollection) Filter(fa FilterArgument) (stream.Producer[RecordConf], error) {
	query, err := ego.translateQuery(fa.QueryConf)
	if err != nil {
		return nil, err
	}

	println("Filter call: ", fa.Skip, fa.Sort, fa.Limit, fa.SortOrder) // TODO incorporate into query

	//add other info from fa
	sortQueryPart := genSortBody(fa.Sort, fa.SortOrder)
	if len(sortQueryPart) > 0 {
		query = query + "&" + sortQueryPart
	}

	if fa.Skip != 0 {
		skipQueryPart := fmt.Sprintf("start=%d", fa.Skip)
		query = query + "&" + skipQueryPart
	}

	if fa.Limit > 0 {
		limitQueryPart := fmt.Sprintf("rows=%d", fa.Limit)
		query = query + "&" + limitQueryPart
	}

	println("Final query: ", query)

	query = url.QueryEscape(query)
	responseBody, err := ego.con.Query(query)

	res, err := io.ReadAll(responseBody)
	println("Response: ", string(res))
	return nil, errors.NewNotImplError(ego)

}

func (ego *SolrCollection) AddRecord(conf RecordConf) (CId, error) {
	return 0, errors.NewNotImplError(ego) //TODO
}

func (ego *SolrCollection) DeleteRecord(RecordConf) error {
	return errors.NewNotImplError(ego) //TODO
}

func (ego *SolrCollection) DeleteByFilter(QueryConf) error {
	return errors.NewNotImplError(ego) //TODO
}

func (ego *SolrCollection) EditRecord(RecordConf, int, any) error {
	return errors.NewNotImplError(ego) //TODO
}

func (ego *SolrCollection) Commit() error {
	return errors.NewNotImplError(ego) //TODO
}

func (ego *SolrCollection) Serialize() gonatus.Conf {
	scc, valid := ego.con.Serialize().(SolrConnectionConf)
	if !valid {
		return nil
	}
	return NewSolrCollectionConf(scc)
}

//-------COLLECTION HELPER STUFF

// translateQuery recursively translates collection.query into main body of solr query (content of q=)
func (ego *SolrCollection) translateQuery(query QueryConf) (string, error) {
	switch qt := query.(type) {
	case QueryAtomConf:
		return ego.translateAtomQuery(qt)
	case QueryContextConf:
		return ego.translateContextQuery(qt, "") //TODO what should we do with pure context query? Default operation should be mandatory if we allow pure context queries; it seems to default to OR in solr 9.3/lucene 9.7.
	case QueryAndConf:
		return ego.translateAndQuery(qt)
	case QueryOrConf:
		return ego.translateOrQuery(qt)
	case QueryNegConf:
		return ego.translateNegQuery(qt)
	case QueryImplicationConf:
		return ego.translateImplicationQuery(qt)
	case QueryRange[int]:
		var q QueryRange[any]
		q.Higher = qt.Higher
		q.Lower = qt.Lower
		q.Name = qt.Name
		return ego.translateRangeQuery(q)
	case QueryRange[int8]:
		var q QueryRange[any]
		q.Higher = qt.Higher
		q.Lower = qt.Lower
		q.Name = qt.Name
		return ego.translateRangeQuery(q)
	case QueryRange[int16]:
		var q QueryRange[any]
		q.Higher = qt.Higher
		q.Lower = qt.Lower
		q.Name = qt.Name
		return ego.translateRangeQuery(q)
	case QueryRange[int32]:
		var q QueryRange[any]
		q.Higher = qt.Higher
		q.Lower = qt.Lower
		q.Name = qt.Name
		return ego.translateRangeQuery(q)
	case QueryRange[int64]:
		var q QueryRange[any]
		q.Higher = qt.Higher
		q.Lower = qt.Lower
		q.Name = qt.Name
		return ego.translateRangeQuery(q)
	case QueryRange[uint]:
		var q QueryRange[any]
		q.Higher = qt.Higher
		q.Lower = qt.Lower
		q.Name = qt.Name
		return ego.translateRangeQuery(q)
	case QueryRange[uint8]:
		var q QueryRange[any]
		q.Higher = qt.Higher
		q.Lower = qt.Lower
		q.Name = qt.Name
		return ego.translateRangeQuery(q)
	case QueryRange[uint16]:
		var q QueryRange[any]
		q.Higher = qt.Higher
		q.Lower = qt.Lower
		q.Name = qt.Name
		return ego.translateRangeQuery(q)
	case QueryRange[uint32]:
		var q QueryRange[any]
		q.Higher = qt.Higher
		q.Lower = qt.Lower
		q.Name = qt.Name
		return ego.translateRangeQuery(q)
	case QueryRange[uint64]:
		var q QueryRange[any]
		q.Higher = qt.Higher
		q.Lower = qt.Lower
		q.Name = qt.Name
		return ego.translateRangeQuery(q)
	case QueryRange[float32]:
		var q QueryRange[any]
		q.Higher = qt.Higher
		q.Lower = qt.Lower
		q.Name = qt.Name
		return ego.translateRangeQuery(q)
	case QueryRange[float64]:
		var q QueryRange[any]
		q.Higher = qt.Higher
		q.Lower = qt.Lower
		q.Name = qt.Name
		return ego.translateRangeQuery(q)
	case QueryRange[string]:
		var q QueryRange[any]
		q.Higher = qt.Higher
		q.Lower = qt.Lower
		q.Name = qt.Name
		return ego.translateRangeQuery(q)
	case QueryRange[time.Time]:
		var q QueryRange[any]
		q.Higher = formatSolrTime(qt.Higher)
		q.Lower = formatSolrTime(qt.Lower)
		q.Name = qt.Name
		return ego.translateRangeQuery(q)
	case QuerySpatialConf:
		fmt.Printf("Spatial %+v", qt)
		return "", errors.NewNotImplError(ego)
	default:
		return "", errors.NewMisappError(ego, fmt.Sprint("unknown query type ", qt))
	}
}

func (ego *SolrCollection) translateAtomQuery(query QueryAtomConf) (string, error) {

	switch typ := query.MatchType.(type) {
	case FullmatchIndexConf[int], FullmatchIndexConf[int8],
		FullmatchIndexConf[int16], FullmatchIndexConf[int32],
		FullmatchIndexConf[int64], FullmatchIndexConf[uint],
		FullmatchIndexConf[uint8], FullmatchIndexConf[uint16],
		FullmatchIndexConf[uint32], FullmatchIndexConf[uint64],
		FullmatchIndexConf[float32], FullmatchIndexConf[float64]:
		return fmt.Sprint(query.Name, ":", query.Value), nil
	case FullmatchIndexConf[[]int], FullmatchIndexConf[[]int8],
		FullmatchIndexConf[[]int16], FullmatchIndexConf[[]int32],
		FullmatchIndexConf[[]int64], FullmatchIndexConf[[]uint],
		FullmatchIndexConf[[]uint8], FullmatchIndexConf[[]uint16],
		FullmatchIndexConf[[]uint32], FullmatchIndexConf[[]uint64],
		FullmatchIndexConf[[]float32], FullmatchIndexConf[[]float64],
		FullmatchIndexConf[[]string]:
		return "", errors.NewNotImplError(ego) //TODO slices... we can convert it into context queries, but what operation should be used?
	case FullmatchIndexConf[time.Time]:
		t, v := query.Value.(time.Time)
		if !v {
			return "", errors.NewValueError(ego, errors.LevelWarning, fmt.Sprint("Expected time value, got", query.Value))
		}
		return fmt.Sprint(query.Name, ":", formatSolrTime(t)), nil
	case FullmatchIndexConf[string]:
		return fmt.Sprint(query.Name, ":", query.Value), nil //keeping it case sensitive here, this is user's resposnisibility
	case PrefixIndexConf[string]:
		return fmt.Sprint(query.Name, ":", query.Value, "*"), nil
	case PrefixIndexConf[int], PrefixIndexConf[int8],
		PrefixIndexConf[int16], PrefixIndexConf[int32],
		PrefixIndexConf[int64], PrefixIndexConf[uint],
		PrefixIndexConf[uint8], PrefixIndexConf[uint16],
		PrefixIndexConf[uint32], PrefixIndexConf[uint64],
		PrefixIndexConf[float32], PrefixIndexConf[float64]:
		return "", errors.NewMisappError(ego, fmt.Sprint("it is not clear how to interpret number", query.Value, " as prefixf)")) //TODO it does not make sense to use numbers as prefixes (or w have to specify the meaning of such prefix)
	case PrefixIndexConf[time.Time]:
		return "", errors.NewNotImplError(ego) //TODO prefixes of time make sense, but it is not that straightforward for solr. Fallback(?): We can overcome it by ranges.
	case PrefixIndexConf[[]int], PrefixIndexConf[[]int8],
		PrefixIndexConf[[]int16], PrefixIndexConf[[]int32],
		PrefixIndexConf[[]int64], PrefixIndexConf[[]uint],
		PrefixIndexConf[[]uint8], PrefixIndexConf[[]uint16],
		PrefixIndexConf[[]uint32], PrefixIndexConf[[]uint64],
		PrefixIndexConf[[]float32], PrefixIndexConf[[]float64],
		PrefixIndexConf[[]string]:
		return "", errors.NewNotImplError(ego) //TODO arrays' prefix also has no clear meaning (there is no order in multiValued fields in solr)

	default:
		return "", errors.NewMisappError(ego, "unknown indexer type: "+fmt.Sprint(typ))
	}

}

func (ego *SolrCollection) translateNegQuery(query QueryNegConf) (string, error) {
	qAtom := query.QueryAtomConf
	qAtomTranslated, err := ego.translateAtomQuery(qAtom)
	if err != nil {
		return qAtomTranslated, err
	}
	return fmt.Sprint("NOT(", qAtomTranslated, ")"), nil //solr is case sensitive in case of operations
}

func (ego *SolrCollection) translateAndQuery(query QueryAndConf) (string, error) {
	return ego.translateContextQuery(query.QueryContextConf, "AND") //solr is case sensitive in case of operations
}

func (ego *SolrCollection) translateOrQuery(query QueryOrConf) (string, error) {
	return ego.translateContextQuery(query.QueryContextConf, "OR") //solr is case sensitive in case of operations
}

func (ego *SolrCollection) translateContextQuery(query QueryContextConf, operation string) (string, error) {
	subqueries := []string{}
	for _, sq := range query.Context {
		sqs, err := ego.translateQuery(sq)
		if err != nil {
			return "", err
		}
		subqueries = append(subqueries, sqs)
	}
	if len(subqueries) == 0 {
		return "", nil
	}
	sb := strings.Builder{}
	sb.WriteString("(")
	sb.WriteString(subqueries[0])
	for _, sq := range subqueries[1:] {
		sb.WriteString(fmt.Sprint(" ", operation, " "))
		sb.WriteString(sq)
	}
	sb.WriteString(")")
	return sb.String(), nil

}

func (ego *SolrCollection) translateImplicationQuery(query QueryImplicationConf) (string, error) {
	lATrans, err := ego.translateAtomQuery(query.Left)
	if err != nil {
		return "", err
	}
	rATrans, err := ego.translateAtomQuery(query.Right)
	if err != nil {
		return "", err
	}
	return fmt.Sprint("(NOT(", lATrans, ") OR ", rATrans, ")"), nil //basic logic stuff: a implies b is equal to not(a) or b
}

func (ego *SolrCollection) translateRangeQuery(query QueryRange[any]) (string, error) {
	l := query.Lower
	h := query.Higher
	name := query.Name
	println("Range query", name, l, h)
	return fmt.Sprint("(", name, ":[", l, " TO ", h, "])"), nil
}

// genSortBody generates sorting part of solr query, if it is needed.
// It returns empty string if sorts is empty.
func genSortBody(sorts []string, order int) string {
	orderString := "ASC"
	if order > 0 {
		orderString = "DESC"
	}
	resSB := strings.Builder{}
	for i, sort := range sorts {
		resSB.WriteString(sort)
		resSB.WriteString(" ")
		resSB.WriteString(orderString)
		if i < len(sorts)-1 {
			resSB.WriteString(", ")
		}
	}
	return resSB.String()
}

// formatSolrTime formats time.Time values into format demanded by TrieDateField, DatePointField, DateRangeField and DatePointFieldsolr field-type in solr
func formatSolrTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339) //assuming Field-Type of  TrieDateField, DatePointField, DateRangeField or DatePointFieldsolr, solr needs EXACTLY this time format and  UTC zone, i.e. change zone and then format it (see solr's docs)
}
