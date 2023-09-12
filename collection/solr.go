package collection

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/SpongeData-cz/gonatus"
	"github.com/SpongeData-cz/gonatus/errors"
	"github.com/SpongeData-cz/gonatus/logging"
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
	//Create core with the given schema and name given by the connection settings
	CreateCore(SchemaConf) error
	//Drop core we are connected to
	DropCore() error
	//Commit all changes since last commit, i.e. hard commit (beware of solr transactions not being classical transactions - they are not isolated)
	Commit() error
	//rawRequest allows for user built get requests (i.e. user specifies the aprt of url after core)
	rawRequest(string) (*http.Response, error)
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
	solrCore string //TODO maybe better to have it in collection? (we already have it there if it is the same as the name in the schema)
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

func (ego *SimpleSolrConnection) CreateCore(schema SchemaConf) error {
	q := "/admin/collections?action=CREATE&name=testing_collection&numShards=2"
	resp, err := ego.adminRequest(q)
	body, _ := io.ReadAll(resp.Body)
	println(string(body))
	return err
}

func (ego *SimpleSolrConnection) DropCore() error {
	return nil
}

func (ego *SimpleSolrConnection) Commit() error {
	return nil
}

func (ego *SimpleSolrConnection) Rollback() error {
	return nil
}

func (ego *SimpleSolrConnection) adminRequest(reqBody string) (*http.Response, error) {
	fullRequest := ego.baseUrl + reqBody
	fmt.Println("\n\n", fullRequest)
	return http.Get(fullRequest)
}

func (ego *SimpleSolrConnection) rawRequest(reqBody string) (*http.Response, error) {
	fullRequest := ego.baseUrl + "/" + ego.solrCore + reqBody
	fmt.Println(fullRequest)
	return http.Get(fullRequest)
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
	ego.Log().Info("Request sent", "query", query)
	fullRequest := ego.username + ":" + ego.password + "@" + ego.baseUrl + "/" + ego.solrCore + "/query?q=" + query
	//fmt.Println(fullRequest)
	resp, err := http.Get(fullRequest)
	//fmt.Printf("Query response: %+v\n", resp)
	//TODO check response content, possibly transform it to error
	if err != nil {
		return nil, err
	}
	return resp.Body, err
}

func (ego *UserPassSolrConnection) SatisfiesSchema(schema SchemaConf) (bool, error) {
	return false, nil
}

func (ego *UserPassSolrConnection) CreateCore(schema SchemaConf) error {
	return nil
}

func (ego *UserPassSolrConnection) DropCore() error {
	return nil
}

func (ego *UserPassSolrConnection) Commit() error {
	return nil
}

func (ego *UserPassSolrConnection) rawRequest(reqBody string) (*http.Response, error) {
	fullRequest := ego.username + ":" + ego.password + "@" + ego.baseUrl + "/" + ego.solrCore + reqBody
	fmt.Println(fullRequest)
	return http.Get(fullRequest)
}

//---------COLLECTION API

type SolrCollectionConf struct {
	SchemaConf
	connection SolrConnectionConf
	numShards  int //how many shards should be used if collection is to be created

}

func NewSolrCollectionConf(schema SchemaConf, solrConnectionConf SolrConnectionConf, numShards int) *SolrCollectionConf {
	return &SolrCollectionConf{
		SchemaConf: schema,
		connection: solrConnectionConf,
		numShards:  numShards,
	}
}

type SolrCollection struct {
	gonatus.Gobject
	param SolrCollectionConf
	con   SolrConnection
}

func NewSolrCollection(conf SolrCollectionConf) *SolrCollection {
	con := NewSolrConnection(conf.connection)
	if con == nil {
		return nil
	}

	res := &SolrCollection{
		Gobject: gonatus.Gobject{},
		con:     con,
		param:   conf,
	}

	schemaOK, err := res.checkSchema()
	if !schemaOK {
		fmt.Printf("schema check err: %v\n", err) //TODO
		err := con.CreateCore(conf.SchemaConf)
		if err != nil {
			logging.DefaultLogger().Warn("solr collection with the given schema does not exist and can not be created", "error", err)
			return nil
		}
	}
	return res

}

func (ego *SolrCollection) Filter(fa FilterArgument) (stream.Producer[RecordConf], error) {
	query, err := ego.translateQuery(fa.QueryConf)
	if err != nil {
		return nil, err
	}

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

	query = url.QueryEscape(query)
	responseBody, err := ego.con.Query(query)
	if err != nil {
		return nil, err
	}
	resJson, err := io.ReadAll(responseBody)
	if err != nil {
		return nil, errors.NewValueError(ego, errors.LevelWarning, "cannot read the respsonse body")
	}
	resStream, err := ego.parseJsonToRecords(resJson)

	println("Response: ", string(resJson)) //TODO
	return resStream, err

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

// Solr does not have typical transactions. It is only transaction log common to all users. Every call to Commit commits all planned work of all users at once.
func (ego *SolrCollection) Commit() error {
	return errors.NewNotImplError(ego)
}

func (ego *SolrCollection) Serialize() gonatus.Conf {
	scc, valid := ego.con.Serialize().(SolrConnectionConf)
	if !valid {
		return nil
	}
	return NewSolrCollectionConf(ego.param.SchemaConf, scc)
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

// sliceToSolrArray converts slice to string format for solr queries
func sliceToSolrArray[T any](sl []T) string {
	res := strings.Builder{}
	res.WriteRune('[')
	for i, v := range sl {
		res.WriteString(fmt.Sprint(v))
		if i < len(sl)-1 {
			res.WriteString(", ")
		}
	}
	res.WriteRune(']')
	return res.String()
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
		//TODO seems there is undocummented(?) fixed order of multivalued fields in solr if they are initialized by array literal (e.g. [1,2,3])
	case FullmatchIndexConf[[]int]:
		t, v := query.Value.([]int)
		if !v {
			return "", errors.NewValueError(ego, errors.LevelWarning, fmt.Sprint("Expected slice value, got", query.Value))
		}
		return fmt.Sprint(query.Name, ":", sliceToSolrArray(t)), nil
	case FullmatchIndexConf[[]int8]:
		t, v := query.Value.([]int8)
		if !v {
			return "", errors.NewValueError(ego, errors.LevelWarning, fmt.Sprint("Expected slice value, got", query.Value))
		}
		return fmt.Sprint(query.Name, ":", sliceToSolrArray(t)), nil
	case FullmatchIndexConf[[]int16]:
		t, v := query.Value.([]int16)
		if !v {
			return "", errors.NewValueError(ego, errors.LevelWarning, fmt.Sprint("Expected slice value, got", query.Value))
		}
		return fmt.Sprint(query.Name, ":", sliceToSolrArray(t)), nil
	case FullmatchIndexConf[[]int32]:
		t, v := query.Value.([]int32)
		if !v {
			return "", errors.NewValueError(ego, errors.LevelWarning, fmt.Sprint("Expected slice value, got", query.Value))
		}
		return fmt.Sprint(query.Name, ":", sliceToSolrArray(t)), nil
	case FullmatchIndexConf[[]int64]:
		t, v := query.Value.([]int64)
		if !v {
			return "", errors.NewValueError(ego, errors.LevelWarning, fmt.Sprint("Expected slice value, got", query.Value))
		}
		return fmt.Sprint(query.Name, ":", sliceToSolrArray(t)), nil
	case FullmatchIndexConf[[]uint]:
		t, v := query.Value.([]uint)
		if !v {
			return "", errors.NewValueError(ego, errors.LevelWarning, fmt.Sprint("Expected slice value, got", query.Value))
		}
		return fmt.Sprint(query.Name, ":", sliceToSolrArray(t)), nil
	case FullmatchIndexConf[[]uint8]:
		t, v := query.Value.([]uint8)
		if !v {
			return "", errors.NewValueError(ego, errors.LevelWarning, fmt.Sprint("Expected slice value, got", query.Value))
		}
		return fmt.Sprint(query.Name, ":", sliceToSolrArray(t)), nil
	case FullmatchIndexConf[[]uint16]:
		t, v := query.Value.([]uint16)
		if !v {
			return "", errors.NewValueError(ego, errors.LevelWarning, fmt.Sprint("Expected slice value, got", query.Value))
		}
		return fmt.Sprint(query.Name, ":", sliceToSolrArray(t)), nil
	case FullmatchIndexConf[[]uint32]:
		t, v := query.Value.([]uint32)
		if !v {
			return "", errors.NewValueError(ego, errors.LevelWarning, fmt.Sprint("Expected slice value, got", query.Value))
		}
		return fmt.Sprint(query.Name, ":", sliceToSolrArray(t)), nil
	case FullmatchIndexConf[[]uint64]:
		t, v := query.Value.([]uint64)
		if !v {
			return "", errors.NewValueError(ego, errors.LevelWarning, fmt.Sprint("Expected slice value, got", query.Value))
		}
		return fmt.Sprint(query.Name, ":", sliceToSolrArray(t)), nil
	case FullmatchIndexConf[[]float32]:
		t, v := query.Value.([]float32)
		if !v {
			return "", errors.NewValueError(ego, errors.LevelWarning, fmt.Sprint("Expected slice value, got", query.Value))
		}
		return fmt.Sprint(query.Name, ":", sliceToSolrArray(t)), nil
	case FullmatchIndexConf[[]float64]:
		t, v := query.Value.([]float64)
		if !v {
			return "", errors.NewValueError(ego, errors.LevelWarning, fmt.Sprint("Expected slice value, got", query.Value))
		}
		return fmt.Sprint(query.Name, ":", sliceToSolrArray(t)), nil
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
		return "", errors.NewNotImplError(ego) //TODO prefix of time makes sense, but we need to specify what exactly is ment by time prefix. Also it is not that straightforward for solr. Fallback: Can we overcome it by ranges?
	case PrefixIndexConf[[]int], PrefixIndexConf[[]int8],
		PrefixIndexConf[[]int16], PrefixIndexConf[[]int32],
		PrefixIndexConf[[]int64], PrefixIndexConf[[]uint],
		PrefixIndexConf[[]uint8], PrefixIndexConf[[]uint16],
		PrefixIndexConf[[]uint32], PrefixIndexConf[[]uint64],
		PrefixIndexConf[[]float32], PrefixIndexConf[[]float64],
		PrefixIndexConf[[]string]:
		//TODO seems there is undocummented(?) fixed order of multivalued fields in solr if they are initialized by array literal (e.g. [1,2,3])
		return "", errors.NewNotImplError(ego) //TODO arrays' prefix? is solr able to prefix multivalued field?

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

// parseJsonToRecords parses solr query response into stream of RecordConfs
func (ego *SolrCollection) parseJsonToRecords(jsonData []byte) (stream.Producer[RecordConf], error) {
	returnBuffer := stream.NewChanneledInput[RecordConf](100)
	//fmt.Printf("ego.param: %+v\n", ego.param)
	resMap := map[string]any{}
	if err := json.Unmarshal(jsonData, &resMap); err != nil {
		return nil, err
	}

	responseData, valid := resMap["response"].(map[string]any)
	if !valid {
		return nil, errors.NewStateError(ego, errors.LevelWarning, "unknown response format while parsing solr collection response")
	}

	responseDocuments, valid := responseData["docs"].([]any)
	if !valid {
		return nil, errors.NewStateError(ego, errors.LevelWarning, "unknown docs format while parsing solr collection response")
	}

	fetchData := func() {
		for _, doc := range responseDocuments {
			docMap, valid := doc.(map[string]any)
			if !valid {
				ego.Log().Warn("Invalid document read (skipped)", "document-data", fmt.Sprintf("%+v", docMap))
				continue
			}
			fmt.Printf("\nok doc: %v\n", docMap)

			id, valid := docMap["id"].(CId)
			if !valid {
				//this should never happen. If it happens, we are surely having wrong schema in solr.
				ego.Log().Warn("Document without id (or with wrong id data type) retrieved form solr (skipped, check solr colelction schema)", "docuemnt-data", fmt.Sprintf("%+v", docMap))
				continue
			}
			res := RecordConf{Id: id}
			res.Cols = make([]FielderConf, len(ego.param.Fields))
			invalidColData := false
			for i := 0; i < len(res.Cols); i++ {
				colValue, valid := docMap[ego.param.FieldsNaming[i]].(FielderConf)
				if !valid {
					ego.Log().Warn("document with wrong data type in column (skipped)", "document-data", fmt.Sprintf("%+v", docMap))
					invalidColData = true
					break
				}
				res.Cols[i] = colValue
			}
			if invalidColData {
				continue
			}
			returnBuffer.Write(res)
		}
		returnBuffer.Close()
	}
	go fetchData()

	return returnBuffer, errors.NewNotImplError(ego)
}

func (ego *SolrCollection) checkSchema() (bool, error) {
	request := "/schema"
	resp, err := ego.con.rawRequest(request)
	if err != nil {
		ego.Log().Info("Solr schema check", "result", false)
		return false, err
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		ego.Log().Info("Solr schema check", "result", false)
		return false, errors.NewStateError(ego, errors.LevelWarning, "could not read schema check response")
	}
	dataMap := map[string]any{}
	if err = json.Unmarshal(data, &dataMap); err != nil {
		ego.Log().Info("Solr schema check", "result", false)
		return false, errors.NewStateError(ego, errors.LevelWarning, "could not parse schema check response bofy")
	}

	fields, valid := dataMap["schema"].(map[string]any)
	if !valid {
		ego.Log().Info("Solr schema check", "result", false)
		return false, errors.NewStateError(ego, errors.LevelWarning, "schema does not contain fields")
	}

	fmt.Printf("fields[\"fields\"]: %v\n", fields["fields"])
	fieldsMap, valid := fields["fields"].([]any)
	if !valid {
		ego.Log().Info("Solr schema check", "result", false)
		return false, errors.NewStateError(ego, errors.LevelWarning, "fields are of invalid type in schema check")
	}
	solrFieldTypeMapping := map[string]string{}
	solrFieldMultivalued := map[string]bool{}
	for _, field := range fieldsMap {
		fieldMap, valid := field.(map[string]any)
		if !valid {
			ego.Log().Info("Solr schema check", "result", false)
			return false, errors.NewStateError(ego, errors.LevelWarning, "invalid field description in schema check")
		}
		fieldName, valid := fieldMap["name"].(string)
		if !valid {
			ego.Log().Info("Solr schema check", "result", false)
			return false, errors.NewStateError(ego, errors.LevelWarning, "invalid field description in schema check")
		}
		fieldType, valid := fieldMap["type"].(string)
		if !valid {
			ego.Log().Info("Solr schema check", "result", false)
			return false, errors.NewStateError(ego, errors.LevelWarning, "invalid field description in schema check")
		}
		fieldMultivalued, valid := fieldMap["multivalued"].(bool)
		if !valid {
			ego.Log().Info("Solr schema check", "result", false)
			return false, errors.NewStateError(ego, errors.LevelWarning, "invalid field description in schema check")
		}
		solrFieldTypeMapping[fieldName] = fieldType
		solrFieldMultivalued[fieldName] = fieldMultivalued
	}

	//assuming it is enough that go's schema is a subset of solr's one
	for i := 0; i < len(ego.param.SchemaConf.Fields); i++ {
		fmt.Printf("\n\n\n %+v\n, %+v", solrFieldTypeMapping[ego.param.SchemaConf.FieldsNaming[i]], ego.param.SchemaConf.Fields[i])
		expected, multivalued, err := ego.fielderTypeToSolrType(ego.param.SchemaConf.Fields[i])
		if err != nil {
			ego.Log().Info("Solr schema check", "result", false)
			return false, err
		}
		if expected != solrFieldTypeMapping[ego.param.SchemaConf.FieldsNaming[i]] || multivalued != solrFieldMultivalued[ego.param.SchemaConf.FieldsNaming[i]] {
			ego.Log().Info("Solr schema check", "result", false)
			return false, errors.NewStateError(ego, errors.LevelWarning,
				fmt.Sprint("incomptaible types in schema and solr for field ", ego.param.SchemaConf.FieldsNaming[i], ":", solrFieldTypeMapping[ego.param.SchemaConf.FieldsNaming[i]], " and ", expected, "(multivalued=", multivalued, ")"))
		}
	}

	ego.Log().Info("Solr schema check", "result", true)
	return true, nil
}

// fielderTypeToSolrType returns name of solr type together with mutlivalued flag (true = field is multivalued/slice) or error
func (ego *SolrCollection) fielderTypeToSolrType(fc FielderConf) (string, bool, error) {
	switch typ := fc.(type) {
	case FieldConf[int64]:
		return "LongPointField", false, nil
	case FieldConf[int32]:
		return "IntPointField", false, nil
	case FieldConf[bool]:
		return "BoolField", false, nil
	case FieldConf[float64]:
		return "DoublePointField", false, nil
	case FieldConf[float32]:
		return "FloatPointField", false, nil
	case FieldConf[time.Time]:
		return "DatePointField", false, nil
	case FieldConf[string]:
		return "TextField", false, nil //solr's StrField is limited to 32 KB
	case FieldConf[[]int64]:
		return "LongPointField", true, nil
	case FieldConf[[]int32]:
		return "IntPointField", true, nil
	case FieldConf[[]bool]:
		return "BoolField", true, nil
	case FieldConf[[]float64]:
		return "DoublePointField", true, nil
	case FieldConf[[]float32]:
		return "FloatPointField", true, nil
	case FieldConf[[]string]:
		return "TextField", true, nil //solr's StrField is limited to 32 KB
	default:
		return "", false, errors.NewMisappError(ego, fmt.Sprintf("solr collection is not set up to work with this type: %+v", typ))
	}
}
