package collection

import (
	"bytes"
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
	//Query queries the given collection with the given query
	Query(collection string, query string) (io.ReadCloser, error)
	//Test connection and authorization to acces given collection
	Test(collection string) error
	//CreateCollection creates collection with the given schema
	CreateCollection(schema SchemaConf, numShards int) error
	//DropCollection deletes the given collection
	DropCollection(collection string) error
	//Commit all changes to the given collection since last commit, i.e. hard commit of collections transaction log (beware of solr transactions not being classical transactions - they are not isolated)
	Commit(collection string) error
	//RawGetRequest allows for user built get requests to solr (i.e. user can ask anything he has rights to do via get parameters)
	RawGetRequest(string) (*http.Response, error)
	//RawPostRequest allows for user built post request to solr (i.e. user can ask anything he has right to do via request body and its content type)
	RawPostRequest(urlSuffix string, contentType string, body io.Reader) (*http.Response, error)
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
	baseUrl string
}

func (ego *SimpleSolrConnection) Query(collection string, query string) (io.ReadCloser, error) {
	ego.Log().Info("Request sent", "query", query)
	fullRequest := ego.baseUrl + "/" + collection + "/query?q=" + query
	resp, err := http.Get(fullRequest)
	//TODO check response content, possibly transfomr it to error
	if err != nil {
		return nil, err
	}
	return resp.Body, err

}

func (ego *SimpleSolrConnection) Test(collection string) error {
	res, err := ego.Query(collection, "*.*")
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
	return &SolrConnectionConf{connectionData: confMap}
}

func (ego *SimpleSolrConnection) CreateCollection(schema SchemaConf, numShards int) error {
	q := fmt.Sprintf("/admin/collections?action=CREATE&name=%s&numShards=%d", schema.Name, numShards)
	resp, err := ego.RawGetRequest(q)
	//create collection
	if err != nil {
		ego.Log().Warn("Failed to create collection", "collection", schema.Name)
		return errors.NewStateError(ego, errors.LevelWarning, fmt.Sprintf("unexpected server response during collection creation (name=%s), (err=%s)", schema.Name, err))
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		ego.Log().Warn("Failed to create collection", "collection", schema.Name)
		return errors.NewStateError(ego, errors.LevelWarning, fmt.Sprintf("can not create a collection (name=%s),(httpStatus=%d), (error=%s)", schema.Name, resp.StatusCode, string(body)))
	}

	//prepare schema
	schemaJsonSB := strings.Builder{}
	schemaJsonSB.WriteRune('{')
	//TODO this is the place to tweak solr fields
	for i := 0; i < len(schema.Fields); i++ {
		fName := schema.FieldsNaming[i]
		fType, fMultiVal, err := fielderTypeToSolrType(schema.Fields[i])
		if err != nil {
			ego.Log().Info("Failed to create collection", "collection", schema.Name)
			return err
		}
		schemaJsonSB.WriteString(fmt.Sprint("\"add-field\":{\"name\":\"", fName, "\",\"type\":\"", fType, "\",\"required\":true,\"indexed\":true,\"stored\":true"))
		if fMultiVal {
			schemaJsonSB.WriteString(",\"multiValued\":true}")
		} else {
			schemaJsonSB.WriteRune('}')
		}
		if i != len(schema.Fields)-1 {
			schemaJsonSB.WriteRune(',')
		}
	}
	//id field
	schemaJsonSB.WriteString(fmt.Sprint("\"add-field\":{\"name\":\"gonatusId\",\"type\":\"plong\",\"required\":true,\"indexed\":true,\"stored\":true, \"multiValued\":false}")) //TODO this is hacky - we use plong for go uint64 - is it really safe?
	//TODO we stroe id as goantusId as solr's default schema creates solr's id of different type (string) - this may (shoul?) be set up in solr's default schema settings
	schemaJsonSB.WriteRune('}')

	//set-up schema of new collection
	resp, err = http.Post(ego.baseUrl+"/"+schema.Name+"/schema", "application/json", strings.NewReader(schemaJsonSB.String()))
	ego.Log().Info("Requeset sent", "request", ego.baseUrl+"/"+schema.Name+"/schema", "request-body", schemaJsonSB.String())
	if err != nil {
		cleanUpErr := ego.DropCollection(schema.Name)
		if cleanUpErr != nil {
			ego.Log().Warn("Failed to create collection (collection stub left in solr)", "collection", schema.Name)
			return errors.Wrap("Cannot set up schema of new collection (collection stub left in solr)", errors.TypeState, err)
		}
		ego.Log().Warn("Failed to create collection (colletion stub removed from solr)", "collection", schema.Name)
		return errors.Wrap("Cannot set up schema of new collection (collection stub removed from solr)", errors.TypeState, err)
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		cleanUpErr := ego.DropCollection(schema.Name)
		if cleanUpErr != nil {
			ego.Log().Warn("Failed to create collection (collection stub left in solr)", "collection", schema.Name)
			return errors.NewStateError(ego, errors.LevelWarning, "Cannot set up schema of new collection (collection stub left in solr)"+string(respBody))
		}
		ego.Log().Warn("Failed to create collection (colletion stub removed from solr)", "collection", schema.Name)
		return errors.NewStateError(ego, errors.LevelWarning, "Cannot set up schema of new collection (collection stub removed from solr)"+string(respBody))
	}
	ego.Log().Info("Collection created", "collection", schema.Name)
	return nil
}

func (ego *SimpleSolrConnection) DropCollection(name string) error {
	q := fmt.Sprintf("/admin/collections?action=DELETE&name=%s", name)
	resp, err := ego.RawGetRequest(q)
	if err != nil {
		return errors.NewStateError(ego, errors.LevelWarning, fmt.Sprintf("unexpected server response during collection creation (name=%s), (err=%s)", name, err))
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return errors.NewStateError(ego, errors.LevelWarning, fmt.Sprintf("can not create a collection (name=%s), (httpStatus=%d), (error=%s)", name, resp.StatusCode, string(body)))
	}
	return nil

}

func (ego *SimpleSolrConnection) Commit(collection string) error {
	resp, err := ego.RawGetRequest("/" + collection + "/update?commit=true")
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		ego.Log().Warn("Commit to collection failed", "collection", collection, "response-code", resp.StatusCode, "response-body", string(respBody))
		return errors.NewStateError(ego, errors.LevelWarning, "Unable to commit to collection")
	}
	return nil
}

func (ego *SimpleSolrConnection) RawGetRequest(reqBody string) (*http.Response, error) {
	fullRequest := ego.baseUrl + reqBody
	ego.Log().Info("Request sent", "request", reqBody)
	return http.Get(fullRequest)
}

func (ego *SimpleSolrConnection) RawPostRequest(urlSuffix string, contentType string, body io.Reader) (*http.Response, error) {
	url := ego.baseUrl + urlSuffix
	var buf bytes.Buffer
	tee := io.TeeReader(body, &buf) //TODO this is increasing complexity just to log the request body... do we need it?
	bodyCopy, _ := io.ReadAll(tee)
	ego.Log().Info("Request sent", "url", url, "content-type", contentType, "body", string(bodyCopy))
	return http.Post(url, contentType, &buf)
}

func NewSimpleSolrConnection(param SolrConnectionConf) *SimpleSolrConnection {
	baseUrl, ok := param.connectionData["url"]
	if !ok {
		slog.Default().Warn("solr server address not specified in the conf (url)")
		return nil
	}

	res := SimpleSolrConnection{
		Gobject: gonatus.Gobject{},
		baseUrl: baseUrl,
	}
	res.SetLog(res.Log().WithGroup("Solr").With("address", res.baseUrl, "auth-type", "no"))
	res.Log().Info("Simple solr connection object created.")
	return &res
}

type UserPassSolrConnection struct {
	gonatus.Gobject
	baseUrl  string
	username string
	password string
}

func (*UserPassSolrConnection) Test(collecton string) error {
	panic("unimplemented")
}

func NewUserPassSolrConnection(param SolrConnectionConf) *UserPassSolrConnection {
	baseUrl, ok := param.connectionData["url"]
	if !ok {
		slog.Default().Warn("solr server address not specified in the conf (url)")
		return nil
	}
	user, ok := param.connectionData["user"]
	if !ok {
		slog.Default().Warn("solr username not specified in the conf (core)")
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
		username: user,
		password: password,
	}
	res.SetLog(res.Log().WithGroup("Solr").With("address", res.baseUrl, "auth-type", "user-password", "user", res.username))
	res.Log().Info("User password solr connection object created.")
	return &res
}

func (ego *UserPassSolrConnection) Serialize() gonatus.Conf {
	confMap := map[string]string{}
	confMap["auth-type"] = "user-password"
	confMap["url"] = ego.baseUrl
	confMap["user"] = ego.username
	confMap["password"] = ego.password
	return &SolrConnectionConf{connectionData: confMap}
}

func (ego *UserPassSolrConnection) Query(collection string, query string) (io.ReadCloser, error) {
	ego.Log().Info("Query request sent", "query", query)
	fullRequest := ego.username + ":" + ego.password + "@" + ego.baseUrl + "/" + collection + "/query?q=" + query
	resp, err := http.Get(fullRequest)
	//TODO check response content, possibly transform it to error
	if err != nil {
		return nil, err
	}
	if resp.Request.Response.StatusCode != http.StatusOK {
		ego.Log().Warn("Query request with not ok response.", "query", query, "http-status", resp.StatusCode)
		rb, _ := io.ReadAll(resp.Body)
		return nil, errors.NewStateError(ego, errors.LevelWarning, fmt.Sprintf("Response status: %d, response body %s", resp.StatusCode, rb))
	}
	return resp.Body, err
}

func (ego *UserPassSolrConnection) CreateCollection(schema SchemaConf, numShards int) error {
	return nil //TODO
}

func (ego *UserPassSolrConnection) DropCollection(collection string) error {
	return nil //TODO
}

func (ego *UserPassSolrConnection) Commit(collection string) error {
	return nil //TODO
}

func (ego *UserPassSolrConnection) RawGetRequest(reqBody string) (*http.Response, error) {
	fullRequest := ego.username + ":" + ego.password + "@" + ego.baseUrl + reqBody
	return http.Get(fullRequest)
}

func (ego *UserPassSolrConnection) RawPostRequest(urlSuffix string, contentType string, body io.Reader) (*http.Response, error) {
	url := ego.username + ":" + ego.password + "@" + ego.baseUrl + urlSuffix
	var buf bytes.Buffer
	tee := io.TeeReader(body, &buf) //TODO this is increasing complexity just to log the request body... do we need it?
	bodyCopy, _ := io.ReadAll(tee)
	ego.Log().Info("Request sent", "url", ego.baseUrl+urlSuffix, "content-type", contentType, "body", string(bodyCopy))
	return http.Post(url, contentType, &buf)

}

//---------SOLR COLLECTION PUBLIC API

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

	schemaOK, _ := res.checkSchema()
	//collection does not exist or has incompatible schema
	if !schemaOK {
		//try to create it
		err := con.CreateCollection(conf.SchemaConf, conf.numShards)
		if err != nil {
			// probably there is another collection with same name
			logging.DefaultLogger().Warn("solr collection with the given name and schema does not exist and can not be created", "error", err, "collection", conf.Name, "schema", fmt.Sprintf("%+v", conf.FieldsNaming))
			return nil
		}
	}
	return res

}

func (ego *SolrCollection) Filter(fa FilterArgument) (stream.Producer[RecordConf], error) {
	query, err := ego.filterArgToSolrQuery(fa)
	if err != nil {
		return nil, err
	}
	query = url.QueryEscape(query)
	responseBody, err := ego.con.Query(ego.param.Name, query)
	if err != nil {
		return nil, err
	}
	resJson, err := io.ReadAll(responseBody)
	if err != nil {
		return nil, errors.NewValueError(ego, errors.LevelWarning, "cannot read the respsonse body")
	}
	resStream, err := ego.parseJsonToRecords(resJson)

	return resStream, err

}

func (ego *SolrCollection) AddRecord(conf RecordConf) (CId, error) {
	jsonAdd := strings.Builder{}
	recJson, err := ego.recordToJson(conf)
	if err != nil {
		return 0, err
	}

	//TODO generate id by hand(similarly to RAM collection)? It seems that solr does not support number id incrementation.

	//request body
	jsonAdd.WriteString("{\"add\":{\"doc\":")
	jsonAdd.WriteString(recJson)
	jsonAdd.WriteString("}}")

	resp, err := ego.con.RawPostRequest("/"+ego.param.Name+"/update", "text/json", strings.NewReader(jsonAdd.String()))
	if err != nil {
		ego.Log().Warn("Can not add record to collection", "record", fmt.Sprintf("%+v", conf), "info", "Post request error")
		return 0, errors.Wrap("Can not add record to collection", errors.TypeState, err)
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		ego.Log().Warn("Can not add record to collection", "record", fmt.Sprintf("%+v", conf), "http-status", resp.StatusCode, "http-response", string(respBody))
		return 0, errors.NewStateError(ego, errors.LevelWarning, "Can not add record to collection")
	}
	return conf.Id, nil

}

func (ego *SolrCollection) DeleteRecord(conf RecordConf) error {
	return ego.DeleteByFilter(FilterArgument{
		QueryConf: QueryAtomConf{
			QueryConf: nil,
			MatchType: FullmatchIndexConf[uint64]{},
			Name:      "gonatusId",
			Value:     conf.Id,
		},
		Sort:      []string{},
		SortOrder: 0,
		Skip:      0,
		Limit:     0,
	})
}

func (ego *SolrCollection) DeleteByFilter(fa FilterArgument) error {
	query, err := ego.filterArgToSolrQuery(fa)
	if err != nil {
		return err
	}
	deleteQuery := strings.NewReader(fmt.Sprintf("{\"delete\": { \"query\":\"%s\"}}", query))
	resp, err := ego.con.RawPostRequest("/"+ego.param.Name+"/update", "text/json", deleteQuery)
	if err != nil {
		ego.Log().Warn("Can not send request to solr while deleting by query", "collection", ego.param.Name, "error", err)
		return errors.Wrap("can not send request to solr", errors.TypeState, err)
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		ego.Log().Warn("Cannot delete by query (unexpected response)", "http-response", string(respBody), "http-status", resp.StatusCode)
		return errors.NewStateError(ego, errors.LevelWarning, fmt.Sprintf("failed to delete data from solr (responseCode=%d)", resp.StatusCode))
	}
	return nil
}

func (ego *SolrCollection) EditRecord(RecordConf, int, any) error {
	return errors.NewNotImplError(ego) //TODO
}

func (ego *SolrCollection) Commit() error {
	// Solr does not have typical transactions. It is only transaction log common to all users. Every call to Commit commits all planned work of all users at once.
	return ego.con.Commit(ego.param.Name)
}

func (ego *SolrCollection) Serialize() gonatus.Conf {
	scc, valid := ego.con.Serialize().(SolrConnectionConf)
	if !valid {
		return nil
	}
	return NewSolrCollectionConf(ego.param.SchemaConf, scc, ego.param.numShards)
}

//-------COLLECTION HELPER STUFF

// filterArgToSolrQuery generates solr query for the given FilterArgument
func (ego *SolrCollection) filterArgToSolrQuery(fa FilterArgument) (string, error) {
	query, err := ego.translateQueryConf(fa.QueryConf)
	if err != nil {
		return "", err
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

	return query, nil
}

// recordToJson transforms record to json for solr update requests
func (ego *SolrCollection) recordToJson(conf RecordConf) (string, error) {
	jsonRecord := strings.Builder{}
	jsonRecord.WriteRune('{')
	for i, field := range ego.param.SchemaConf.FieldsNaming {
		jsonRecord.WriteString("\"" + field + "\":")
		switch typ := conf.Cols[i].(type) {
		case FieldConf[int64]:
			jsonRecord.WriteString(fmt.Sprint(typ.Value))
		case FieldConf[int32]:
			jsonRecord.WriteString(fmt.Sprint(typ.Value))
		case FieldConf[bool]:
			jsonRecord.WriteString(fmt.Sprint(typ.Value))
		case FieldConf[float64]:
			jsonRecord.WriteString(fmt.Sprint(typ.Value))
		case FieldConf[float32]:
			jsonRecord.WriteString(fmt.Sprint(typ.Value))
		case FieldConf[string]:
			jsonRecord.WriteString("\"" + typ.Value + "\" ")
		case FieldConf[time.Time]:
			jsonRecord.WriteString("\"" + formatSolrTime(typ.Value) + "\"")
		case FieldConf[[]int64]:
			jsonRecord.WriteString(sliceToSolrArray(typ.Value))
		case FieldConf[[]int32]:
			jsonRecord.WriteString(sliceToSolrArray(typ.Value))
		case FieldConf[[]bool]:
			jsonRecord.WriteString(sliceToSolrArray(typ.Value))
		case FieldConf[[]float64]:
			jsonRecord.WriteString(sliceToSolrArray(typ.Value))
		case FieldConf[[]float32]:
			jsonRecord.WriteString(sliceToSolrArray(typ.Value))
		case FieldConf[[]string]:
			jsonRecord.WriteString(sliceToSolrArray(typ.Value))
		default:
			return "", errors.Wrap(fmt.Sprintf("solr collection is not set up to work with this type: %+v", typ), errors.TypeState, nil)
		}
		jsonRecord.WriteRune(',')

	}
	jsonRecord.WriteString(fmt.Sprintf("\"gonatusId\":%d", conf.Id))
	jsonRecord.WriteRune('}')
	return jsonRecord.String(), nil
}

// translateQueryConf recursively translates collection.query into main body of solr query (content of q=)
func (ego *SolrCollection) translateQueryConf(query QueryConf) (string, error) {
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
		str, isStr := any(v).(string)
		if isStr { //strings are to be quoted
			res.WriteString("\"" + str + "\"")
		} else {
			res.WriteString(fmt.Sprint(v))
		}
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
		sqs, err := ego.translateQueryConf(sq)
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

			id, valid := docMap["gonatusId"].(float64) //TODO gonatusId type hell begins here
			if !valid {
				//this should never happen. If it happens, we are surely having wrong schema in solr.
				ego.Log().Warn("Document without id (or with wrong id data type) retrieved form solr (skipped, check solr colelction schema)", "docuemnt-data", fmt.Sprintf("%+v", docMap))
				continue
			}
			res := RecordConf{Id: CId(id)} //TODO hacky - no uint64 in solr, so we take it as int64 there, read it as float64 from json  and  interpret it as uint64 (through CId) here (eh?)
			//TODO type hell ends here
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

	return returnBuffer, nil
}

func (ego *SolrCollection) checkSchema() (bool, error) {
	request := "/" + ego.param.Name + "/schema"
	resp, err := ego.con.RawGetRequest(request)
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
		return false, errors.NewStateError(ego, errors.LevelWarning, "could not parse schema check response body")
	}

	fields, valid := dataMap["schema"].(map[string]any)
	if !valid {
		ego.Log().Info("Solr schema check", "result", false)
		return false, errors.NewStateError(ego, errors.LevelWarning, "schema does not contain fields")
	}

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
			ego.Log().Info("Solr schema check", "result", false, "hint", field)
			return false, errors.NewStateError(ego, errors.LevelWarning, "invalid field description in schema check")
		}
		fieldName, valid := fieldMap["name"].(string)
		if !valid {
			ego.Log().Info("Solr schema check", "result", false, "hint", field)
			return false, errors.NewStateError(ego, errors.LevelWarning, "invalid field description in schema check")
		}
		fieldType, valid := fieldMap["type"].(string)
		if !valid {
			ego.Log().Info("Solr schema check", "result", false, "hint", field)
			return false, errors.NewStateError(ego, errors.LevelWarning, "invalid field description in schema check")
		}

		if fieldMap["multiValued"] != nil {
			fieldMultivalued, valid := fieldMap["multiValued"].(bool)
			if !valid {
				ego.Log().Info("Solr schema check", "result", false, "hint", field)
				return false, errors.NewStateError(ego, errors.LevelWarning, "invalid field description in schema check")
			}
			solrFieldMultivalued[fieldName] = fieldMultivalued
		}
		solrFieldTypeMapping[fieldName] = fieldType

	}

	//assuming it is enough that go's schema is a subset of solr's one
	for i := 0; i < len(ego.param.SchemaConf.Fields); i++ {
		expected, multivalued, err := fielderTypeToSolrType(ego.param.SchemaConf.Fields[i])
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
func fielderTypeToSolrType(fc FielderConf) (string, bool, error) {
	switch typ := fc.(type) {
	case FieldConf[int64]:
		return "plong", false, nil //TODO seems solr cloud uses these types for default clasess as IntPointFiled, LongPointFiled, etc.
	case FieldConf[int32]:
		return "pint", false, nil
	case FieldConf[bool]:
		return "boolean", false, nil
	case FieldConf[float64]:
		return "pdouble", false, nil
	case FieldConf[float32]:
		return "pfloat", false, nil
	case FieldConf[time.Time]:
		return "pdate", false, nil
	case FieldConf[string]:
		return "text_general", false, nil //solr's string (StrField) is limited to 32 KB
	case FieldConf[[]int64]:
		return "plong", true, nil
	case FieldConf[[]int32]:
		return "pint", true, nil
	case FieldConf[[]bool]:
		return "boolean", true, nil
	case FieldConf[[]float64]:
		return "pdouble", true, nil
	case FieldConf[[]float32]:
		return "pfloat", true, nil
	case FieldConf[[]string]:
		return "text_general", true, nil //solr's string (StrField) is limited to 32 KB
	default:
		return "", false, errors.Wrap(fmt.Sprintf("solr collection is not set up to work with this type: %+v", typ), errors.TypeState, nil)
	}
}
