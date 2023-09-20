package collection

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/SpongeData-cz/gonatus"
	"github.com/SpongeData-cz/gonatus/errors"
	"github.com/SpongeData-cz/stream"
)

//-----------------------------------------------STRUCTS AND CONSTRUCTORS

// SolrCollectionConf contains configuration information for solr collection
type SolrCollectionConf struct {
	SchemaConf                    // collections schema information
	connection SolrConnectionConf //connection information
	numShards  int                //how many shards should be used if collection is to be created
	nextId     CId                //ids for newly added records will be higher than this value
}

func NewSolrCollectionConf(schema SchemaConf, solrConnectionConf SolrConnectionConf, numShards int, nextId CId) *SolrCollectionConf {
	return &SolrCollectionConf{
		SchemaConf: schema,
		connection: solrConnectionConf,
		numShards:  numShards,
		nextId:     nextId,
	}
}

type SolrCollection struct {
	gonatus.Gobject
	param           SolrCollectionConf //configuration information
	con             SolrConnection     //conection to send requests through to solr
	nextId          CId                //id for next added record
	transactionPlan []solrOperation    //operations planned for commit
	idLock          sync.Mutex         //lock for generating new ids for records (access to nextId)
	transactionLock sync.Mutex         //lock for planing or commiting operations (access to transactionPlan)
}

func NewSolrCollection(conf SolrCollectionConf) *SolrCollection {
	con := NewSolrConnection(conf.connection)
	if con == nil {
		return nil
	}

	res := &SolrCollection{
		Gobject:         gonatus.Gobject{},
		con:             con,
		param:           conf,
		nextId:          0,
		transactionPlan: []solrOperation{},
		idLock:          sync.Mutex{},
		transactionLock: sync.Mutex{},
	}

	schemaOK, _ := res.checkSchema()

	//collection does not exist or has incompatible schema
	if !schemaOK {
		//try to create it
		err := con.CreateCollection(conf.SchemaConf, conf.numShards)
		if err != nil {
			// probably there is another collection with same name
			res.Log().Warn("solr collection with the given name and schema does not exist in solr and can not be created", "error", err, "collection", conf.Name, "schema", fmt.Sprintf("%+v", conf.FieldsNaming))
			return nil
		}
		res.nextId = max(1, conf.nextId)
	} else {
		lastId, err := res.queryLastId()
		if err != nil {
			res.Log().Warn("could not read used ids from the solr collection")
			return nil
		}
		res.nextId = max(lastId+1, conf.nextId)
	}
	res.SetLog(res.Log().With("collection", conf.Name))
	res.Log().Info("Collection connected to solr")
	return res

}

//-----------------------------------------------PUBLIC API

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
		return nil, errors.NewValueError(ego, errors.LevelWarning, "cannot read the respOnse body")
	}
	resStream, err := ego.parseJsonToRecords(resJson)

	return resStream, err

}

func (ego *SolrCollection) AddRecord(conf RecordConf) (CId, error) {
	ego.idLock.Lock()
	if conf.Id > ego.nextId {
		ego.nextId = conf.Id + 1
		ego.idLock.Unlock()
	} else {
		ego.idLock.Unlock()
		newId, err := ego.newCid()
		if err != nil {
			return 0, errors.Wrap("Can not add new record into collection (problem with id)", errors.TypeState, err)
		}
		conf.Id = newId
	}

	addOp := &solrAddOp{
		record:     conf,
		collection: ego,
	}
	ego.planOperation(addOp)

	return conf.Id, nil
}

func (ego *SolrCollection) DeleteRecord(conf RecordConf) error {
	delOp := &solrDeleteByIdOp{
		record:     conf,
		collection: ego,
	}
	ego.planOperation(delOp)
	return nil
}

func (ego *SolrCollection) DeleteByFilter(fa FilterArgument) error {
	delOp := &solrDeleteByQueryOp{
		collection: ego,
		filterArg:  fa,
	}
	ego.planOperation(delOp)
	return nil
}

func (ego *SolrCollection) EditRecord(conf RecordConf) error {
	updateOp := &solrUpdateOp{
		collection: ego,
		record:     conf,
	}
	ego.planOperation(updateOp)
	return nil
}

func (ego *SolrCollection) Commit() error {
	// Solr does not have typical transactions. It is only transaction log common to all users. Every call to Commit commits all planned work of all users at once.

	ego.transactionLock.Lock()
	defer ego.transactionLock.Unlock()
	querySB := strings.Builder{}
	querySB.WriteRune('{')
	for _, op := range ego.transactionPlan {
		opJson, err := op.toJson()
		if err != nil {
			return err
		}
		querySB.WriteString(opJson)
	}
	querySB.WriteRune('}')
	query := strings.NewReader(querySB.String())

	resp, err := ego.con.RawPostRequest("/"+ego.param.Name+"/update", "text/json", query)
	if err != nil {
		//ego.Log().Warn("Can not send request to solr", "collection", ego.param.Name, "error", err)
		return errors.Wrap("can not send request to solr", errors.TypeState, err)
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		ego.Log().Warn("Cannot commit transaction plan", "http-response", string(respBody), "http-status", resp.StatusCode)
		return errors.NewStateError(ego, errors.LevelWarning, fmt.Sprintf("failed to commit transaction plan to solr (responseCode=%d)", resp.StatusCode))
	}
	res := ego.con.Commit(ego.param.Name)
	if res != nil {
		res = errors.Wrap("Transaction plan accepted by solr but solr's commit returned error.", errors.TypeState, err)
	}
	ego.transactionPlan = []solrOperation{}
	return res
}

func (ego *SolrCollection) Serialize() gonatus.Conf {
	scc, valid := ego.con.Serialize().(SolrConnectionConf)
	if !valid {
		return nil
	}
	return NewSolrCollectionConf(ego.param.SchemaConf, scc, ego.param.numShards, ego.nextId)
}

//----------------------------------------------- HELPER STUFF

// solrOperation is a contract for any operation which may be planned for commit
type solrOperation interface {
	toJson() (string, error) //convert operation to json for solr's json update api
}

// solrAddOp is a record of add record (solr's add) operation to be planned for commit
type solrAddOp struct {
	collection *SolrCollection
	record     RecordConf
}

func (ego *solrAddOp) toJson() (string, error) {
	recJson, err := ego.collection.recordToJson(ego.record)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("\"add\":{\"doc\":%s}\n", recJson), nil
}

// solrDeleteByIdOp is a record of delete record (solr's delete by id) operation to be planned for commit
type solrDeleteByIdOp struct {
	collection *SolrCollection
	record     RecordConf
}

func (ego *solrDeleteByIdOp) toJson() (string, error) {
	return fmt.Sprintf("\"delete\":{\"id\":\"%d\"}\n", ego.record.Id), nil
}

// solrDeleteByQueryOp is a record of delete by filter (solr's delete by query) to be planned for commit
type solrDeleteByQueryOp struct {
	collection *SolrCollection
	filterArg  FilterArgument
}

func (ego *solrDeleteByQueryOp) toJson() (string, error) {
	query, err := ego.collection.filterArgToSolrQuery(ego.filterArg)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("\"delete\": { \"query\":\"%s\"}", query), nil
}

// solrUpdateOp is a record of edit record (solr's delete and add) operation to be planned for commit.
// For simplicity and beacuse it is not planned to be used in the system, the implementation is naive - i.e. delete followed by add instead of solr's update
type solrUpdateOp struct {
	collection *SolrCollection
	record     RecordConf
}

func (ego *solrUpdateOp) toJson() (string, error) {
	delOp := solrDeleteByIdOp{
		collection: ego.collection,
		record:     ego.record,
	}
	delPart, err := delOp.toJson()
	if err != nil {
		return "", err
	}
	addOp := solrAddOp{
		collection: ego.collection,
		record:     ego.record,
	}
	addPart, err := addOp.toJson()
	if err != nil {
		return "", err
	}
	return addPart + delPart, nil
}

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
	jsonRecord.WriteString(fmt.Sprintf("\"id\":\"%d\"", conf.Id)) //TODO
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

// translateAtomQuery converts QueryAtomConf int solr's query formula
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
		return "", errors.NewMisappError(ego, fmt.Sprint("it is not clear how to interpret number", query.Value, " as prefixf)")) //TODO it does not make sense to use numbers as prefixes (or we have to specify the meaning of such prefix)
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

// translateNegQuery converts QueryNegConf into solr's query formula
func (ego *SolrCollection) translateNegQuery(query QueryNegConf) (string, error) {
	qInner := query.QueryConf
	qInnerTranslated, err := ego.translateQueryConf(qInner)
	if err != nil {
		return qInnerTranslated, err
	}
	return fmt.Sprint("NOT(", qInnerTranslated, ")"), nil //solr is case sensitive in case of operations
}

// translateAndQuery converts QueryAndConf into solr's query formula
func (ego *SolrCollection) translateAndQuery(query QueryAndConf) (string, error) {
	return ego.translateContextQuery(query.QueryContextConf, "AND") //solr is case sensitive in case of operations
}

// translateOrQuery converts QueryOrConf into solr's query formula
func (ego *SolrCollection) translateOrQuery(query QueryOrConf) (string, error) {
	return ego.translateContextQuery(query.QueryContextConf, "OR") //solr is case sensitive in case of operations
}

// translateContextQuery converts QueryContextConf into solr's query formula. Uses elements of context as operands to specified binary operation (assumes associativity).
func (ego *SolrCollection) translateContextQuery(query QueryContextConf, binOper string) (string, error) {
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
		sb.WriteString(fmt.Sprint(" ", binOper, " "))
		sb.WriteString(sq)
	}
	sb.WriteString(")")
	return sb.String(), nil

}

// translateImplicationQuery converts QueryImplicationQuery into solr's query formula
func (ego *SolrCollection) translateImplicationQuery(query QueryImplicationConf) (string, error) {
	lATrans, err := ego.translateQueryConf(query.Left)
	if err != nil {
		return "", err
	}
	rATrans, err := ego.translateQueryConf(query.Right)
	if err != nil {
		return "", err
	}
	return fmt.Sprint("(NOT(", lATrans, ") OR ", rATrans, ")"), nil // a implies b is equal to not(a) or b
}

// translateRangeQuery converts QueryRange into solr's range query
func (ego *SolrCollection) translateRangeQuery(query QueryRange[any]) (string, error) {
	l := query.Lower
	h := query.Higher
	name := query.Name
	//println("Range query", name, l, h)
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

// formatSolrTime formats time.Time values into format demanded by solr for datetime types
func formatSolrTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339) //solr needs EXACTLY this time format and  UTC zone, i.e. change zone and then format it (see solr's docs)
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

			id, valid := docMap["id"].(string)
			if !valid {
				//this should never happen. If it happens, we are surely having wrong schema in solr.
				ego.Log().Warn("Document without id (or with wrong id data type) retrieved form solr (skipped, check solr colelction schema)", "docuemnt-data", fmt.Sprintf("%+v", docMap))
				continue
			}
			uintID, err := strconv.ParseUint(id, 10, 64)
			if err != nil {

				ego.Log().Warn("Document id can not be parsed in CId", "docuemnt-data", fmt.Sprintf("%+v", docMap))
				continue

			}
			res := RecordConf{Id: CId(uintID)}
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

// checkSchema checks if collection's schema and schema of solr's collection of the same name are compatible - i.e. if go's schema is a subset of solr's schema.
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

// newCid generates new id for a record (the last used one  + 1). Returns error if all valid ids were already used.
func (ego *SolrCollection) newCid() (CId, error) {
	ego.idLock.Lock()
	defer ego.idLock.Unlock()
	if ego.nextId == CId(MaxUint) {
		ego.Log().Warn("IDs depleted")
		return 0, errors.NewValueError(ego, errors.LevelWarning, "CId pool depleted")
	}
	ret := ego.nextId
	ego.nextId++
	return ret, nil
}

// planOperation apends op to the transactionPlan of the collection
func (ego *SolrCollection) planOperation(op solrOperation) {
	ego.transactionLock.Lock()
	ego.transactionPlan = append(ego.transactionPlan, op)
	ego.transactionLock.Unlock()
}

// queryLastId gets last id used in the solr image of this collection. Returns error if ids cannot be queried.
func (ego *SolrCollection) queryLastId() (CId, error) {
	resp, err := ego.con.Query(ego.param.Name, "*:*&fl=numId&sort=numId%20desc&rows=1")
	if err != nil {
		return 0, errors.NewStateError(ego, errors.LevelWarning, "Cannot query ids from the collection "+ego.param.Name)
	}
	rBody, _ := io.ReadAll(resp)
	result := map[string]any{}
	json.Unmarshal(rBody, &result)
	result, ok := result["response"].(map[string]any)
	if !ok {
		return 0, errors.NewStateError(ego, errors.LevelWarning, "Cannot query ids from the collection "+ego.param.Name)
	}
	reusltsNumDict, ok := result["numFound"].(float64)
	if !ok {
		return 0, errors.NewStateError(ego, errors.LevelWarning, "Cannot query ids from the collection "+ego.param.Name)
	}
	if int(reusltsNumDict) == 0 {
		//no documents in solr yet
		return 1, nil
	}
	resultDocs, ok := result["docs"].([]any)
	if !ok {
		return 0, errors.NewStateError(ego, errors.LevelWarning, "Cannot query ids from the collection "+ego.param.Name)
	}
	resultDocDict, ok := resultDocs[0].(map[string]any)
	if !ok {
		return 0, errors.NewStateError(ego, errors.LevelWarning, "Cannot query ids from the collection "+ego.param.Name)
	}
	resultingId, ok := resultDocDict["numId"].(float64) //json parses all numbers as doubles
	if !ok {
		return 0, errors.NewStateError(ego, errors.LevelWarning, "Cannot query ids from the collection "+ego.param.Name)
	}
	return CId(resultingId), nil
}
