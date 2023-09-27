package collection

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/SpongeData-cz/gonatus"
	"github.com/SpongeData-cz/gonatus/errors"
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
	//RawPostRequest allows for user built post request to solr (i.e. user can ask anything he has rights to do via request body and its content type)
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
	fullRequest := ego.baseUrl + "/" + collection + "/select?q=" + query
	ego.Log().Info("Request sent", "collection", collection, "query", query, "fullrequest", fullRequest)

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
			schemaJsonSB.WriteString(",\"multiValued\":false}")
		}
		//		if i != len(schema.Fields)-1 {
		schemaJsonSB.WriteRune(',')
		//}
	}
	//brace yourself type-hell starts here
	//id field is in solr by default (it is string), but sorting on strings is lexiocraphical
	//therefore we automatically copy it into numeric (double) version for possibility of sorting - yes, solr implcitly parses the string into double-number (and no, solr does not have ulong)
	schemaJsonSB.WriteString("\"add-field\":{\"name\":\"numId\",\"type\":\"pdouble\",\"required\":true,\"indexed\":true,\"stored\":true},")
	schemaJsonSB.WriteString("\"add-copy-field\":{\"source\":\"id\",\"dest\":\"numId\"}")
	//type hell ends here
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
