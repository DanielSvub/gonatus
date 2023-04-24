package gonatus

import (
	"encoding/json"
	"reflect"
)

type Conf struct {
	Class string
	Value map[string]any
}

func (ego *Conf) Marshal() ([]byte, error) {
	return json.Marshal(ego.Value)
}

func (ego *Conf) Unmarshal(jsonBytes []byte) error {
	return json.Unmarshal(jsonBytes, &ego.Value)
}

type Gobjecter interface {
	Serialize() Conf
}

type Gobject struct {
	ptr  any
	conf Conf
}

func (ego *Gobject) Init(egoPtr any, conf Conf) {
	res, err := conf.Marshal()
	if err != nil {
		panic(err)
	}
	// !!! marshalling and unmarshalling again
	err = json.Unmarshal(res, egoPtr)
	if err != nil {
		panic(err)
	}
	ego.ptr = egoPtr
	// TODO conf.clone()
	ego.conf = conf
}

func (ego *Gobject) Serialize() Conf {
	obj := reflect.ValueOf(ego.ptr).Elem()
	res, err := json.Marshal(obj.Interface())
	if err != nil {
		panic(err)
	}
	var conf Conf
	err = conf.Unmarshal(res)
	if err != nil {
		panic(err)
	}
	conf.Class = reflect.TypeOf(ego.ptr).Elem().Name()
	return conf
}
