package gonatus

import (
	"encoding/json"
	"reflect"

	"github.com/mitchellh/mapstructure"
)

type Conf interface {
	Class() string
	SetClass(class string) Conf

	Set(pairs ...Pair) Conf
	Get(key string) any

	Clone() Conf

	Marshal() ([]byte, error)
	Unmarshal(jsonBytes []byte) error
	Encode(ptr any) error
	Decode(ptr any) error

	String() string
}

type MapConf struct {
	class string
	value map[string]any
}

func NewConf(class string) Conf {
	return &MapConf{
		class: class,
		value: map[string]any{},
	}
}

func (ego *MapConf) Class() string {
	return ego.class
}

func (ego *MapConf) SetClass(class string) Conf {
	ego.class = class
	return ego
}

func (ego *MapConf) Set(pairs ...Pair) Conf {
	for _, pair := range pairs {
		ego.value[pair.Key] = pair.Value
	}
	return ego
}

func (ego *MapConf) Get(key string) any {
	return ego.value[key]
}

func (ego *MapConf) Clone() Conf {
	new := &MapConf{
		class: ego.Class(),
		value: map[string]any{},
	}
	for key, value := range ego.value {
		new.value[key] = value
	}
	return new
}

func (ego *MapConf) Marshal() ([]byte, error) {
	return json.Marshal(ego.value)
}

func (ego *MapConf) Unmarshal(jsonBytes []byte) error {
	return json.Unmarshal(jsonBytes, &ego.value)
}

func (ego *MapConf) Encode(ptr any) error {
	obj := reflect.ValueOf(ptr).Elem()
	return mapstructure.Decode(obj.Interface(), &ego.value)
}

func (ego *MapConf) Decode(ptr any) error {
	return mapstructure.Decode(ego.value, ptr)
}

func (ego *MapConf) String() string {
	bytes, err := ego.Marshal()
	if err != nil {
		panic("Unable to serialize the conf.")
	}
	return string(bytes)
}

type Pair struct {
	Key   string
	Value any
}

func NewPair(key string, value any) Pair {
	if key == "" {
		panic("Conf key has to be non-empty string.")
	}
	return Pair{key, value}
}

type Gobjecter interface {
	Serialize() Conf
	Ptr() any
}

type Gobject struct {
	ptr  any
	conf Conf
}

func (ego *Gobject) Init(egoPtr any, conf Conf) {

	className := reflect.TypeOf(egoPtr).Elem().Name()

	if conf != nil {

		err := conf.Decode(egoPtr)
		if err != nil {
			panic(err)
		}
		ego.conf = conf.Clone()

	} else {

		ego.conf = NewConf(className)

	}

	ego.ptr = egoPtr

}

func (ego *Gobject) Serialize() Conf {

	conf := NewConf(ego.conf.Class())
	err := conf.Encode(ego.ptr)
	if err != nil {
		panic(err)
	}
	return conf

}

func (ego *Gobject) Ptr() any {
	return ego.ptr
}
