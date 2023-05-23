package gonatus

import (
	"encoding/json"
	"reflect"

	"github.com/mitchellh/mapstructure"
)

type Conf map[string]any

func NewConf(class string) Conf {
	ego := Conf{}
	ego["CLASS"] = class
	return ego
}

func (ego Conf) unfold() {
	for key, value := range ego {
		ego[key] = unfold(value)
	}
}

func unfold(value any) any {
	nestedMap, isMap := value.(map[string]any)
	if isMap {
		for key, value := range nestedMap {
			nestedMap[key] = unfold(value)
		}
		return Conf(nestedMap)
	}
	nestedSlice, isSlice := value.([]any)
	if isSlice {
		for _, value := range nestedSlice {
			nestedSlice = append(nestedSlice, unfold(value))
		}
	}
	return value
}

func (ego Conf) Load(target Gobjecter) error {

	className := reflect.TypeOf(target).Elem().Name()

	if ego != nil {

		if err := ego.Decode(target); err != nil {
			return err
		}
		target.setConf(ego.Clone())

	} else {

		target.setConf(NewConf(className))

	}

	target.setPtr(target)
	return nil

}

func (ego Conf) Class() string {
	class, ok := ego["CLASS"].(string)
	if !ok {
		panic("The class property is not set.")
	}
	return class
}

func (ego Conf) Clone() Conf {
	new := Conf{}
	for key, value := range ego {
		new[key] = value
	}
	return new
}

func (ego Conf) Marshal() ([]byte, error) {
	return json.Marshal(ego)
}

func (ego Conf) Unmarshal(jsonBytes []byte) error {
	if err := json.Unmarshal(jsonBytes, &ego); err != nil {
		return err
	}
	ego.unfold()
	return nil
}

func (ego Conf) Encode(ptr any) error {
	obj := reflect.ValueOf(ptr).Elem()
	if err := mapstructure.Decode(obj.Interface(), &ego); err != nil {
		return err
	}
	ego.unfold()
	return nil
}

func (ego Conf) Decode(ptr any) error {
	return mapstructure.Decode(ego, ptr)
}

func (ego Conf) String() string {
	bytes, err := ego.Marshal()
	if err != nil {
		panic("Unable to serialize the conf.")
	}
	return string(bytes)
}

type Gobjecter interface {
	Serialize() Conf
	Ptr() any
	setPtr(ptr Gobjecter)
	setConf(conf Conf)
}

type Gobject struct {
	ptr   Gobjecter
	conf  Conf
	CLASS string
}

func (ego *Gobject) Init() {}

func (ego *Gobject) Serialize() Conf {

	conf := NewConf(reflect.TypeOf(ego.ptr).Elem().Name())
	err := conf.Encode(ego.ptr)
	if err != nil {
		panic(err)
	}
	return conf

}

func (ego *Gobject) Ptr() any {
	return ego.ptr
}

func (ego *Gobject) setPtr(ptr Gobjecter) {
	ego.ptr = ptr
}

func (ego *Gobject) setConf(conf Conf) {
	ego.conf = conf
}
