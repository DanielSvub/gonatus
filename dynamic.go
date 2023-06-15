package gonatus

import (
	"encoding/json"
	"reflect"

	"github.com/mitchellh/mapstructure"
)

type DynamicConf map[string]any

func NewConf(class string) DynamicConf {
	ego := DynamicConf{}
	ego["CLASS"] = class
	return ego
}

func (ego DynamicConf) unfold() {
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
		return DynamicConf(nestedMap)
	}
	nestedSlice, isSlice := value.([]any)
	if isSlice {
		for _, value := range nestedSlice {
			nestedSlice = append(nestedSlice, unfold(value))
		}
	}
	return value
}

func (ego DynamicConf) Class() string {
	class, ok := ego["CLASS"].(string)
	if !ok {
		panic("The class property is not set.")
	}
	return class
}

func (ego DynamicConf) Clone() DynamicConf {
	new := DynamicConf{}
	for key, value := range ego {
		new[key] = value
	}
	return new
}

func (ego DynamicConf) Marshal() ([]byte, error) {
	return json.Marshal(ego)
}

func (ego DynamicConf) Unmarshal(jsonBytes []byte) error {
	if err := json.Unmarshal(jsonBytes, &ego); err != nil {
		return err
	}
	ego.unfold()
	return nil
}

func (ego DynamicConf) Encode(ptr any) error {
	obj := reflect.ValueOf(ptr).Elem()
	if err := mapstructure.Decode(obj.Interface(), &ego); err != nil {
		return err
	}
	ego.unfold()
	return nil
}

func (ego DynamicConf) Decode(ptr any) error {
	return mapstructure.Decode(ego, ptr)
}

func (ego DynamicConf) String() string {
	bytes, err := ego.Marshal()
	if err != nil {
		panic("Unable to serialize the conf.")
	}
	return string(bytes)
}
