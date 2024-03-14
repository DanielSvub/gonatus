package dynamic_test

import (
	"testing"

	. "github.com/DanielSvub/gonatus/dynamic"
)

func TestGonatusBase(t *testing.T) {

	type Dog struct {
		Gobject
		Name string
		Age  int
	}

	NewDog := func(conf Conf) *Dog {
		ego := new(Dog)
		conf.Load(ego)
		ego.Init(ego)
		return ego
	}

	init := NewConf("Dog")
	init["Name"] = "Doge"
	init["Age"] = 2
	dog := NewDog(init)

	conf := dog.Serialize()
	copy := conf.Clone()

	if dog.Ptr() != dog {
		t.Errorf("Object and its pointer are not equal.")
	}

	if conf["Name"] != copy["Name"] {
		t.Errorf("Names are not equal.")
	}

	if conf["Age"] != copy["Age"] {
		t.Errorf("Ages are not equal.")
	}

	test := NewConf("Dog")
	if test.Class() != "Dog" {
		t.Errorf("Invalid class.")
	}

	err := test.Unmarshal([]byte(conf.String()))
	if err != nil {
		t.Errorf("Error during unmarshalling.")
	}

	empty := NewDog(nil)
	if empty.Serialize().Class() != "Dog" {
		t.Errorf("Problem with an empty Conf.")
	}

}
