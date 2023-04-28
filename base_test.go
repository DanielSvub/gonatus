package gonatus_test

import (
	"testing"

	. "github.com/SpongeData-cz/gonatus"
)

func TestGonatusBase(t *testing.T) {

	type Dog struct {
		Gobject
		Name string
		Age  int
	}

	NewDog := func(conf Conf) *Dog {
		ego := &Dog{}
		ego.Init(ego, conf)
		return ego
	}

	init := NewConf("Dog").Set(
		NewPair("Name", "Doge"),
		NewPair("Age", 2),
	)
	dog := NewDog(init)

	conf := dog.Serialize()
	copy := conf.Clone()

	if conf.Get("Name") != copy.Get("Name") {
		t.Errorf("Names are not equal.")
	}

	if conf.Get("Age") != copy.Get("Age") {
		t.Errorf("Ages are not equal.")
	}

	test := NewConf("Default")
	test.SetClass("Dog")
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
