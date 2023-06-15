package adt_test

import (
	"strconv"
	"testing"

	. "github.com/SpongeData-cz/gonatus/adt"
)

func TestList(t *testing.T) {

	t.Run("basics", func(t *testing.T) {
		l := NewList(1, 2, 3)
		if l.Get(0) != 1 {
			t.Error("Get should return 1.")
		}
		if !l.Insert(1, 4).Equals(NewList(1, 4, 2, 3)) {
			t.Error("Element has not been inserted properly.")
		}
		if !l.Replace(1, 5).Equals(NewList(1, 5, 2, 3)) {
			t.Error("Element has not been replaced properly.")
		}
		if !l.Delete(1).Equals(NewList(1, 2, 3)) {
			t.Error("Element has not been deleted properly.")
		}
		if l.Add(4).Pop() != 4 {
			t.Error("Pop does not return a correct value.")
		}
		if !l.Equals(NewList(1, 2, 3)) {
			t.Error("Pop is not working properly.")
		}
		if !l.Clone().Clear().Equals(NewList[int]()) {
			t.Error("List has not been cleared properly.")
		}
		if l.Count() != 3 {
			t.Error("List should have 3 elements.")
		}
		if l.Empty() {
			t.Error("List should not be empty.")
		}
		if !NewList[int]().Empty() {
			t.Error("Empty list should be empty.")
		}
		if !NewList(1, 2).Concat(NewList(3, 4)).Equals(NewList(1, 2, 3, 4)) {
			t.Error("Concatenation does not work properly.")
		}
		if !l.Contains(1) {
			t.Error("List should contain element 1.")
		}
		if l.Contains(4) {
			t.Error("List should not contain element 4.")
		}
		if l.IndexOf(2) != 1 {
			t.Error("Element 2 should be at index 1.")
		}
		if l.IndexOf(4) != -1 {
			t.Error("IndexOf should return -1 if the element is not present.")
		}
		if !l.Clone().Reverse().Equals(NewList(3, 2, 1)) {
			t.Error("Reversing does not work properly.")
		}
		if l.String() != `[1,2,3]` {
			t.Error("Serialization does not work properly.")
		}
	})

	t.Run("constructors", func(t *testing.T) {
		if !NewListOf(1, 3).Equals(NewList(1, 1, 1)) {
			t.Error("ListOf does not work properly.")
		}
		if !NewListFrom(make([]int, 3)).Equals(NewList(0, 0, 0)) {
			t.Error("ListFrom does not work properly.")
		}
	})

	t.Run("sublist", func(t *testing.T) {
		l := NewList(0, 1, 2, 3, 4)
		if !l.SubList(0, 0).Equals(l) {
			t.Error("SubList(0, 0) should return original list.")
		}
		if !l.SubList(2, 4).Equals(NewList(2, 3)) {
			t.Error("SubList(2, 4) should return two elements.")
		}
		if !l.SubList(0, -2).Equals(NewList(0, 1, 2)) {
			t.Error("SubList(0, -2) should cut last two elements.")
		}
	})

	t.Run("functional", func(t *testing.T) {
		l := NewList(1, 2, 3, 4, 5)
		t1 := NewList[int]()
		l.ForEach(func(value int) { t1.Add(value) })
		if !t1.Equals(l) {
			t.Error("ForEach does not work properly.")
		}
		if !l.Map(func(value int) int { return value }).Equals(l) {
			t.Error("Map does not work properly.")
		}
		if l.Reduce(0, func(sum, x int) int { return sum + x }) != 15 {
			t.Error("Reduce does not work properly.")
		}
		if l.Filter(func(value int) bool { return value <= 3 }).Count() != 3 {
			t.Error("Filter does not work properly.")
		}
	})

	t.Run("numeric", func(t *testing.T) {
		if NewList(2.0, 4.0, 3.0, 5.0, 1.0).Max() != 5.0 {
			t.Error("Float max does not work.")
		}
		if NewList(2, 4, 3, 5, 1).Max() != 5.0 {
			t.Error("Int max does not work.")
		}
		if NewList(2.0, 4.0, 3.0, 5.0, 1.0).Min() != 1.0 {
			t.Error("Float min does not work.")
		}
		if NewList(2, 4, 3, 5, 1).Min() != 1.0 {
			t.Error("Min does not work.")
		}
		if NewList(1.0, 4.0, 5.0).Sum() != 10.0 {
			t.Error("Float sum does not work.")
		}
		if NewList(1, 4, 5).Sum() != 10.0 {
			t.Error("Int sum does not work.")
		}
		if NewList(1.0, 4.0, 5.0).Prod() != 20.0 {
			t.Error("Float prod does not work.")
		}
		if NewList(1, 4, 5).Prod() != 20.0 {
			t.Error("Int prod does not work.")
		}
		if NewList(0.0, 5.0, 5.0, 10.0).Avg() != 5.0 {
			t.Error("Float avg does not work.")
		}
		if NewList(0, 5, 5, 10).Avg() != 5.0 {
			t.Error("Int avg does not work.")
		}
	})

	t.Run("sorting", func(t *testing.T) {
		if !NewList(2, 4, 3, 5, 1).Sort().Equals(NewList(1, 2, 3, 4, 5)) {
			t.Error("Ascending int sorting does not work properly.")
		}
		if !NewList(2.0, 4.0, 3.0, 5.0, 1.0).Sort().Equals(NewList(1.0, 2.0, 3.0, 4.0, 5.0)) {
			t.Error("Ascending float sorting does not work properly.")
		}
		if !NewList("b", "c", "a").Sort().Equals(NewList("a", "b", "c")) {
			t.Error("Ascending string sorting does not work properly.")
		}
	})

}

func TestDict(t *testing.T) {

	t.Run("basics", func(t *testing.T) {
		o := NewDict[string, int]().
			Set("first", 1).
			Set("second", 2).
			Set("third", 3)

		if o.Get("first") != 1 {
			t.Error("Get should return 1.")
		}
		if !o.Keys().Contains("second") {
			t.Error("Key list should contain the key.")
		}
		if !o.Values().Contains(2) {
			t.Error("Value list should contain the value.")
		}
		if !o.Contains(3) {
			t.Error("Dict should contain value 3.")
		}
		if o.Contains(4) {
			t.Error("Dict should  not contain value 4.")
		}
		if o.Count() != 3 {
			t.Error("Dict should have 3 fields.")
		}
		if o.KeyOf(2) != "second" {
			t.Error("Key for value 2 should be 'second'.")
		}
		if o.Equals(NewDict[string, int]()) {
			t.Error("Dict should not be equal to empty Dict.")
		}
		if o.Pluck("first", "second").Count() != 2 {
			t.Error("Plucked Dict should have 2 fields.")
		}
		o.Unset("third")
		if !NewDict[string, int]().Set("first", 1).Merge(NewDict[string, int]().Set("second", 2)).Equals(o) {
			t.Error("Merge does not work properly.")
		}
		json := o.String()
		if json != `{"first":1,"second":2}` && json != `{"second":2,"first":1}` {
			t.Error("Serialization does not work properly.")
		}
		o.Clear()
		if !o.Empty() {
			t.Error("Dict should be empty.")
		}
	})

	t.Run("cloning", func(t *testing.T) {
		o := NewDict[string, any]().
			Set("string", "test").
			Set("bool", true).
			Set("int", 1).
			Set("float", 3.14).
			Set("nil", nil)

		if !o.Equals(o.Clone()) {
			t.Error("Dict should be equal to itself.")
		}
	})

	t.Run("functional", func(t *testing.T) {
		o := NewDict[string, int]().
			Set("first", 1).
			Set("second", 2).
			Set("third", 3)
		t1 := NewDict[string, int]()
		o.ForEach(func(key string, value int) { t1.Set(key, value) })
		if !t1.Equals(o) {
			t.Error("ForEach does not work properly.")
		}
		if !o.Map(func(key string, value int) int { return value }).Equals(o) {
			t.Error("Map does not work properly.")
		}
	})

}

func TestTools(t *testing.T) {

	t.Run("mapList", func(t *testing.T) {
		l := NewList(1, 2, 3)
		t1 := NewList("1", "2", "3")
		if !MapList(l, func(value int) string {
			return strconv.Itoa(value)
		}).Equals(t1) {
			t.Error("MapList does not work properly.")
		}
	})

	t.Run("mapDict", func(t *testing.T) {
		o := NewDict[string, int]().
			Set("first", 1).
			Set("second", 2).
			Set("third", 3)
		t1 := NewDict[string, string]().
			Set("first", "1").
			Set("second", "2").
			Set("third", "3")
		if !MapDict(o, func(key string, value int) string {
			return strconv.Itoa(value)
		}).Equals(t1) {
			t.Error("MapDict does not work properly.")
		}
	})
}
