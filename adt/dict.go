/*
ADT Library for Go
Dictionary type
*/
package adt

/*
Interface for a dictionary.

Type parameters:
  - K - type of dictionary keys,
  - V - type of dictionary values.
*/
type Dict[K comparable, V comparable] interface {

	/*
		Acquires the value of the dictionary.

		Returns:
		  - inner map of the dictionary.
	*/
	getVal() map[K]V

	/*
		Asserts that the dictionary is initialized.
	*/
	assert()

	/*
		Sets the values of the fields of the dictionary.
		If the key already exists, the value is overwritten, if not, a new field is created.
		If one key is given multiple times, the value is set to the last one.

		Parameters:
		  - values... - any amount of key-value pairs to set.

		Returns:
		  - updated dictionary.
	*/
	Set(key K, value V) Dict[K, V]

	/*
		Deletes the fields with given keys.

		Parameters:
		  - keys... - any amount of keys to delete.

		Returns:
		  - updated dictionary.
	*/
	Unset(keys ...K) Dict[K, V]

	/*
		Deletes all fields of the dictionary.

		Returns:
		  - updated dictionary.
	*/
	Clear() Dict[K, V]

	/*
		Acquires the value under the specified key of the dictionary.

		Parameters:
		  - key - key of the field to get.

		Returns:
		  - corresponding value.
	*/
	Get(key K) V

	/*
		Serializes the dictionary into the JSON format.
		Can be called recursively.

		Returns:
		  - string representing serialized dictionary.
	*/
	String() string

	/*
		Converts the dictionary into a Go map.

		Returns:
		  - map.
	*/
	GoMap() map[K]V

	/*
		Convers the dictionary to a list of its keys.

		Returns:
		  - list of keys of the dictionary.
	*/
	Keys() List[K]

	/*
		Convers the dictionary to a list of its values.

		Returns:
		  - list of values of the dictionary.
	*/
	Values() List[V]

	/*
		Creates a deep copy of the dictionary.

		Returns:
		  - copied dictionary.
	*/
	Clone() Dict[K, V]

	/*
		Gives a number of fields of the dictionary.

		Returns:
		  - number of fields.
	*/
	Count() int

	/*
		Checks whether the dictionary is empty.

		Returns:
		  - true if the dictionary is empty, false otherwise.
	*/
	Empty() bool

	/*
		Checks if the content of the dictionary is equal to the content of another dictionary.
		Nested dictionaries and lists are compared by reference.

		Parameters:
		  - another - a dictionary to compare with.

		Returns:
		  - true if the dictionary are equal, false otherwise.
	*/
	Equals(another Dict[K, V]) bool

	/*
		Creates a new dictionary containing all elements of the old dictionary and another dictionary.
		The old dictionary remains unchanged.
		If both dictionaries contain a key, the value from another dictionary is used.

		Parameters:
		  - another - a dictionary to merge.

		Returns:
		  - new dictionary.
	*/
	Merge(another Dict[K, V]) Dict[K, V]

	/*
		Creates a new dictionary containing the given fields of the existing dictionary.

		Parameters:
		  - keys... - any amount of keys to be in the new dictionary.

		Returns:
		  - created plucked dictionary.
	*/
	Pluck(keys ...K) Dict[K, V]

	/*
		Checks if the dictionary contains a field with a given value.
		Dictionaries and lists are compared by reference.

		Parameters:
		  - value - the value to check.

		Returns:
		  - true if the dictionary contains the value, false otherwise.
	*/
	Contains(elem V) bool

	/*
		Gives a key containing a given value.
		If multiple keys contain the value, any of them is returned.
		Panics if the key does not exist.

		Parameters:
		  - value - the value to check.

		Returns:
		  - key for the value.
	*/
	KeyOf(elem V) K

	/*
		Checks if a given key exists within the dictionary.

		Parameters:
		  - key - the key to check.

		Returns:
		  - true if the key exists, false otherwise.
	*/
	KeyExists(key K) bool

	/*
		Executes a given function over an every field of the dictionary.
		The function has two parameters: key of the current field and its value.

		Parameters:
		  - function - anonymous function to be executed.

		Returns:
		  - unchanged dictionary.
	*/
	ForEach(function func(k K, v V)) Dict[K, V]

	/*
		Copies the dictionary and modifies each field by a given mapping function.
		The resulting element has to be of a same type as the original one.
		The function has two parameters: key of the current field and its value.
		The old dictionary remains unchanged.

		Parameters:
		  - function - anonymous function to be executed.

		Returns:
		  - new dictionary.
	*/
	Map(function func(k K, v V) V) Dict[K, V]
}

/*
Dictionary, a reference type. Contains a map of key-value pairs.

Implements:
  - Dicter.

Type parameters:
  - K - type of dictionary keys,
  - V - type of dictionary values.
*/
type mapDict[K comparable, V comparable] struct {
	val map[K]V
}

/*
Dictionary constructor.
Creates a new dictionary.

Parameters:
  - values... - any amount of key-value pairs to set after the dictionary creation.

Type parameters:
  - K - type of dictionary keys,
  - V - type of dictionary values.

Returns:
  - pointer to the created dictionary.
*/
func NewDict[K comparable, V comparable]() Dict[K, V] {
	ego := mapDict[K, V]{make(map[K]V)}
	return &ego
}

/*
Dictionary constructor.
Converts a map of supported types to a dictionary.

Parameters:
  - dict - original map.

Type parameters:
  - K - type of dictionary keys,
  - V - type of dictionary values.

Returns:
  - pointer to the created dictionary.
*/
func NewDictFrom[K comparable, V comparable](goMap map[K]V) *mapDict[K, V] {
	NewDict[any, any]()
	return &mapDict[K, V]{goMap}
}

func (ego *mapDict[K, V]) getVal() map[K]V {
	return ego.val
}

func (ego *mapDict[K, V]) assert() {
	if ego == nil || ego.getVal() == nil {
		panic("The dictionary is not initialized.")
	}
}

func (ego *mapDict[K, V]) Set(key K, value V) Dict[K, V] {
	ego.assert()
	ego.getVal()[key] = value
	return ego
}

func (ego *mapDict[K, V]) Unset(keys ...K) Dict[K, V] {
	ego.assert()
	for _, key := range keys {
		if !ego.KeyExists(key) {
			panic("The Dictionary does not contain the key " + toString(key) + ".")
		}
		delete(ego.getVal(), key)
	}
	return ego
}

func (ego *mapDict[K, V]) Clear() Dict[K, V] {
	ego.assert()
	ego.val = make(map[K]V, 0)
	return ego
}

func (ego *mapDict[K, V]) Get(key K) V {
	ego.assert()
	if !ego.KeyExists(key) {
		panic("The Dictionary does not contain the key " + toString(key) + ".")
	}
	return ego.getVal()[key]
}

func (ego *mapDict[K, V]) String() string {
	result := "{"
	i := 0
	for key, value := range ego.getVal() {
		result += toString(key) + ":" + toString(value)
		if i++; i < len(ego.getVal()) {
			result += ","
		}
	}
	result += "}"
	return result
}

func (ego *mapDict[K, V]) GoMap() map[K]V {
	ego.assert()
	return ego.getVal()
}

func (ego *mapDict[K, V]) Keys() List[K] {
	keys := NewList[K]()
	for key := range ego.getVal() {
		keys.Add(key)
	}
	return keys
}

func (ego *mapDict[K, V]) Values() List[V] {
	values := NewList[V]()
	for _, value := range ego.getVal() {
		values.Add(value)
	}
	return values
}

func (ego *mapDict[K, V]) Clone() Dict[K, V] {
	obj := NewDict[K, V]()
	for key, value := range ego.getVal() {
		obj.Set(key, value)
	}
	return obj
}

func (ego *mapDict[K, V]) Count() int {
	ego.assert()
	return len(ego.getVal())
}

func (ego *mapDict[K, V]) Empty() bool {
	return ego.Count() == 0
}

func (ego *mapDict[K, V]) Equals(another Dict[K, V]) bool {
	if ego.Count() != another.Count() {
		return false
	}
	for k := range ego.getVal() {
		if ego.getVal()[k] != another.getVal()[k] {
			return false
		}
	}
	return true
}

func (ego *mapDict[K, V]) Merge(another Dict[K, V]) Dict[K, V] {
	ego.assert()
	result := ego.Clone()
	another.ForEach(func(key K, val V) {
		result.Set(key, val)
	})
	return result
}

func (ego *mapDict[K, V]) Pluck(keys ...K) Dict[K, V] {
	ego.assert()
	result := NewDict[K, V]()
	for _, key := range keys {
		result.Set(key, ego.Get(key))
	}
	return result
}

func (ego *mapDict[K, V]) Contains(value V) bool {
	ego.assert()
	for _, item := range ego.getVal() {
		if item == value {
			return true
		}
	}
	return false
}

func (ego *mapDict[K, V]) KeyOf(value V) K {
	ego.assert()
	for key, item := range ego.getVal() {
		if item == value {
			return key
		}
	}
	panic("The dictionary does not contain the value " + toString(value) + ".")
}

func (ego *mapDict[K, V]) KeyExists(key K) bool {
	ego.assert()
	_, ok := ego.getVal()[key]
	return ok
}

func (ego *mapDict[K, V]) ForEach(function func(K, V)) Dict[K, V] {
	ego.assert()
	for key, item := range ego.getVal() {
		function(key, item)
	}
	return ego
}

func (ego *mapDict[K, V]) Map(function func(K, V) V) Dict[K, V] {
	ego.assert()
	result := NewDict[K, V]()
	for key, item := range ego.getVal() {
		result.Set(key, function(key, item))
	}
	return result
}
