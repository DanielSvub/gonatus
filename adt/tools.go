/*
ADT Library for Go
Additional tools
*/
package adt

import "strconv"

/*
Converts a value of any type to string.

Parameters:
  - value - value to convert.

Returns:
  - value converted to string.
*/
func toString(value any) string {
	switch val := any(value).(type) {
	case string:
		return `"` + val + `"`
	case bool:
		return strconv.FormatBool(val)
	case int:
		return strconv.Itoa(val)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case List[string]:
		return val.String()
	case List[bool]:
		return val.String()
	case List[int]:
		return val.String()
	case List[float64]:
		return val.String()
	case Dict[string, string]:
		return val.String()
	case Dict[string, bool]:
		return val.String()
	case Dict[string, int]:
		return val.String()
	case Dict[string, float64]:
		return val.String()
	case Dict[bool, string]:
		return val.String()
	case Dict[bool, bool]:
		return val.String()
	case Dict[bool, int]:
		return val.String()
	case Dict[bool, float64]:
		return val.String()
	case Dict[int, string]:
		return val.String()
	case Dict[int, bool]:
		return val.String()
	case Dict[int, int]:
		return val.String()
	case Dict[int, float64]:
		return val.String()
	case Dict[float64, string]:
		return val.String()
	case Dict[float64, bool]:
		return val.String()
	case Dict[float64, int]:
		return val.String()
	case Dict[float64, float64]:
		return val.String()
	default:
		panic("Value cannot be converted to string.")
	}
}

/*
Copies a list and modifies each element by a given mapping function.
The resulting element can be of a different type than the original one.
The function has one parameter, the current element.
The old list remains unchanged.

Parameters:
  - list - old list,
  - function - anonymous function to be executed.

Type parameters:
  - T - type of old list elements,
  - N - type of new list elements.

Returns:
  - new list.
*/
func MapList[T comparable, N comparable](list List[T], function func(x T) N) List[N] {
	new := NewList[N]()
	list.ForEach(func(value T) {
		new.Add(function(value))
	})
	return new
}

/*
Copies a dictionary and modifies each field by a given mapping function.
The resulting element can be of a different type than the original one.
The function has two parameters: key of the current field and its value.
The old dictionary remains unchanged.

Parameters:
  - dict - old dictionary,
  - function - anonymous function to be executed.

Type parameters:
  - K - type of dictionary keys,
  - V - type of old dictionary values,
  - N - type of new dictionary values.

Returns:
  - new dictionary.
*/
func MapDict[K comparable, V comparable, N comparable](dict Dict[K, V], function func(k K, v V) N) Dict[K, N] {
	new := NewDict[K, N]()
	dict.ForEach(func(key K, value V) {
		new.Set(key, function(key, value))
	})
	return new
}
