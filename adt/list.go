/*
ADT Library for Go
List type
*/
package adt

import (
	"math"
	"sort"
	"strconv"
)

/*
Interface for a list.

Type parameters:
  - T - type of list elements.
*/
type List[T comparable] interface {

	/*
		Acquires the value of the list.

		Returns:
		  - inner slice of the list.
	*/
	getVal() []T

	/*
		Asserts that the list is initialized.
	*/
	assert()

	/*
		Panics if the index is out of range.
	*/
	indexCheck(index int)

	/*
		Inserts new elements at the end of the list.

		Parameters:
		  - values... - any amount of elements to add.

		Returns:
		  - updated list.
	*/
	Add(val ...T) List[T]

	/*
		Inserts a new element at the specified position in the list.

		Parameters:
		  - index - position where the element should be inserted,
		  - value - element to insert.

		Returns:
		  - updated list.
	*/
	Insert(index int, value T) List[T]

	/*
		Replaces an existing element of the list with a new one.

		Parameters:
		  - index - position of the element which should be replaced,
		  - value - new element.

		Returns:
		  - updated list.
	*/
	Replace(index int, value T) List[T]

	/*
		Deletes the elements at the specified positions in the list.

		Parameters:
		  - indexes... - any amount of positions of the elements to delete.

		Returns:
		  - updated list.
	*/
	Delete(index ...int) List[T]

	/*
		Deletes the last element in the list and returns it.

		Returns:
		  - popped element.
	*/
	Pop() T

	/*
		Deletes all elements in the list.

		Returns:
		  - updated list.
	*/
	Clear() List[T]

	/*
		Acquires the element at the specified position in the list.

		Parameters:
		  - index - position of the element to get.

		Returns:
		  - corresponding value.
	*/
	Get(index int) T

	/*
		Serializes the list into the JSON format.
		Can be called recursively.

		Returns:
		  - string representing serialized list.
	*/
	String() string

	/*
		Converts the list into a Go slice.
		The slice is a reference.

		Returns:
		  - slice.
	*/
	GoSlice() []T

	/*
		Creates a copy of the list.

		Returns:
		  - copied list.
	*/
	Clone() List[T]

	/*
		Gives a number of elements in the list.

		Returns:
		  - number of elements.
	*/
	Count() int

	/*
		Checks whether the list is empty.

		Returns:
		  - true if the list is empty, false otherwise.
	*/
	Empty() bool

	/*
		Checks if the content of the list is equal to the content of another list.
		Nested dictionaries and lists are compared by reference.

		Parameters:
		  - another - a list to compare with.

		Returns:
		  - true if the lists are equal, false otherwise.
	*/
	Equals(another List[T]) bool

	/*
		Creates a new list containing all elements of the old list and another list.
		The old list remains unchanged.

		Parameters:
		  - another - a list to append.

		Returns:
		  - new list.
	*/
	Concat(another List[T]) List[T]

	/*
		Creates a new list containing the elements from the starting index (including) to the ending index (excluding).
		If the ending index is zero, it is set to the length of the list. If negative, it is counted from the end of the list.
		Starting index has to be non-negative and cannot be higher than the ending index.

		Parameters:
		  - start - starting index,
		  - end - ending index.

		Returns:
		  - created sub list.
	*/
	SubList(start int, end int) List[T]

	/*
		Checks if the list contains a given element.
		Dictionaries and lists are compared by reference.

		Parameters:
		  - elem - the element to check.

		Returns:
		  - true if the list contains the element, false otherwise.
	*/
	Contains(elem T) bool

	/*
		Gives a position of the first occurrence of a given element.

		Parameters:
		  - elem - the element to check.

		Returns:
		  - index of the element (-1 if the list does not contain the element).
	*/
	IndexOf(elem T) int

	/*
		Searches for an element satisfying a condition.
		The function has one parameter, the current element, and returns bool.

		Parameters:
		  - fn - anonymous function to be executed.

		Returns:
		  - pointer to the first element of the list satisfying the condition, nil if no such item.
	*/
	Search(fn func(x T) bool) *T

	/*
		Reverses the order of elements in the list.

		Returns:
		  - updated list.
	*/
	Reverse() List[T]

	/*
		Executes a given function over an every element of the list.
		The function has one parameter, the current element.

		Parameters:
		  - function - anonymous function to be executed.

		Returns:
		  - unchanged list.
	*/
	ForEach(function func(x T)) List[T]

	/*
		Copies the list and modifies each element by a given mapping function.
		The resulting element has to be of a same type as the original one.
		The function has one parameter, the current element.
		The old list remains unchanged.

		Parameters:
		  - function - anonymous function to be executed.

		Returns:
		  - new list.
	*/
	Map(function func(x T) T) List[T]

	/*
		Reduces all elements of the list into a single value.
		The result has to be of the same type as the elements of the list.
		The function has two parameters: value returned by the previous iteration and value of the current element.
		The old list remains unchanged.

		Parameters:
		  - function - anonymous function to be executed.

		Returns:
		  - computed value.
	*/
	Reduce(initial T, function func(res T, x T) T) T

	/*
		Creates a new list containing elements of the old one satisfying a condition.
		The function has one parameter, the current element, and returns bool.
		The old list remains unchanged.

		Parameters:
		  - function - anonymous function to be executed.

		Returns:
		  - filtered list.
	*/
	Filter(function func(x T) bool) List[T]

	/*
		Sorts the elements in the list (ascending).
		Only lists of strings, ints and floats are sortable.

		Returns:
		  - updated list.
	*/
	Sort() List[T]

	/*
		Finds a minimum of the list.
		The list has to be numeric.

		Returns:
		  - found minimum.
	*/
	Min() float64

	/*
		Finds a maximum of the list.
		The list has to be numeric.

		Returns:
		  - found maximum.
	*/
	Max() float64

	/*
		Computes a sum of the list.
		The list has to be numeric.

		Returns:
		  - sum of the elements.
	*/
	Sum() float64

	/*
		Computes a product of the list.
		The list has to be numeric.

		Returns:
		  - product of the elements.
	*/
	Prod() float64

	/*
		Computes an avarage of the list.
		The list has to be numeric.

		Returns:
		  - average of the elements.
	*/
	Avg() float64
}

/*
sliceList, a reference type. Contains a slice of elements.

Implements:
  - Lister.

Type parameters:
  - T - type of sliceList elements.
*/
type sliceList[T comparable] struct {
	val []T
}

/*
List constructor.
Creates a new list.

Parameters:
  - values... - any amount of initial elements.

Type parameters:
  - T - type of list elements.

Returns:
  - pointer to the created list.
*/
func NewList[T comparable](values ...T) List[T] {
	ego := sliceList[T]{}
	ego.val = make([]T, 0)
	ego.Add(values...)
	return &ego
}

/*
List constructor.
Creates a new list of n repeated values.

Parameters:
  - value - value to repeat,
  - count - number of repetitions.

Type parameters:
  - T - type of list elements.

Returns:
  - pointer to the created list.
*/
func NewListOf[T comparable](value T, count int) *sliceList[T] {
	ego := sliceList[T]{make([]T, count)}
	for i := 0; i < count; i++ {
		ego.getVal()[i] = value
	}
	return &ego
}

/*
List constructor.
Converts a slice to a list.

Parameters:
  - slice - original slice.

Type parameters:
  - T - type of list elements.

Returns:
  - pointer to the created list.
*/
func NewListFrom[T comparable](goSlice []T) *sliceList[T] {
	return &sliceList[T]{goSlice}
}

func (ego *sliceList[T]) getVal() []T {
	return ego.val
}

func (ego *sliceList[T]) assert() {
	if ego == nil || ego.getVal() == nil {
		panic("The list is not initialized.")
	}
}

func (ego *sliceList[T]) indexCheck(index int) {
	if index < 0 || index > ego.Count() {
		panic("Index " + strconv.Itoa(index) + " out of range.")
	}
}

func (ego *sliceList[T]) Add(values ...T) List[T] {
	ego.assert()
	for _, val := range values {
		ego.val = append(ego.getVal(), val)
	}
	return ego
}

func (ego *sliceList[T]) Insert(index int, value T) List[T] {
	ego.assert()
	ego.indexCheck(index)
	if index == ego.Count() {
		return ego.Add(value)
	}
	ego.val = append(ego.getVal()[:index+1], ego.getVal()[index:]...)
	ego.getVal()[index] = value
	return ego
}

func (ego *sliceList[T]) Replace(index int, value T) List[T] {
	ego.assert()
	ego.indexCheck(index)
	ego.getVal()[index] = value
	return ego
}

func (ego *sliceList[T]) Delete(indexes ...int) List[T] {
	ego.assert()
	if len(indexes) > 1 {
		sort.Ints(indexes)
	}
	for i := len(indexes) - 1; i >= 0; i-- {
		index := indexes[i]
		ego.indexCheck(index)
		ego.val = append(ego.getVal()[:index], ego.getVal()[index+1:]...)
	}
	return ego
}

func (ego *sliceList[T]) Pop() T {
	count := ego.Count()
	if count == 0 {
		panic("Cannot pop from an empty list.")
	}
	last := ego.Count() - 1
	elem := ego.getVal()[last]
	ego.Delete(last)
	return elem
}

func (ego *sliceList[T]) Clear() List[T] {
	ego.assert()
	ego.val = make([]T, 0)
	return ego
}

func (ego *sliceList[T]) Get(index int) T {
	ego.assert()
	ego.indexCheck(index)
	return ego.getVal()[index]
}

func (ego *sliceList[T]) String() string {
	result := "["
	for i, value := range ego.getVal() {
		result += toString(value)
		if i+1 < len(ego.getVal()) {
			result += ","
		}
	}
	result += "]"
	return result
}

func (ego *sliceList[T]) GoSlice() []T {
	ego.assert()
	return ego.getVal()
}

func (ego *sliceList[T]) Clone() List[T] {
	ego.assert()
	return NewList(ego.getVal()...)
}

func (ego *sliceList[T]) Count() int {
	ego.assert()
	return len(ego.getVal())
}

func (ego *sliceList[T]) Empty() bool {
	return ego.Count() == 0
}

func (ego *sliceList[T]) Equals(another List[T]) bool {
	if ego.Count() != another.Count() {
		return false
	}
	for i := range ego.getVal() {
		if ego.getVal()[i] != another.getVal()[i] {
			return false
		}
	}
	return true
}

func (ego *sliceList[T]) Concat(another List[T]) List[T] {
	ego.assert()
	return &sliceList[T]{append(ego.getVal(), another.getVal()...)}
}

func (ego *sliceList[T]) SubList(start int, end int) List[T] {
	ego.assert()
	if end <= 0 {
		end = ego.Count() + end
	}
	if start > end {
		panic("Starting index higher than ending index.")
	}
	if ego.Count() < end || start < 0 {
		panic("Indexes out of range.")
	}
	list := &sliceList[T]{make([]T, end-start)}
	copy(list.getVal(), ego.getVal()[start:end])
	return list
}

func (ego *sliceList[T]) Contains(elem T) bool {
	ego.assert()
	for _, item := range ego.getVal() {
		if item == elem {
			return true
		}
	}
	return false
}

func (ego *sliceList[T]) IndexOf(elem T) int {
	ego.assert()
	for i, item := range ego.getVal() {
		if item == elem {
			return i
		}
	}
	return -1
}

func (ego *sliceList[T]) Search(fn func(elem T) bool) *T {
	ego.assert()
	for _, item := range ego.getVal() {
		if fn(item) {
			return &item
		}
	}
	return nil
}

func (ego *sliceList[T]) Reverse() List[T] {
	ego.assert()
	for i := ego.Count()/2 - 1; i >= 0; i-- {
		opp := ego.Count() - 1 - i
		ego.getVal()[i], ego.getVal()[opp] = ego.getVal()[opp], ego.getVal()[i]
	}
	return ego
}

func (ego *sliceList[T]) ForEach(function func(T)) List[T] {
	ego.assert()
	for _, item := range ego.getVal() {
		function(item)
	}
	return ego
}

func (ego *sliceList[T]) Map(function func(T) T) List[T] {
	ego.assert()
	result := NewList[T]()
	for _, item := range ego.getVal() {
		result.Add(function(item))
	}
	return result
}

func (ego *sliceList[T]) Reduce(initial T, function func(T, T) T) T {
	ego.assert()
	result := initial
	for _, item := range ego.getVal() {
		result = function(result, item)
	}
	return result
}

func (ego *sliceList[T]) Filter(function func(T) bool) List[T] {
	ego.assert()
	result := NewList[T]()
	for _, item := range ego.getVal() {
		if function(item) {
			result.Add(item)
		}
	}
	return result
}

func (ego *sliceList[T]) Sort() List[T] {
	ego.assert()
	switch val := any(ego.getVal()).(type) {
	case []string:
		sort.Strings(val)
	case []int:
		sort.Ints(val)
	case []float64:
		sort.Float64s(val)
	default:
		panic("Unsortable list.")
	}
	return ego
}

func (ego *sliceList[T]) Min() float64 {
	if ego.Empty() {
		return 0
	}
	min := math.MaxFloat64
	switch val := any(ego.getVal()).(type) {
	case []int:
		for _, item := range val {
			float := float64(item)
			if float < min {
				min = float
			}
		}
	case []float64:
		for _, item := range val {
			if item < min {
				min = item
			}
		}
	default:
		panic("The list is not numeric.")
	}
	return min
}

func (ego *sliceList[T]) Max() float64 {
	if ego.Empty() {
		return 0
	}
	max := -math.MaxFloat64
	switch val := any(ego.getVal()).(type) {
	case []int:
		for _, item := range val {
			float := float64(item)
			if float > max {
				max = float
			}
		}
	case []float64:
		for _, item := range val {
			if item > max {
				max = item
			}
		}
	default:
		panic("The list is not numeric.")
	}
	return max
}

func (ego *sliceList[T]) Sum() float64 {
	if ego.Empty() {
		return 0
	}
	var sum float64
	switch val := any(ego.getVal()).(type) {
	case []int:
		for _, item := range val {
			sum += float64(item)
		}
	case []float64:
		for _, item := range val {
			sum += item
		}
	default:
		panic("The list is not numeric.")
	}
	return sum
}

func (ego *sliceList[T]) Prod() float64 {
	if ego.Empty() {
		return 0
	}
	var prod float64 = 1
	switch val := any(ego.getVal()).(type) {
	case []int:
		for _, item := range val {
			prod *= float64(item)
		}
	case []float64:
		for _, item := range val {
			prod *= item
		}
	default:
		panic("The list is not numeric.")
	}
	return prod
}

func (ego *sliceList[T]) Avg() float64 {
	return ego.Sum() / float64(ego.Count())
}
