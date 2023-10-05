# Gonatus
Gonatus is an opensource Golang library for highly scalable environments for unstructured, structured, and relational data analysis.

## Gonatus System
While Gonatus is meant to be Big Data framework for large scalable environments, we cover these areas:
  * object system with functional serialization,
  * reinvented file system,
  * logging,
  * network service over various protocols (HTTP, TCP, unix sockets, ...),
  * services proxying user input into implementations based on the various protocols,
  * collection definitions for fast column based index search and manipulation with driver definitions for various technology (solr, elastic, ...),
  * cluster shared memory:
    - persistent object shared store,
    - unique identifier generation,
    - configuration store.
  * dynamic scaling of gonatus nodes with scheduling.

## Principles & Best Practices
We define Pseudo-Object system for Golang. Every structure implementing the `Gobjecter` interface is called a class. The structure can be private, then all interactions with it are made through an interface the structure implements. In this case, we speak about interface class.

Instead of `self` or `this`, we use `ego` as a receiver name (although uniform receiver names are not recommended in Golang).

## Object system
To be a valid Gobject, the structure has to implement `Gobjecter` interface and to embed `Gobject` struct.

The Constructor for a class A should be in form of `func NewA(AConf) *A` or `func NewA(AConf) A` if A is an interface class. `AConf` is structure containing Golang basic types (numbers, strings, simple structures, slices and maps). Pointers and Gobjects are not allowed.

### Serialization
Every Gonatus class has to be serializable into its own serialization structure, so that the following applies:
```go
// let a be a Gobject and A its class
NewA(a.serialize()).serialize() == a.serialize()
```

For this purpose, every class A must have a `Serialize() Conf` method (required by `Gobjecter` interface) and a constructor `NewA(AConf) A` has to exist. The serialization method returns an universal `Conf`, so to use it in the constructor, it has to be asserted as `AConf`.

This serialization principle is defined as so for transparent instance transferring accross the whole cluster and its materialization elsewhere within the cluster.

### Example
```go
type AnimalConf struct {
	Weight int
	Size   int
}

type Animal struct {
	weight int
	size   int
}

func (ego *Animal) Breathe() {
	fmt.Println("Inhale...")
	fmt.Println("Exhale...")
}

type DogConf struct {
	AnimalConf
	Name string
	Age  int
}

type Dog struct {
	gonatus.Gobject
	Animal
	name        string
	yearOfBirth int
}

func NewDog(conf DogConf) *Dog {
	ego := new(Dog)
	ego.weight = conf.Weight
	ego.size = conf.Size
	ego.name = conf.Name
	ego.yearOfBirth = time.Now().Year() - conf.Age
	return ego
}

func (ego *Dog) Serialize() gonatus.Conf {
	return DogConf{
		AnimalConf: AnimalConf{
			Weight: ego.weight,
			Size:   ego.size,
		},
		Name: ego.name,
		Age:  time.Now().Year() - ego.yearOfBirth,
	}
}

func (ego *Dog) Bark() {
	fmt.Println("Woof")
}
```

## Reinvented File System
Due to limitations of traditional file systems (max path legth, max name size, unicode problems, reserved characters, inode count, ...), we redefined the the architecture of a file system. The term "directory" does not exist, all records in the FS are called files. Each file may have content, topology (child records) or both (so files can also behave as directories). This way we can extract archives into the original file topology.

For more information, check `fs/README.md`.
