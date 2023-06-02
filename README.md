# Gonatus
Gonatus is an opensource Golang library for highly scalable environments for unstructured, structured and relational data analysis.

# Gonatus Basics

## Gonatus System
While Gonatus is meant to be Big Data framework for large scalable environments, we define designed those areas:
  * objective system with functional serialization,
  * lazy streams,
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
We define Pseudo-Objective system for Golang (however not recommended by Golang to say objective system).

We use instead of `self` or `this` receiver the `ego` naming (however not recommended by Golang).

## Objective system
To be a valid Gobject you have to implement `Gobjecter` interface and you have to include `Gobject` struct to the first line of your struct fields.

The Constructor for a class A must be in form of  `func NewA(AConf) *A` or `func NewA(AConf) A` if class A is interface. `AConf` is structure containing Golang atomic types (numbers, strings, structures, serializable slices, arrays and maps). Pointers, instances, interfaces are not allowed.

Constructors must set field `Gobject.CLASS` to `"A"`.

Constructors may call `a.Init()` meant to be object intialization procedure where `a` is instance of `A`.

For those purpose every class X must implement `serialize()` and `NewX()` leading to object creation according to its serialized form.

### Serialization
Serialize/deserialize scheme as follows:

```go
// let a be a Gonatus object A be its class
NewA(a.serialize()).serialize() == a.serialize()
```

This serialization principle is defined as so for transparent instance transferring accross the whole cluster and its materialization elsewhere within the cluster.

### Example
```go
type MammalConf struct {
  Weight int
  Size int
}

type Mammal struct {
  Gobject
  Gobjecter
  params MammalConf
}

func MammalNew(conf MammalConf) *Mammal {
  ego := new(Mammal{params: conf})
  ego.Gobject.CLASS="Mammal"
  return ego
}

func (ego *Mammal) Serialize() MammalConf {
  return ego.params
}

type DogConf struct {
  MammalConf
  Name string
  Age int
}

type Dog struct {
  Gobject
  Gobjecter
  params DogConf
  yearOfBirth int
}

func DogNew(conf DogConf) *Dog {
  ego := new(Dog{params: conf})

  ego.Gobject.CLASS="Dog"
  ego.yearOfBith := time.Now().Year() - conf.Age

  return ego
}

func (ego *Dog) Serialize() Dog {
  // Recompute DogConf if something happened in the meanwhile...
  return ego.params
}

func (ego *Dog) Woof() Dog {
  fmt.Println("Woof")
}
```

## The Lazy Streams
Lazy streams are implemented due to ability of perfomance management during the stream lifetime.

### Stream Node Types
  * Buffered Stream
  * Input Stream
  * Output Stream
  * Transform Stream
  * Filter Stream
  * Merge Stream
  * Split Stream

### Semantics

### Interfaces

### Functions
  * `pull`
  * `collect`

### Writing Custom Streams

### Example

## Reinvented File System
Due to classic filesystem limitations (max path, max name size, unicode problems, reserved characters, inode count, ...) we redefined classical Filesystem. First change in comparation to classical filesystem is that we recognize multiple states of entries - undetermined, topology, content, topology & content. Due to this file may also have children (as only directory can in normal fs) and directory may also hold a content. So we may expand archives for example into the original file topology.

### Record types
  * `undetermined` - Unknown reserved kind of a record - state when the record is empty and have no topology.
  * `topology` - Kind when the record may hold some children (directory in standard fs).
  * `content` - When record holds also a binary content.
  * `topology|conent` - When record holds topology and content as well.


### Limitations
Design of the filesystem moves limitation behind technical possibilities of compute machines generaly.
  * Path length - Limited by RAM size.
  * Path forbidden characters  - no (also `/`, `\`, ... may be used!).
  * Records count - Limitations according to the Storage collection used - near to mmap limits usually.
  * Record content size - unlimited.
  * Record topology size - unlimited - listing is done via the Lazy Streams.

### Path
Path is internally defined as `[]string` so it is just a slice of any valid strings, each slice record is meant to be a name of path level. For example `/home/foo/bar/c.bin` is represented as `["home", "foo", "bar", "c.bin"]`. The advantage is that all slice operations may be used.

### Classes
#### Storages
  * NativeStorage
  * LocalStorage

#### StorageManager

#### File
File is ...
The conf:
```go
type FileConf struct {
	StorageId StorageId
	Path      Path
	Name      string
	Flags     FileFlags
}
```

### Operations
  * `FileNew()`
  * `Path()`
  * `Name()`
  * `Fullpath()`
  * `Copy()`
  * `Move()`
  * `Delete()`
  * `Open()`
  * `Stat()`
  * `io.Reader`
	* `io.Writer`
	* `io.Seeker`
	* `io.Closer`
	* `io.ReaderFrom`

### Writing Custom Driver
You must implement the `StorageDriver`

```go
type StorageDriver interface {
	Open(path Path, name string, mode FileMode) (FileDescriptor, error)
	Close(path Path, name string) error

	MkDir(path Path, name string) error

	Copy(srcPath Path, srcName string, dstPath Path, dstName string) error
	Move(srcPath Path, srcName string, dstPath Path, dstName string) error
	Delete(path Path, name string) error

	Filter(path Path, depth Depth) (streams.ReadableOutputStreamer[File], error)
	Flags(path Path, name string) FileFlags

	Commit() error
	Clear() error
}
```

### Example

```go
storage := NewLocalStorage({Prefix: "/tmp/local2"})
GStorageManager.RegisterStorage(storage)

storage2 := NewLocalStorage({Prefix: "/tmp/local2"})
GStorageManager.RegisterStorage(storage2)

new := NewFile(
  FileConf{
    StorageId: storage.Id(),
    Location:  Path{"t"},
    Name:      "test.txt",
})

new2 := NewFile(
  FileConf{
    StorageId: storage2.Id(),
    Location:  Path{"t2"},
    Name:      "test2.txt",
})

_, err = new.Copy(new2)
```


