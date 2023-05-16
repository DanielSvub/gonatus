# Gonatus streams

This package provides generic data streams with a lazy init capability. Data are not loaded until they are needed, which allowes to process a large amount of data with constant space complexity. To operate, the streams have to be connected together, creating a data flow called *pipe*. The processing is then initiated by reading from the end of the pipe.

Each stream can be in two states - open or closed. The stream is closed, when there is no more data to read. The closed state propagates from the start to the end of the pipe, until the output is closed, what makes the whole process to end.

All streams inplement at least one of these two main interfaces:

- **InputStreamer -** A stream to which an output stream can be attached,
- **OutputStreamer -** A stream which can be attached to an input stream.

Some types of streams implement both of them. These are called *two-sided* streams and are used to modify the data. There are five of them:

- **Transform stream -** transforms a value of each item,
- **Filter stream -** discards items not safisfying a certain condition,
- **Duplication stream -** creates two streams identical to the source stream,
- **Split stream -** splits one stream into two based on a certain condition,
- **Merge stream -** merges multiple streams into one.

## Usage

### Input streams

Input streams read data from the source.

#### BufferInputStream

In this case, the source is a buffer (implemented as a channel) of defined capacity. The data are passed to the stream by *Write* method. The method can be called multiple times. If the buffer is full, the program waits for some space to be freed. When the writing is done, the stream has to be manually closed.

```go
is := streams.NewBufferInputStream[int](3)
is.Write(1, 2, 3)
is.Close()
```

#### NdjsonInputStream

Here, the source is a `.ndjson` file. It is opened when the stream is created and closed automatically, when the last row is read. Attempting to open a non-existing file causes a panic.

```go
is := streams.NewNdjsonInputStream("example.ndjson")
```

### Output streams

Output stream terminates the pipe and exports the data to some destination. The stream has to be attached to a source of the data with *Pipe* method, then the processing can be initiated (the exact way to do this differs from one stream to another).

#### ReadableOutputStream

In this case, the destination is a slice. Two methods can be used to initiate the processing: *Read*, which reads concrete amount of items to a precreated slice, and *Collect*, which reads until the source stream is closed and then returns the result. When no data to read (but the source is open), the stream waits.

```go
os := NewReadableOutputStream[int]()
is.Pipe(os)
result := make([]int, 3)
n, err := os.Read(result)
```

```go
os := NewReadableOutputStream[int]()
is.Pipe(os)
result, err := os.Collect()
```

#### NdjsonOutputStream

Here, the destination is a `.ndjson` file. The file can be open in two modes - *FileWrite* (overwrites the file if it already exists) or *FileAppend* (appends to the end of the file if exists). The file is opened when the stream is created and closed automatically, when the stream is closed. The processing is initiated with the *Run* method.

```go
os := streams.NewNdjsonOutputStream("example.ndjson", streams.FileWrite)
is.Pipe(os)
os.Run()
```

```go
os := streams.NewNdjsonOutputStream("example.ndjson", streams.FileAppend)
is.Pipe(os)
os.Run()
```

### Two-sided streams

#### Transform stream

The transform stream works as a *map* method in many programming languages. Each item of the stream is modified by the given transformation function. The output can be of a different type than the input.

```go
ts := streams.NewTransformStream(func(x int) int {
    return x * x
})
is.Pipe(ts).Pipe(os)
```

#### Filter stream

The filter stream simply filters the data by dropping all items for which the given function returns false.

```go
fs := streams.NewFilterStream(func(x int) bool {
	return x <= 2
})
is.Pipe(fs).Pipe(os)
```

#### Duplication stream

The duplication stream clones its source stream. It contains two BufferInputStreams that are filled automatically when the stream is attached to a source. The duplication is performed using *Duplicate* method on an InputStreamer.

```go
ds := streams.NewDuplicationStream[int](3)
d1, d2 := is.Duplicate(ds)
d1.Pipe(os1)
d2.Pipe(os2)
```

#### Split stream

The split stream is similar to duplication stream, but each item is written to only one of the nested BufferInputStreams, depending on whether it satisfies the given condition or not. Splitting is performed by calling the *Split* method.

```go
ss := streams.NewSplitStream(3, func(x int) bool {
	return x <= 2
})
s1, s2 := is.Split(ss)
s1.Pipe(os1)
s2.Pipe(os2)
```

#### Merge stream

The merge stream merges multiple streams into a single one. It can have various implementations, the most primitive is the round robin merging, which simply cycles between sources after each loaded item. It can be configured to close automatically (after closing of all currently attached sources) or manually.

```go
ms := NewRRMergeStream[int](true)
is1.Pipe(ms)
is2.Pipe(ms)
ms.Pipe(os)
```

## Examples

1. Computes a square of three numbers. The result will be [1, 4, 9].

```go
is := streams.NewBufferInputStream[int](3)
ts := streams.NewTransformStream(func(x int) int {
    return x * x
})
os := streams.NewReadableOutputStream[int]()

is.Write(1, 2, 3)
is.Close()
is.Pipe(ts).Pipe(os)

result, err := os.Collect()
```

2. Parallelly creates a million of numbers and prints them increased by 1. Only one integer is stored in memory at the time.

```go
is := streams.NewBufferInputStream[int](1)
ts := streams.NewTransformStream(func(x int) int {
    return x + 1
})
os := streams.NewReadableOutputStream[int]()

var wg sync.WaitGroup
wg.Add(2)

write := func() {
    defer wg.Done()
    defer is.Close()
    for i := 0; i < 100; i++ {
        is.Write(i)
    }
}

is.Pipe(ts).Pipe(os)

read := func() {
    defer wg.Done()
    result := make([]int, 1)
    for true {
        n, err := os.Read(result)
        if n != 1 {
            break
        }
        if err != nil {
            panic(err)
        }
        fmt.Println(result[0])
    }
}

go write()
go read()
wg.Wait()
```

3. Copies the `.ndjson` file.

```go
is := streams.NewNdjsonInputStream("original.ndjson")
os := streams.NewNdjsonOutputStream("copy.ndjson", streams.FileWrite)
is.Pipe(os)
os.Run()
```
