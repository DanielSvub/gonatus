package fs_test

import (
	"io"
	"testing"
	"time"

	. "github.com/SpongeData-cz/gonatus/fs"
	. "github.com/SpongeData-cz/gonatus/fs/drivers"
)

func containsPath(slice []File, path Path) bool {
	for _, elem := range slice {
		if elem.Path().Equals(path) {
			return true
		}
	}
	return false
}

func TestPath(t *testing.T) {

	p1 := Path{"a", "b"}
	p2 := Path{"c", "d"}

	if !p1.Equals(p1) || p1.Equals(p2) {
		t.Fatal("Path equality test does not work.")
	}

	p := p1.Join(p2)
	if !p.Equals(Path{"a", "b", "c", "d"}) {
		t.Error("Path join failed.")
	}

	if p.Base() != "d" {
		t.Error("Invalid filename.")
	}
	if !p.Dir().Equals(Path{"a", "b", "c"}) {
		t.Error("Invalid directory path.")
	}

	c := p.Clone()
	if !c.Equals(Path{"a", "b", "c", "d"}) {
		t.Error("Path clone failed.")
	}

}

func TestStorage(t *testing.T) {

	var storage Storage
	var sid StorageId

	setup := func() {

		storage = NewLocalStorage(LocalStorageConf{Prefix: "/tmp/storage"})
		GStorageManager.RegisterStorage(storage)
		sid, _ = GStorageManager.GetId(storage)

		// /a/
		NewFile(FileConf{Path: Path{"a"}, StorageId: sid}).MkDir()

		// /b/
		NewFile(FileConf{Path: Path{"b"}, StorageId: sid}).MkDir()

		// /a/c/
		NewFile(FileConf{
			Path:      Path{"a", "c"},
			StorageId: sid,
		}).MkDir()

		// /a/c/d/
		NewFile(FileConf{
			Path:      Path{"a", "c", "d"},
			StorageId: sid,
		}).MkDir()

		// /a/c/d/file
		file := NewFile(FileConf{
			Path:      Path{"a", "c", "d", "file"},
			StorageId: sid,
		})

		if err := file.Open(ModeWrite); err != nil {
			t.Error(err)
		}
		if err := file.Close(); err != nil {
			t.Error(err)
		}

	}

	cleanup := func() {
		GStorageManager.UnregisterStorage(storage)
		storage.Commit()
		storage.Clear()
	}

	t.Run("tree", func(t *testing.T) {

		setup()

		unlimited, err := storage.Tree(DepthUnlimited)
		if err != nil {
			t.Error(err)
		}
		if res, err := unlimited.Collect(); err != nil {
			t.Error(err)
		} else if len(res) != 6 {
			t.Error("Wrong number of files in the unlimited tree.")
		} else if !(containsPath(res, Path{}) &&
			containsPath(res, Path{"a"}) &&
			containsPath(res, Path{"b"}) &&
			containsPath(res, Path{"a", "c"}) &&
			containsPath(res, Path{"a", "c", "d"}) &&
			containsPath(res, Path{"a", "c", "d", "file"})) {
			t.Error("Missing file(s) in the unlimited tree.")
		}

		ls, err := storage.Tree(DepthLs)
		if err != nil {
			t.Error(err)
		}
		if res, err := ls.Collect(); err != nil {
			t.Error(err)
		} else if len(res) != 3 {
			t.Error("Wrong number of files in LS.")
		} else if !(containsPath(res, Path{}) &&
			containsPath(res, Path{"a"}) &&
			containsPath(res, Path{"b"})) {
			t.Error("Missing file in LS.")
		}

		limit, err := storage.Tree(2)
		if err != nil {
			t.Error(err)
		}
		if res, err := limit.Collect(); err != nil {
			t.Error(err)
		} else if len(res) != 4 {
			t.Error("Wrong number of files in the limited tree.")
		} else if !(containsPath(res, Path{}) &&
			containsPath(res, Path{"a"}) &&
			containsPath(res, Path{"b"}) &&
			containsPath(res, Path{"a", "c"})) {
			t.Error("Missing file(s) in the limited tree.")
		}

		cleanup()

	})

	t.Run("merge", func(t *testing.T) {

		setup()

		copy := NewLocalStorage(LocalStorageConf{Prefix: "/tmp/storage2"})
		GStorageManager.RegisterStorage(copy)

		if err := copy.Merge(storage); err != nil {
			t.Error(err)
		}

		unlimited, err := copy.Tree(DepthUnlimited)
		if err != nil {
			t.Error(err)
		}
		if res, err := unlimited.Collect(); err != nil {
			t.Error(err)
		} else if len(res) != 6 {
			t.Error("Wrong number of files in the destination storage.")
		} else if !(containsPath(res, Path{}) &&
			containsPath(res, Path{"a"}) &&
			containsPath(res, Path{"b"}) &&
			containsPath(res, Path{"a", "c"}) &&
			containsPath(res, Path{"a", "c", "d"}) &&
			containsPath(res, Path{"a", "c", "d", "file"})) {
			t.Error("Missing file(s) in the destination storage.")
		}

		GStorageManager.UnregisterStorage(copy)
		copy.Commit()
		copy.Clear()

		cleanup()

	})

}

func TestFile(t *testing.T) {

	var storage1 Storage
	var sid1 StorageId

	var storage2 Storage
	var sid2 StorageId

	setup := func() {

		storage1 = NewLocalStorage(LocalStorageConf{Prefix: "/tmp/storage"})
		GStorageManager.RegisterStorage(storage1)
		sid1, _ = GStorageManager.GetId(storage1)

		storage2 = NewLocalStorage(LocalStorageConf{Prefix: "/tmp/storage2"})
		GStorageManager.RegisterStorage(storage2)
		sid2, _ = GStorageManager.GetId(storage2)

		// /a/
		NewFile(FileConf{Path: Path{"a"}, StorageId: sid1}).MkDir()

		// /b/
		NewFile(FileConf{Path: Path{"b"}, StorageId: sid1}).MkDir()

		// /a/c/
		NewFile(FileConf{
			Path:      Path{"a", "c"},
			StorageId: sid1,
		}).MkDir()

		// /a/c/file
		file := NewFile(FileConf{
			Path:      Path{"a", "c", "file"},
			StorageId: sid1,
		})

		if err := file.Open(ModeWrite); err != nil {
			t.Error(err)
		}

		if err := file.Close(); err != nil {
			t.Error(err)
		}

	}

	cleanup := func() {

		storage1.Commit()
		storage1.Clear()
		GStorageManager.UnregisterStorage(storage1)

		storage2.Commit()
		storage2.Clear()
		GStorageManager.UnregisterStorage(storage2)

	}

	t.Run("fcopy", func(t *testing.T) {

		setup()

		file := NewFile(FileConf{Path: Path{"a", "c", "file"}, StorageId: sid1})
		copy := NewFile(FileConf{Path: Path{"b", "copy"}, StorageId: sid1})
		if err := file.Copy(copy); err != nil {
			t.Error(err)
		}

		interStorageCopy := NewFile(FileConf{Path: Path{"d", "copy"}, StorageId: sid2})
		if err := file.Copy(interStorageCopy); err != nil {
			t.Error(err)
		}

		s1Tree, err := storage1.Tree(DepthUnlimited)
		if err != nil {
			t.Error(err)
		}
		if res, err := s1Tree.Collect(); err != nil {
			t.Error(err)
		} else if len(res) != 6 {
			t.Error("Wrong number of files in the original storage.")
		} else if !(containsPath(res, Path{}) &&
			containsPath(res, Path{"a"}) &&
			containsPath(res, Path{"b"}) &&
			containsPath(res, Path{"a", "c"}) &&
			containsPath(res, Path{"a", "c", "file"}) &&
			containsPath(res, Path{"b", "copy"})) {
			t.Error("Missing file(s) in the original storage.")
		}

		s2Tree, err := storage2.Tree(DepthUnlimited)
		if err != nil {
			t.Error(err)
		}
		if res, err := s2Tree.Collect(); err != nil {
			t.Error(err)
		} else if len(res) != 3 {
			t.Error("Wrong number of files the destination storage.")
		} else if !(containsPath(res, Path{}) &&
			containsPath(res, Path{"d"}) &&
			containsPath(res, Path{"d", "copy"})) {
			t.Error("Missing file(s) in the destination storage.")
		}

		cleanup()

	})

	t.Run("dcopy", func(t *testing.T) {

		setup()

		dir := NewFile(FileConf{Path: Path{"a"}, StorageId: sid1})

		interStorageCopy := NewFile(FileConf{Path: Path{"b", "copy"}, StorageId: sid2})
		if err := dir.Copy(interStorageCopy); err != nil {
			t.Error(err)
		}

		s1Tree, err := storage1.Tree(DepthUnlimited)
		if err != nil {
			t.Error(err)
		}
		if res, err := s1Tree.Collect(); err != nil {
			t.Error(err)
		} else if len(res) != 5 {
			t.Error("Wrong number of files in the original storage.")
		} else if !(containsPath(res, Path{}) &&
			containsPath(res, Path{"a"}) &&
			containsPath(res, Path{"b"}) &&
			containsPath(res, Path{"a", "c"}) &&
			containsPath(res, Path{"a", "c", "file"})) {
			t.Error("Missing file(s) in the original storage.")
		}

		s2Tree, err := storage2.Tree(DepthUnlimited)
		if err != nil {
			t.Error(err)
		}
		if res, err := s2Tree.Collect(); err != nil {
			t.Error(err)
		} else if len(res) != 5 {
			t.Error("Wrong number of files in the destination storage.")
		} else if !(containsPath(res, Path{}) &&
			containsPath(res, Path{"b"}) &&
			containsPath(res, Path{"b", "copy"}) &&
			containsPath(res, Path{"b", "copy", "c"}) &&
			containsPath(res, Path{"b", "copy", "c", "file"})) {
			t.Error("Missing file(s) in the destination storage.")
		}

		cleanup()

	})

	t.Run("move", func(t *testing.T) {

		setup()

		file := NewFile(FileConf{Path: Path{"a", "c", "file"}, StorageId: sid1})
		moved := NewFile(FileConf{Path: Path{"b", "moved"}, StorageId: sid1})
		if err := file.Move(moved); err != nil {
			t.Error(err)
		}

		interStorageMove := NewFile(FileConf{Path: Path{"d", "moved"}, StorageId: sid2})
		if err := file.Move(interStorageMove); err != nil {
			t.Error(err)
		}

		s1Tree, err := storage1.Tree(DepthUnlimited)
		if err != nil {
			t.Error(err)
		}
		if res, err := s1Tree.Collect(); err != nil {
			t.Error(err)
		} else if len(res) != 4 {
			t.Error("Wrong number of files in the original storage.")
		} else if !(containsPath(res, Path{}) &&
			containsPath(res, Path{"a"}) &&
			containsPath(res, Path{"b"}) &&
			containsPath(res, Path{"a", "c"})) {
			t.Error("Missing file(s) in the original storage.")
		}

		s2Tree, err := storage2.Tree(DepthUnlimited)
		if err != nil {
			t.Error(err)
		}
		if res, err := s2Tree.Collect(); len(res) != 3 || err != nil {
			t.Error("Move failed.")
		} else if !(containsPath(res, Path{}) &&
			containsPath(res, Path{"d"}) &&
			containsPath(res, Path{"d", "moved"})) {
			t.Error("Missing file(s) in the destination storage.")
		}

		cleanup()

	})

	t.Run("tree", func(t *testing.T) {

		setup()

		file := NewFile(FileConf{Path: Path{"a"}, StorageId: sid1})

		unlimited, err := file.Tree(DepthUnlimited)
		if err != nil {
			t.Error(err)
		}
		if res, err := unlimited.Collect(); err != nil {
			t.Error()
		} else if len(res) != 3 {
			t.Error("Wrong number of files in the unlimited tree.")
		} else if !(containsPath(res, Path{"a"}) &&
			containsPath(res, Path{"a", "c"}) &&
			containsPath(res, Path{"a", "c", "file"})) {
			t.Error("Missing file(s) in the unlimited tree.")
		}

		ls, err := file.Tree(DepthLs)
		if err != nil {
			t.Error(err)
		}
		if res, err := ls.Collect(); err != nil {
			t.Error(err)
		} else if len(res) != 2 {
			t.Error("Wrong number of files in LS.")
		} else if !(containsPath(res, Path{"a"}) &&
			containsPath(res, Path{"a", "c"})) {
			t.Error("Missing file(s) in the unlimited tree.")
		}

		cleanup()

	})

	t.Run("time", func(t *testing.T) {

		setup()

		file := NewFile(FileConf{
			Path:      Path{"a", "c", "file"},
			StorageId: sid1,
		})

		time := time.Unix(0, 0)
		file.SetOrigTime(time)

		if stat, err := file.Stat(); err != nil {
			t.Error(err)
		} else if stat.OrigTime != time {
			t.Error("Time setting failed.")
		}

		cleanup()

	})

	t.Run("rws", func(t *testing.T) {

		setup()

		input := []byte("test")
		output := make([]byte, 4)

		file := NewFile(FileConf{
			Path:      Path{"a", "c", "file"},
			StorageId: sid1,
		})

		if err := file.Open(ModeRW); err != nil {
			t.Fatal(err)
		}
		if n, err := file.Write(input); err != nil {
			t.Error(err)
		} else if n != 4 {
			t.Error("Write failed.")
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			t.Error(err)
		}
		if n, err := file.Read(output); err != nil {
			t.Error(err)
		} else if n != 4 {
			t.Error("Read failed.")
		}
		if err := file.Close(); err != nil {
			t.Error(err)
		}

		for i := range input {
			if input[i] != output[i] {
				t.Fatal("Input is not equal to output.")
			}
		}

		cleanup()

	})

	t.Run("conf", func(t *testing.T) {

		setup()

		conf := FileConf{
			Path:      Path{"a", "c", "file"},
			StorageId: sid1,
			Flags:     FileContent | FileTopology,
		}
		file := NewFile(conf)
		serialized := file.Serialize().(FileConf)

		if !file.Path().Equals(serialized.Path) {
			t.Error("Path does not match.")
		}
		if file.Storage().Id() != serialized.StorageId {
			t.Error("Storage ID does not match.")
		}
		if stat, err := file.Stat(); err != nil {
			t.Error(err)
		} else if stat.Flags != serialized.Flags {
			t.Error("Flags do not match.")
		}

		cleanup()

	})

}
