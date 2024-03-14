package fs_test

import (
	"io"
	"testing"
	"time"

	. "github.com/DanielSvub/gonatus/fs"
	. "github.com/DanielSvub/gonatus/fs/driver"
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

	setup := func() {

		storage = NewLocalCountedStorage(LocalCountedStorageConf{Prefix: "/tmp/storage"})
		GStorageManager.RegisterStorage(storage)

		// /a/
		NewFile(FileConf{Path: Path{"a"}, StorageId: storage.Id()}).MkDir()

		// /b/
		NewFile(FileConf{Path: Path{"b"}, StorageId: storage.Id()}).MkDir()

		// /a/c/
		NewFile(FileConf{
			Path:      Path{"a", "c"},
			StorageId: storage.Id(),
		}).MkDir()

		// /a/c/d/
		NewFile(FileConf{
			Path:      Path{"a", "c", "d"},
			StorageId: storage.Id(),
		}).MkDir()

		// /a/c/d/file
		file := NewFile(FileConf{
			Path:      Path{"a", "c", "d", "file"},
			StorageId: storage.Id(),
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

		copy := NewLocalCountedStorage(LocalCountedStorageConf{Prefix: "/tmp/storage2"})
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

		GStorageManager.UnregisterStorage(storage)
		copy.Commit()
		copy.Clear()

		cleanup()

	})

}

func TestFile(t *testing.T) {

	var storage1 Storage
	var storage2 Storage

	setup := func() {

		storage1 = NewLocalCountedStorage(LocalCountedStorageConf{Prefix: "/tmp/storage"})
		GStorageManager.RegisterStorage(storage1)

		storage2 = NewLocalCountedStorage(LocalCountedStorageConf{Prefix: "/tmp/storage2"})
		GStorageManager.RegisterStorage(storage2)

		// /a/
		NewFile(FileConf{Path: Path{"a"}, StorageId: storage1.Id()}).MkDir()

		// /b/
		NewFile(FileConf{Path: Path{"b"}, StorageId: storage1.Id()}).MkDir()

		// /a/c/
		NewFile(FileConf{
			Path:      Path{"a", "c"},
			StorageId: storage1.Id(),
		}).MkDir()

		// /a/c/file
		file := NewFile(FileConf{
			Path:      Path{"a", "c", "file"},
			StorageId: storage1.Id(),
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

		file := NewFile(FileConf{Path: Path{"a", "c", "file"}, StorageId: storage1.Id()})
		copy := NewFile(FileConf{Path: Path{"b", "copy"}, StorageId: storage1.Id()})
		if err := file.Copy(copy); err != nil {
			t.Error(err)
		}

		interStorageCopy := NewFile(FileConf{Path: Path{"d", "copy"}, StorageId: storage2.Id()})
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

		dir := NewFile(FileConf{Path: Path{"a"}, StorageId: storage1.Id()})

		interStorageCopy := NewFile(FileConf{Path: Path{"b", "copy"}, StorageId: storage2.Id()})
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

		file := NewFile(FileConf{Path: Path{"a", "c", "file"}, StorageId: storage1.Id()})
		moved := NewFile(FileConf{Path: Path{"b", "moved"}, StorageId: storage1.Id()})
		if err := file.Move(moved); err != nil {
			t.Error(err)
		}

		interStorageMove := NewFile(FileConf{Path: Path{"d", "moved"}, StorageId: storage2.Id()})
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

		file := NewFile(FileConf{Path: Path{"a"}, StorageId: storage1.Id()})

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
			StorageId: storage1.Id(),
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
			StorageId: storage1.Id(),
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
			StorageId: storage1.Id(),
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
