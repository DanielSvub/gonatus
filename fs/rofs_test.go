package fs_test

import (
	"testing"

	"github.com/SpongeData-cz/gonatus/errors"
	. "github.com/SpongeData-cz/gonatus/fs"
	. "github.com/SpongeData-cz/gonatus/fs/drivers"
)

func TestROStorage(t *testing.T) {

	var storage Storage
	var sid StorageId

	setup := func() {

		storage = NewNativeStorage(NativeStorageConf{Prefix: "/home/dan/GIT/gonatus/fs/fixtures"})
		GStorageManager.RegisterStorage(storage)
		sid, _ = GStorageManager.GetId(storage)

	}

	cleanup := func() {
		GStorageManager.UnregisterStorage(storage)
	}

	t.Run("tree", func(t *testing.T) {

		setup()

		unlimited, err := storage.Tree(DepthUnlimited)
		if err != nil {
			t.Error(err)
		}
		if res, err := unlimited.Collect(); err != nil {
			t.Error(err)
		} else if len(res) != 5 {
			t.Error("Wrong number of files in the unlimited tree.")
		} else if !(containsPath(res, Path{}) &&
			containsPath(res, Path{"a"}) &&
			containsPath(res, Path{"b"}) &&
			containsPath(res, Path{"a", "c"}) &&
			containsPath(res, Path{"a", "c", "file"})) {
			t.Error("Missing file(s) in the unlimited tree.")
		}

		ls, err := storage.Tree(DepthLs)
		if err != nil {
			t.Error(err)
		}
		if res, err := ls.Collect(); err != nil {
			t.Error(err)
		} else if len(res) != 3 {
			println(len(res))
			t.Error("Wrong number of files in LS.")
		} else if !(containsPath(res, Path{}) &&
			containsPath(res, Path{"a"}) &&
			containsPath(res, Path{"b"})) {
			t.Error("Missing file(s) in LS.")
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
		} else if len(res) != 5 {
			t.Error("Wrong number of files in the destination storage.")
		} else if !(containsPath(res, Path{}) &&
			containsPath(res, Path{"a"}) &&
			containsPath(res, Path{"b"}) &&
			containsPath(res, Path{"a", "c"}) &&
			containsPath(res, Path{"a", "c", "file"})) {
			t.Error("Missing file(s) in the destination storage.")
		}

		GStorageManager.UnregisterStorage(copy)
		copy.Commit()
		copy.Clear()

		cleanup()

	})

	t.Run("copy", func(t *testing.T) {

		setup()

		storage2 := NewLocalStorage(LocalStorageConf{Prefix: "/tmp/storage2"})
		GStorageManager.RegisterStorage(storage2)
		sid2, _ := GStorageManager.GetId(storage2)

		file := NewFile(FileConf{
			StorageId: sid,
			Path:      Path{"a", "c", "file"},
		})

		copy := NewFile(FileConf{
			StorageId: sid2,
			Path:      Path{"copy"},
		})

		if err := file.Copy(copy); err != nil {
			t.Error(err)
		}

		tree, err := storage2.Tree(DepthUnlimited)
		if err != nil {
			t.Error(err)
		}
		if res, err := tree.Collect(); err != nil {
			t.Error(err)
		} else if len(res) != 2 {
			t.Error("Wrong number of files in the destination storage.")
		} else if !(containsPath(res, Path{}) &&
			containsPath(res, Path{"copy"})) {
			t.Error("Missing file(s) in the destination storage.")
		}

		if err := copy.Open(ModeRead); err != nil {
			t.Error(err)
		}

		res := make([]byte, 12)

		if n, err := copy.Read(res); err != nil {
			t.Error(err)
		} else if n != 12 {
			t.Error("Wrong length of the file content.")
		} else if string(res) != "Sample text." {
			t.Error("Wrong file content.")
		}

		if err := copy.Close(); err != nil {
			t.Error(err)
		}

		GStorageManager.UnregisterStorage(storage2)
		storage2.Commit()
		storage2.Clear()

		cleanup()

	})

	t.Run("move", func(t *testing.T) {

		setup()

		storage2 := NewLocalStorage(LocalStorageConf{Prefix: "/tmp/storage2"})
		GStorageManager.RegisterStorage(storage2)
		sid2, _ := GStorageManager.GetId(storage2)

		file := NewFile(FileConf{
			StorageId: sid,
			Path:      Path{"a", "c", "file"},
		})

		moved := NewFile(FileConf{
			StorageId: sid2,
			Path:      Path{"moved"},
		})

		if err := file.Move(moved); err == nil || !errors.OfType(errors.Unwrap(err), errors.TypeNotImpl) {
			t.Error("Moved a file from read only storage.")
		}

		GStorageManager.UnregisterStorage(storage2)
		storage2.Commit()
		storage2.Clear()

		cleanup()

	})

}

/* func TestErr(t *testing.T) {
	file := NewFile(FileConf{Path: Path{"a"}})
	err := file.Close()
	println(errors.Traceback(err))
	t.Error(err)
}
*/
