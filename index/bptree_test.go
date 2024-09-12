package index

import (
	"bitcask-go/data"
	"os"
	"path/filepath"
	"testing"
)

func TestNewPlusTree_Put(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-put")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.Remove(path)
	}()
	t.Log(path)
	tree := NewBPlusTree(path, false)
	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 9999})
	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 9999})
	tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 123, Offset: 9999})
}

func TestNewPlusTree_Get(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-get")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.Remove(path)
	}()
	t.Log(path)
	tree := NewBPlusTree(path, false)

	pos := tree.Get([]byte("not exist"))
	t.Log(pos)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 9999})
	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 9999})
	tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 123, Offset: 9999})
	pos = tree.Get([]byte("aac"))
	t.Log(pos)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 9884, Offset: 1232})
	pos = tree.Get([]byte("aac"))
	t.Log(pos)
}

func TestBPlusTree_Delete(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-delete")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.Remove(path)
	}()
	tree := NewBPlusTree(path, false)
	res := tree.Delete([]byte("not exist"))
	t.Log(res)

	tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 123, Offset: 9999})
	pos := tree.Get([]byte("acc"))
	t.Log(pos)
	res = tree.Delete([]byte("acc"))
	t.Log(res)
}

func TestBPlusTree_Size(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-size")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.Remove(path)
	}()
	tree := NewBPlusTree(path, false)
	t.Log(tree.Size())

	tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 123, Offset: 9999})
	t.Log(tree.Size())
}

func TestBptree_Iterator(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-iter")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.Remove(path)
	}()
	t.Log(path)
	tree := NewBPlusTree(path, false)

	pos := tree.Get([]byte("not exist"))
	t.Log(pos)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 9999})
	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 9999})
	tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 123, Offset: 9999})
	iter := tree.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		t.Log(string(iter.Key()))
	}
	iter = tree.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		t.Log(string(iter.Key()))
	}
}
