package bitcask_go

import (
	"bitcask-go/utils"
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDB_ListKey(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-list-keys")
	opts.DirPath = dir
	t.Log(dir)
	db, err := Open(opts)
	defer os.RemoveAll(dir)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//数据库为空
	keys := db.ListKeys()
	t.Log(len(keys))
	assert.Equal(t, 0, len(keys))

	//只有一条数据
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(20))
	assert.Nil(t, err)
	keys2 := db.ListKeys()
	assert.Equal(t, 1, len(keys2))

	//有多条数据
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(33), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(44), utils.RandomValue(20))
	assert.Nil(t, err)
	assert.Nil(t, err)
	keys3 := db.ListKeys()
	assert.Equal(t, 4, len(keys3))
	for _, k := range keys3 {
		assert.NotNil(t, k)
	}
}

func TestDB_Fold(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-list-fold")
	opts.DirPath = dir
	t.Log(dir)
	db, err := Open(opts)
	defer os.RemoveAll(dir)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//数据库为空
	keys := db.ListKeys()
	t.Log(len(keys))
	assert.Equal(t, 0, len(keys))

	err = db.Put(utils.GetTestKey(11), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(44), utils.RandomValue(20))
	assert.Nil(t, err)

	err = db.Fold(func(key []byte, value []byte) bool {
		assert.NotNil(t, key)
		assert.NotNil(t, value)
		if bytes.Compare(key, utils.GetTestKey(22)) == 0 {
			return false
		}
		return true
	})
	assert.Nil(t, err)
}

func TestDB_Close(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-list-close")
	opts.DirPath = dir
	t.Log(dir)
	db, err := Open(opts)
	defer os.RemoveAll(dir)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.RandomValue(20))
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_Sync(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-list-sync")
	opts.DirPath = dir
	t.Log(dir)
	db, err := Open(opts)
	defer os.RemoveAll(dir)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.RandomValue(20))
	assert.Nil(t, err)

	err = db.Sync()
	assert.Nil(t, err)
}

func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.Close()
		}
		err := os.Remove(db.options.DirPath)
		if err != nil {
		}
	}
}

func TestDB_FileLock(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-filelock")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	_, err = Open(opts)
	db.Close()
	db2, err := Open(opts)
	t.Log(err)
	db2.Close()
}

func TestDB_Stat(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-stat")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	_, err = Open(opts)
	db.Close()
	db2, err := Open(opts)
	t.Log(err)
	db2.Close()

	for i := 100; i < 200; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(120))
		assert.Nil(t, err)
	}
	for i := 100; i < 200; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	stat := db.Stat()
	t.Log(stat)
	t.Log(db.reclaimSize)
}

func TestDB_Merge(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "tmp/bitcask-go"
	opts.MMapAtStartup = false
	now := time.Now()
	db, err := Open(opts)
	t.Log("open time", time.Since(now))
	assert.Nil(t, err)
	assert.NotNil(t, db)
}
func TestDB_BackUp(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-backup")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	for i := 1; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(120))
		assert.Nil(t, err)
	}
	backupDir, _ := os.MkdirTemp("", "bitcask-go-backup-test")
	err = db.BackUp(backupDir)
	assert.Nil(t, err)
	opts1 := DefaultOptions
	opts1.DirPath = backupDir
	db2, err := Open(opts1)
	defer destroyDB(db2)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
}
