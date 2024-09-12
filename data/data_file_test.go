package data

import (
	"bitcask-go/fio"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenDataFile(t *testing.T) {
	fileName1, err := OpenDataFile(os.TempDir(), 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, fileName1)
	t.Log(os.TempDir())

	fileName2, err := OpenDataFile(os.TempDir(), 111, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, fileName2)
	t.Log(os.TempDir())

	fileName3, err := OpenDataFile(os.TempDir(), 111, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, fileName3)
	t.Log(os.TempDir())
}

func TestDataFile_Write(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = dataFile.Write([]byte("bbbb"))
	assert.Nil(t, err)

	err = dataFile.Write([]byte("ccc"))
	assert.Nil(t, err)

	err = dataFile.Write([]byte("ddd"))
	assert.Nil(t, err)
}
func TestDataFile_Close(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 123, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = dataFile.Close()
	assert.Nil(t, err)
}
func TestDataFile_Sync(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = dataFile.Sync()
	assert.Nil(t, err)
}
func TestDataFile_ReadLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 555, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	//只有一条LogRecord
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask kv go"),
	}
	res1, size1 := EncodeLogRecord(rec1)
	err = dataFile.Write(res1)
	assert.Nil(t, err)

	readRec1, readSize1, err := dataFile.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, rec1, readRec1)
	assert.Equal(t, size1, readSize1)
	t.Log(readSize1)
	//多条LogRecord从不同的位置读取
	rec2 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("a new value"),
	}
	res2, size2 := EncodeLogRecord(rec2)
	err = dataFile.Write(res2)
	assert.Nil(t, err)

	readRec2, readSize2, err := dataFile.ReadLogRecord(size1)
	assert.Nil(t, err)
	assert.Equal(t, rec2, readRec2)
	assert.Equal(t, size2, readSize2)
	//被删除的数据在文件的末尾
	rec3 := &LogRecord{
		Key:   []byte("1"),
		Value: []byte(""),
		Type:  LogRecordDeleted,
	}
	res3, size3 := EncodeLogRecord(rec3)
	err = dataFile.Write(res3)
	assert.Nil(t, err)
	t.Log(size3)

	readRec3, readSize3, err := dataFile.ReadLogRecord(size1 + size2)
	assert.Nil(t, err)
	assert.Equal(t, rec3, readRec3)
	assert.Equal(t, size3, readSize3)
}
