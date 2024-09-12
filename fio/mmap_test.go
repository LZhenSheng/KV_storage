package fio

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMMap_Read(t *testing.T) {
	path := filepath.Join("/tmp", "mmap-a.data")
	defer destoryFile(path)

	mmapIO, err := NewMapIOManager(path)
	assert.Nil(t, err)
	//文件为空
	b1 := make([]byte, 2)
	n1, err := mmapIO.Read(b1, 0)
	t.Log(n1)
	t.Log(err)

	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)
	_, err = fio.Write([]byte("dlkjf"))
	assert.Nil(t, err)
	fio.Close()

	mmapIO2, err := NewMapIOManager(path)
	t.Log(mmapIO2.Size())
	assert.Nil(t, err)
	//文件为空
	n1, err = mmapIO2.Read(b1, 0)
	t.Log(n1)
	assert.Nil(t, err)
}
