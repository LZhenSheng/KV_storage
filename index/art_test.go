package index

import (
	"bitcask-go/data"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 12})

}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	pos := art.Get([]byte("key-1"))
	t.Log(pos)
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()

	res1, ok1 := art.Delete([]byte("not exist"))
	assert.Nil(t, res1)
	assert.False(t, ok1)

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	pos := art.Get([]byte("key-1"))

	t.Log(pos)

	res2, ok2 := art.Delete([]byte("key-1"))
	assert.NotNil(t, res2)
	assert.True(t, ok2)
}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewART()

	t.Log(art.Size())
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	t.Log(art.Size())
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()

	t.Log(art.Size())
	art.Put([]byte("ksdfa"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("dfs"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("khj"), &data.LogRecordPos{Fid: 1, Offset: 12})

	iter := art.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		t.Log(string(iter.Key()))
	}
}
