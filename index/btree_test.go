package index

import (
	"bitcask-go/data"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)
	res := bt.Get([]byte(""))
	t.Log(res)
	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)
	res2 = bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 22})
	t.Log(res2)
	t.Log(bt.Size())
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.NotNil(t, res3)

	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)

}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)
	res2, flag := bt.Delete(nil)
	assert.Nil(t, res2)
	assert.True(t, flag)

	res3 := bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 22, Offset: 33})
	assert.Nil(t, res3)
	res4, flag := bt.Delete([]byte("aaa"))
	assert.NotNil(t, res4)
	assert.True(t, flag)

}

// func TestBTree_Iterator(t *testing.T) {
// 	bt1 := NewBTree()
// 	//1.BTree为空的情况
// 	iter1 := bt1.Iterator(false)
// 	t.Log(iter1.Valid())

// 	//2BTree有数据的情况
// 	bt1.Put([]byte("code"), &data.LogRecordPos{Fid: 1, Offset: 10})
// 	iter2 := bt1.Iterator(false)
// 	assert.Equal(t, true, iter2.Valid())
// 	assert.NotNil(t, iter2.Key())
// 	assert.NotNil(t, iter2.Value())

// 	iter2.Next()
// 	assert.Equal(t, false, iter2.Valid())

// 	//有多条数据
// 	bt1.Put([]byte("acee"), &data.LogRecordPos{Fid: 1, Offset: 10})
// 	bt1.Put([]byte("eede"), &data.LogRecordPos{Fid: 1, Offset: 10})
// 	bt1.Put([]byte("bbcd"), &data.LogRecordPos{Fid: 1, Offset: 10})
// 	iter3 := bt1.Iterator(false)
// 	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
// 		t.Log("key=", string(iter3.Key()))
// 		assert.NotNil(t, iter3.Key())
// 	}

// 	iter4 := bt1.Iterator(true)
// 	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
// 		t.Log("key=", string(iter4.Key()))
// 		assert.NotNil(t, iter4.Key())
// 	}

// 	//重复测试seek
// 	iter5 := bt1.Iterator(false)
// 	for iter5.Seek([]byte("cc")); iter5.Valid(); iter5.Next() {
// 		t.Log(string(iter5.Key()))
// 		assert.NotNil(t, iter5.Key())
// 	}

// 	//反向遍历的seek
// 	iter6 := bt1.Iterator(true)
// 	for iter6.Seek([]byte("zz")); iter6.Valid(); iter6.Next() {
// 		t.Log(string(iter6.Key()))
// 		assert.NotNil(t, iter6.Key())
// 	}
// }
