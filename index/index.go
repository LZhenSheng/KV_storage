package index

import (
	"bitcask-go/data"
	"bytes"

	"github.com/google/btree"
)

// Index抽象索引接口，后续如果想要接入其他的数据结构，则直接实现这个接口即可
type Indexer interface {
	//Put 向对象中存储key对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos
	//Get 根据key获取对应的索引信息
	Get(key []byte) *data.LogRecordPos
	// Delete根据key删除对应的索引信息
	Delete(key []byte) (*data.LogRecordPos, bool)

	//Size索引中的数据量
	Size() int
	//Iterator索引迭代器
	Iterator(reverse bool) Iterator
}
type IndexType = int8

const (
	//BTree索引
	Btree IndexType = iota + 1
	//ART自适应基数据索引
	ART
	//B+树索引
	BPTree
)

func NewIndexer(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return NewART()
	case BPTree:
		return NewBPlusTree(dirPath, sync)
	default:
		panic("unsupported index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

// Iterator 通用索引迭代器
type Iterator interface {
	// Rewind重新回到迭代器的起点，即第一个数据
	Rewind()
	// Seek根据传入的key查询到第一个大于（或小于）等于的目标key，根据从这个key开始遍历
	Seek(key []byte)
	// Next跳转到下一个key
	Next()
	// Valid是否有效，即是否已经遍历了所有的key，用于退出遍历
	Valid() bool
	// Key当前遍历位置的key数据
	Key() []byte
	// Value当前遍历位置的Value数据
	Value() *data.LogRecordPos
	// Close关闭迭代器，释放相关资源
	Close()
}
