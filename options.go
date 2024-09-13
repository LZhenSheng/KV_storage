package bitcask_go

import "os"

type Options struct {
	DirPath            string      //数据哭数据目录
	DataFileSize       int64       //数据文件的大小
	SyncWrites         bool        //每次写数据是否持久化
	BytesPerSync       uint        //累计写到多少字节后进行持久化
	IndexType          IndexerType //索引类型
	MMapAtStartup      bool        //启动时是否使用mmap加载数据
	DataFileMergeRatio float32     //数据文件合并的数据
}

// IteratorOptions索引迭代器配置项
type IteratorOptions struct {
	//遍历前缀为指定值的key，模式为空
	Prefix []byte
	//是否反向遍历，默认为false是正向
	Reverse bool
}

// WriteBatchOptions批量配置项
type WriteBatchOptions struct {
	//一个批次当中最大的数据量
	MaxBatchNum uint
	//提交时查看sync持久化
	SyncWrites bool
}
type IndexerType = int8

const (
	//BTree索引
	BTree IndexerType = iota + 1
	//ART索引类型
	ART
	//B+树索引，将索引存储到磁盘上
	BPlusTree
)

var DefaultOptions = Options{
	DirPath:            os.TempDir(),
	DataFileSize:       256 * 1024 * 1024,
	SyncWrites:         false,
	BytesPerSync:       0,
	IndexType:          BTree,
	MMapAtStartup:      true,
	DataFileMergeRatio: 0.5,
}

var DefalutIteratorOptinos = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
