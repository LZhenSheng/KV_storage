package data

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
)

// crc 4byte type 1byte keySize static valueSize static
// 4+1+5+5
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// 写入到数据文件的记录
// /日志（数据文件的数据是追加的，类似日志的格式)
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecord的头部信息
type LogRecordHeader struct {
	crc        uint32        //crc校验值
	recordType LogRecordType //标识LogRecord的类型
	keySize    uint32        //key的长度
	valueSize  uint32        //value的长度
}

// LogRecordPos 数据内村索引，主要是描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 //文件id，表示将数据存储到了那个文件当中
	Offset int64  //偏移，表示将数据存储到了数据文件中的那个位置
	Size   uint32 //标识数据在磁盘上的大小
}

// TransactionRecord 暂存的事务相关的数据
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// EncodeLogRecord对LogRecord进行编码，返回字符数组及长度
// crc校验值 type类型 keysize valuesize key value
// 4字节 1字节 变长（最大5字节）（最大5字节） 变长 变长
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	//初始化一个header部分的字节数组
	header := make([]byte, maxLogRecordHeaderSize)
	//第五个字节存储Type
	header[4] = logRecord.Type
	var index = 5
	//5之后存放key和value的变长信息
	//使用变长类型，节省空间
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))
	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encBytes := make([]byte, size)
	//将header部分的内容拷贝过来
	copy(encBytes[:index], header[:index])
	//将key和value数据拷贝到字节数组中
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)
	//对整个LogRecord的数据进行CRC校验
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)
	fmt.Printf("header length:%d,crc:%d\n", index, crc)

	return encBytes, int64(size)
}

// EncodeLogRecordPos对位置信息进行编码
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32*2+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	index += binary.PutVarint(buf[index:], int64(pos.Size))
	return buf[:index]
}

// DecodeLogRecordPos解码LogRecordPos
func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	var index = 0
	fileId, n := binary.Varint(buf[index:])
	index += n
	offset, n := binary.Varint(buf[index:])
	index += n
	size, _ := binary.Varint(buf[index:])
	return &LogRecordPos{Fid: uint32(fileId), Offset: offset, Size: uint32(size)}
}

// 对字节数组中的Header信息进行解码
func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}
	var index = 5
	//取出keySize
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n
	//取出valueSize
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n
	return header, int64(index)
}
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}
