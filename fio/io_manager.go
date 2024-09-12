package fio

const DataFileParm = 0644

type FileIOType = byte

const (
	// StandardFIO 标准文件IO
	StandardFIO FileIOType = iota
	//MemoryMap内存文件映射
	MemoryMap
)

// IOManager抽象IO管理接口
type IOManager interface {
	//Read从文件的给定位置读取对应的数据
	Read([]byte, int64) (int, error)

	//Write写入字节数组到文件中
	Write([]byte) (int, error)

	//Sync 持久化数据
	Sync() error

	//Close 关闭文件
	Close() error

	//获取文件大小
	Size() (int64, error)
}

// 初始化IOManager，目前只支持标准FileIO
func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFIO:
		return NewFileIOManager(fileName)
	case MemoryMap:
		return NewMapIOManager(fileName)
	default:
		panic("unsupported io type")
	}
}
