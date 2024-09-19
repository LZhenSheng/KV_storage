package main

import (
	bitcask "bitcask-go"
	bitcask_redis "bitcask-go/redis"
	"log"
	"sync"

	"github.com/tidwall/redcon"
)

const addr = "127.0.0.1:6380"

type BitcaskServer struct {
	dbs    map[int]*bitcask_redis.RedisDataStructure
	server *redcon.Server
	mu     sync.RWMutex
}

func main() {
	//打开redis数据结构服务
	redisDataStructure, err := bitcask_redis.NewRedisDataStructure(bitcask.DefaultOptions)
	if err != nil {
		panic(err)
	}
	//初始化bitcaskserver
	bitcaskServer := &BitcaskServer{
		dbs: make(map[int]*bitcask_redis.RedisDataStructure),
	}
	bitcaskServer.dbs[0] = redisDataStructure
	//初始化一个Redis服务器
	bitcaskServer.server = redcon.NewServer(addr, execClientCommand, bitcaskServer.accept, bitcaskServer.close)
	bitcaskServer.listen()
}
func (svr *BitcaskServer) listen() {
	log.Println("bitcask server running,ready to accept connecting.")
	_ = svr.server.ListenAndServe()
}
func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	cli := new(BitcaskClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()
	cli.server = svr
	cli.db = svr.dbs[0]
	conn.SetContext(cli)
	return true
}
func (svr *BitcaskServer) close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		_ = db.Close()
	}
	_ = svr.server.Close()
}

// import (
// 	"bufio"
// 	"fmt"
// 	"net"
// )

// // redis协议解析的示例
// func main() {
// 	conn, err := net.Dial("tcp", "localhost:6379")
// 	if err != nil {
// 		panic(err)
// 	}
// 	//向redis发送一个命令
// 	cmd := "set k-name-2 bitcask-kv-2\r\n"
// 	conn.Write([]byte(cmd))

// 	//解析Redis响应
// 	reader := bufio.NewReader(conn)
// 	res, err := reader.ReadString('\n')
// 	if err != nil {
// 		panic(err)
// 	}
// 	fmt.Println(res)
// }
