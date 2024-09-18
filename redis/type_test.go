package redis

import (
	bitcask "bitcask-go"
	"bitcask-go/utils"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRedisDataStructure_Get(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)
	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)
	err = rds.Set(utils.GetTestKey(2), time.Second*5, utils.RandomValue(100))
	assert.Nil(t, err)
	val1, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	t.Log(val1)

	val1, err = rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	t.Log(val1)
}

func TestRedisDataStructure_Del_Type(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-del-type")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	//del
	err = rds.Del(utils.GetTestKey(11))
	t.Log(err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)
	//type
	typ, err := rds.Type(utils.GetTestKey(1))
	assert.Nil(t, err)
	t.Log(typ)

	err = rds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)

	_, err = rds.Get(utils.GetTestKey(1))
	t.Log(err)
}

func TestRedisDataStructure_HGet(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-hget")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("filed1"), utils.RandomValue(28))
	t.Log(ok1)
	v1 := utils.RandomValue(28)
	t.Log(string(v1))
	assert.True(t, ok1)
	assert.Nil(t, err)
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("filed1"), v1)
	t.Log(ok2)
	assert.Nil(t, err)
	assert.False(t, ok2)
	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field3"), v1)
	t.Log(ok3)
	assert.Nil(t, err)
	assert.True(t, ok3)
	val1, err := rds.HGet(utils.GetTestKey(1), []byte("filed1"))
	assert.Nil(t, err)
	assert.Equal(t, v1, val1)
	val2, err := rds.HGet(utils.GetTestKey(1), []byte("field3"))
	assert.Nil(t, err)
	assert.Equal(t, v1, val2)
	_, err = rds.HGet(utils.GetTestKey(1), []byte("field-not-found"))
	assert.Equal(t, bitcask.ErrKeyNotFound, err)
}

func TestRedisDataStructure_HDel(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-hdel")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	del1, err := rds.HDel(utils.GetTestKey(1), nil)
	t.Log(del1, err)
	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("filed1"), utils.RandomValue(28))
	t.Log(ok1)
	v1 := utils.RandomValue(28)
	t.Log(string(v1))
	assert.True(t, ok1)
	assert.Nil(t, err)
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("filed1"), v1)
	t.Log(ok2)
	assert.Nil(t, err)
	assert.False(t, ok2)

	del2, err := rds.HDel(utils.GetTestKey(1), []byte("filed1"))
	t.Log(del2)
	val2, err := rds.HGet(utils.GetTestKey(1), []byte("field3"))
	t.Log(val2)
}

func TestRedisDataStructure_SIsMember(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-SIsMember")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok, err := rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	t.Log(ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	t.Log(ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-2"))
	t.Log(ok)
	ok, err = rds.SIsMember(utils.GetTestKey(2), []byte("val-1"))
	t.Log(ok, err)
	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val-1"))
	t.Log(ok, err)

	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val-2"))
	t.Log(ok, err)

	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val-not-found"))
	t.Log(ok, err)

}

func TestRedisDataStructure_SRem(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-SRem")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok, err := rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	t.Log(ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	t.Log(ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-2"))

	ok, err = rds.SRem(utils.GetTestKey(2), []byte("val-1"))
	t.Log(ok, err)
	ok, err = rds.SRem(utils.GetTestKey(1), []byte("val-1"))
	t.Log(ok, err)

}
