package utils

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDirSize(t *testing.T) {
	dir, _ := os.Getwd()
	dirSize, err := DirSize(dir)
	// dirSize, err := DirSize("/usr/local/go/src/GolangStudy/kv-projects/bitcask-go/tmp/test-dir")
	t.Log(filepath.Join("/tmp/test-dir"))
	assert.Nil(t, err)
	t.Log(dirSize)
}

func TestAvailableDiskSize(t *testing.T) {
	size, err := AvailableDiskSize()
	assert.Nil(t, err)
	t.Log(size / 1024 / 1024 / 1024)
}
