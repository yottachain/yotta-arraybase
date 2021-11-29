package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"time"

	ytab "github.com/yottachain/yotta-arraybase"
)

var r *rand.Rand

func init() {
	r = rand.New(rand.NewSource(time.Now().Unix()))
}

func RandBytes(len int) []byte {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		b := r.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return bytes
}

func FixedBytes(x byte, len int) []byte {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		bytes[i] = x
	}
	return bytes
}

func main() {
	arraybase, err := ytab.InitArrayBase(context.Background(), "d:\\syncdata", 1000000, 10, 10)
	if err != nil {
		panic(err)
	}
	var start uint64 = 7035801066784423936
	count := 100000
	for m := 0; m < 100; m++ {
		blocks := make([]*ytab.Block, 0)
		for i := 0; i < count; i++ {
			start++
			block := new(ytab.Block)
			block.ID = start
			block.VNF = 164
			block.AR = 128
			for j := 0; j < 164; j++ {
				shard := new(ytab.Shard)
				shard.VHF = RandBytes(16)
				shard.NodeID = 41
				shard.NodeID2 = 108
				block.Shards = append(block.Shards, shard)
			}
			blocks = append(blocks, block)
		}
		start := time.Now().UnixNano()
		from, to, err := arraybase.Write(blocks, nil, nil)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%d blocks: %d->%d, cost %dms\n", len(blocks), from, to, time.Duration(time.Now().UnixNano()-start)*time.Nanosecond/time.Millisecond)
	}
	time.Sleep(10 * time.Second)
}

func main1() {
	file, err := os.OpenFile("d://driver.zip", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}
	defer func() {
		file.Close()
	}()
	go func() {
		for i := 0; i < 100; i++ {
			data := FixedBytes(byte(33+i), 4096)
			n, err := file.WriteAt(data, int64(i*100))
			if err != nil {
				panic(err)
			}
			fmt.Printf("write %d bytes at %d\n", n, i*100)
		}
	}()

	go func() {
		for i := 0; i < 100; i++ {
			data := make([]byte, 1024)
			n, err := file.ReadAt(data, int64(i*100))
			if err != nil {
				panic(err)
			}
			fmt.Printf("read %d bytes at %d: %s\n", n, i*100, hex.EncodeToString(data))
		}
	}()
	time.Sleep(1000 * time.Second)
}

func main2() {
	//mask := syscall.Umask(0)
	//defer syscall.Umask(mask)
	file, err := os.OpenFile("d://test.bin", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}
	defer func() {
		file.Close()
	}()
	cursor := int64(0)
	round := 0
	var start byte = 33
	for {
		data := FixedBytes(start, 4096)
		// data := RandBytes(4096)
		// _, err := file.Seek(cursor, 0)
		// if err != nil {
		// 	panic(err)
		// }
		data[len(data)-1] = 10
		n, err := file.WriteAt(data, cursor)
		if err != nil {
			panic(err)
		}
		fmt.Printf("writes %d bytes at %d\n", n, cursor)
		if n != 4096 {
			panic("corrupt file size")
		}
		start += 1
		cursor += 4096
		round += 1
		if round == 50 {
			return
		}
	}

}
