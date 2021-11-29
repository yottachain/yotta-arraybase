package ytarraybase

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type Block struct {
	ID      uint64    //block ID
	VNF     uint8     //分片数
	AR      int16     //副本数，
	Shards  []*Shard  //block所属分片列表
	Padding [149]byte //补齐4K
}

type Shard struct {
	VHF     []byte //shard哈希值
	NodeID  uint32 //shard所属矿机ID
	NodeID2 uint32 //LRC2下shard所属副本矿机ID
}

type ShardRebuildMeta struct {
	BIndex uint64 //被重建shard对应的block在数组文件中的下标
	Offset uint8  //被重建shard在block中的序号
	NID    uint32 //重建后shard所属矿机ID
	SID    uint32 //重建前shard所属矿机ID
}

func (block *Block) ConvertBytes() []byte {
	bytebuf := bytes.NewBuffer([]byte{})
	binary.Write(bytebuf, binary.BigEndian, block.ID)
	binary.Write(bytebuf, binary.BigEndian, block.VNF)
	binary.Write(bytebuf, binary.BigEndian, block.AR)
	for _, shard := range block.Shards {
		bytebuf.Write(shard.ConvertBytes())
	}
	bytebuf.Write(block.Padding[:])
	return bytebuf.Bytes()
}

func (shard *Shard) ConvertBytes() []byte {
	bytebuf := bytes.NewBuffer([]byte{})
	bytebuf.Write(shard.VHF[:])
	binary.Write(bytebuf, binary.BigEndian, shard.NodeID)
	binary.Write(bytebuf, binary.BigEndian, shard.NodeID2)
	return bytebuf.Bytes()
}

func (block *Block) FillBy(data []byte) error {
	err := binary.Read(bytes.NewReader(data[0:8]), binary.BigEndian, &(block.ID))
	if err != nil {
		return err
	}
	err = binary.Read(bytes.NewReader(data[8:9]), binary.BigEndian, &(block.VNF))
	if err != nil {
		return err
	}
	err = binary.Read(bytes.NewReader(data[9:11]), binary.BigEndian, &(block.AR))
	if err != nil {
		return err
	}
	for i := 11; i < int(11+24*block.VNF); i += 24 {
		shard := new(Shard)
		err := shard.FillBy(data[i : i+24])
		if err != nil {
			return err
		}
		block.Shards = append(block.Shards, shard)
	}
	return nil
}

func (shard *Shard) FillBy(data []byte) error {
	if len(data) != 24 {
		return errors.New("length of shard data is not 24")
	}
	shard.VHF = data[0:16]
	err := binary.Read(bytes.NewReader(data[16:20]), binary.BigEndian, &(shard.NodeID))
	if err != nil {
		return err
	}
	err = binary.Read(bytes.NewReader(data[20:24]), binary.BigEndian, &(shard.NodeID2))
	if err != nil {
		return err
	}
	return nil
}
