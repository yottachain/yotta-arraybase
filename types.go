package ytarraybase

import (
	"encoding/binary"
	"errors"
)

type Block struct {
	ID      uint64   `json:"id"`     //block ID
	VHP     []byte   `json:"vhp"`    //VHP
	VHB     []byte   `json:"vhb"`    //VHB
	KED     []byte   `json:"ked"`    //KED
	VNF     uint8    `json:"vnf"`    //分片数
	AR      int16    `json:"ar"`     //副本数，
	Shards  []*Shard `json:"shards"` //block所属分片列表
	Padding [70]byte `json:"-"`      //补齐4K
}

type Shard struct {
	VHF     []byte `json:"vhf"`  //shard哈希值
	NodeID  uint32 `json:"nid"`  //shard所属矿机ID
	NodeID2 uint32 `json:"nid2"` //LRC2下shard所属副本矿机ID
}

type ShardRebuildMeta struct {
	BIndex uint64 `json:"bindex"` //被重建shard对应的block在数组文件中的下标
	Offset uint8  `json:"offset"` //被重建shard在block中的序号
	NID    uint32 `json:"nid"`    //重建后shard所属矿机ID
	SID    uint32 `json:"sid"`    //重建前shard所属矿机ID
}

type RebuildMeta struct {
	BIndex    uint64           `json:"bindex"`
	Transfers []*ShardTransfer `json:"transfers"`
}

type ShardTransfer struct {
	Offset uint8  `json:"offset"`
	NID    uint32 `json:"nid"`
	SID    uint32 `json:"sid"`
}

type RebuildSlice []*ShardRebuildMeta

func (s RebuildSlice) Len() int           { return len(s) }
func (s RebuildSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s RebuildSlice) Less(i, j int) bool { return s[i].BIndex < s[j].BIndex }

func (block *Block) ConvertBytes() []byte {
	buf := make([]byte, 4096)
	binary.BigEndian.PutUint64(buf[0:8], block.ID)
	copy(buf[8:40], block.VHP)
	copy(buf[40:56], block.VHB)
	copy(buf[56:88], block.KED)
	buf[88] = block.VNF
	binary.BigEndian.PutUint16(buf[89:91], uint16(block.AR))
	i := 91
	for _, shard := range block.Shards {
		copy(buf[i:i+24], shard.ConvertBytes())
		i += 24
	}
	return buf
}

func (shard *Shard) ConvertBytes() []byte {
	buf := make([]byte, 24)
	copy(buf, shard.VHF)
	binary.BigEndian.PutUint32(buf[16:20], shard.NodeID)
	binary.BigEndian.PutUint32(buf[20:24], shard.NodeID2)
	return buf
}

func (block *Block) FillBy(data []byte) error {
	block.ID = binary.BigEndian.Uint64(data[0:8])
	block.VHP = data[8:40]
	block.VHB = data[40:56]
	block.KED = data[56:88]
	block.VNF = data[88]
	block.AR = int16(binary.BigEndian.Uint16(data[89:91]))
	// err := binary.Read(bytes.NewReader(data[0:8]), binary.BigEndian, &(block.ID))
	// if err != nil {
	// 	return err
	// }
	// err = binary.Read(bytes.NewReader(data[8:9]), binary.BigEndian, &(block.VNF))
	// if err != nil {
	// 	return err
	// }
	// err = binary.Read(bytes.NewReader(data[9:11]), binary.BigEndian, &(block.AR))
	// if err != nil {
	// 	return err
	// }
	end := 91 + 24*int(block.VNF)
	for i := 91; i < end; i += 24 {
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
	shard.NodeID = binary.BigEndian.Uint32(data[16:20])
	shard.NodeID2 = binary.BigEndian.Uint32(data[20:24])
	// err := binary.Read(bytes.NewReader(data[16:20]), binary.BigEndian, &(shard.NodeID))
	// if err != nil {
	// 	return err
	// }
	// err = binary.Read(bytes.NewReader(data[20:24]), binary.BigEndian, &(shard.NodeID2))
	// if err != nil {
	// 	return err
	// }
	return nil
}
