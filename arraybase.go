package ytarraybase

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type ReadAck struct {
	FIndex uint64
	Blocks map[uint64]*Block
	Err    error
}

type ReadMsg struct {
	Bindexes []uint64
	AckCh    chan *ReadAck
}

type WriteAck struct {
	From uint64
	To   uint64
	Err  error
}

type WriteMsg struct {
	Blocks   []*Block
	Rebuilds []*RebuildMeta
	Deletes  []uint64
	AckCh    chan *WriteAck
}

type NullAck struct {
	FIndex uint64
	Err    error
}

type InsertMsg struct {
	Blocks []*Block
	Offset uint64
	AckCh  chan *NullAck
}

type UpdateMsg struct {
	Rebuilds []*RebuildMeta
	AckCh    chan *NullAck
}

type DeleteMsg struct {
	Deletes []uint64
	AckCh   chan *NullAck
}

type SyncMsg struct {
	AckCh chan *NullAck
}

type FileOpChan struct {
	QueryCh  chan *ReadMsg
	UpdateCh chan *UpdateMsg
	InsertCh chan *InsertMsg
	DeleteCh chan *DeleteMsg
	SyncCh   chan *SyncMsg
}

type CheckPoint struct {
	FileIndex   uint64
	FileOffset  uint64
	RowsPerFile uint64
	UpdateTime  int64
}

type ArrayBase struct {
	db         *sql.DB
	writeCh    chan *WriteMsg
	readCh     chan *ReadMsg
	baseDir    string
	checkpoint *CheckPoint
	opchans    []*FileOpChan
	sync.RWMutex
}

var ErrChFullFilled = errors.New("channel is fullfilled")

func InitArrayBase(ctx context.Context, baseDir string, rowsPerFile uint64, readChLen, writeChLen int) (*ArrayBase, error) {
	dbDir := path.Join(baseDir, "db")
	if !isExist(baseDir) {
		err := os.Mkdir(baseDir, 0775)
		if err != nil {
			panic(err)
		}
	}
	if !isExist(dbDir) {
		err := os.Mkdir(dbDir, 0775)
		if err != nil {
			panic(err)
		}
	}
	db, err := sql.Open("sqlite3", path.Join(dbDir, "checkpoint.db"))
	if err != nil {
		panic(err)
	}
	arrayBase := new(ArrayBase)
	arrayBase.db = db
	arrayBase.readCh = make(chan *ReadMsg, readChLen)
	arrayBase.writeCh = make(chan *WriteMsg, writeChLen)
	arrayBase.baseDir = baseDir
	err = arrayBase.InitTable(ctx)
	if err != nil {
		panic(err)
	}
	checkPoint, err := arrayBase.QueryCheckPoint(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			arrayBase.checkpoint = &CheckPoint{FileIndex: 0, FileOffset: 0, RowsPerFile: rowsPerFile, UpdateTime: time.Now().Unix()}
			err = arrayBase.UpdateCheckPoint(ctx, 0, 0, time.Now().Unix())
			if err != nil {
				panic(err)
			}
		} else {
			panic(err)
		}
	} else {
		arrayBase.checkpoint = checkPoint
	}
	for i := 0; i <= int(arrayBase.checkpoint.FileIndex); i++ {
		opch := &FileOpChan{
			QueryCh:  make(chan *ReadMsg),
			UpdateCh: make(chan *UpdateMsg),
			InsertCh: make(chan *InsertMsg),
			DeleteCh: make(chan *DeleteMsg),
			SyncCh:   make(chan *SyncMsg),
		}
		f, err := os.OpenFile(filepath.Join(arrayBase.baseDir, fmt.Sprintf("%d.dat", i)), os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			panic(err)
		}
		arrayBase.opchans = append(arrayBase.opchans, opch)
		index := uint64(i)
		go arrayBase.processRead(index, opch, f)
		go arrayBase.processWrite(index, opch, f)
	}
	go arrayBase.read()
	go arrayBase.write(ctx)
	return arrayBase, nil
}

type BlockIterator struct {
	files       []*os.File
	count       uint64
	index       uint64
	rowsPerFile uint64
	fileOffset  uint64
}

func (iter *BlockIterator) HasNext() bool {
	b := iter.index < (iter.count-1)*iter.rowsPerFile+iter.fileOffset
	if !b {
		for _, f := range iter.files {
			f.Close()
		}
	}
	return b
}

func (iter *BlockIterator) Close() {
	for _, f := range iter.files {
		f.Close()
	}
}

func (iter *BlockIterator) Next(count uint64) (v []*Block) {
	findex := iter.index / iter.rowsPerFile
	if findex == iter.count-1 {
		if (iter.count-1)*iter.rowsPerFile+iter.fileOffset-iter.index < count {
			count = (iter.count-1)*iter.rowsPerFile + iter.fileOffset - iter.index
		}
	} else {
		if (findex+1)*iter.rowsPerFile-iter.index < count {
			count = (findex+1)*iter.rowsPerFile - iter.index
		}
	}
	buf := make([]byte, 4096*count)
	n, err := iter.files[findex].ReadAt(buf, int64(iter.index-findex*iter.rowsPerFile)*4096)
	if err != nil {
		panic(err)
	}
	if n != 4096*int(count) {
		panic(fmt.Errorf("length of read data is not %d", int(iter.index-findex*iter.rowsPerFile)*4096))
	}
	blocksTmp := make([]Block, count)
	blocks := make([]*Block, count)
	for i := 0; i < int(count); i++ {
		err = (&blocksTmp[i]).FillBy(buf[4096*i : 4096*(i+1)])
		if err != nil {
			panic(err)
		}
		blocks[i] = &blocksTmp[i]
	}

	iter.index += count
	return blocks
}

func (db *ArrayBase) Iterator() *BlockIterator {
	if db.checkpoint == nil {
		panic(errors.New("arraybase is not initialized"))
	}
	files := make([]*os.File, 0)
	for i := 0; i <= int(db.checkpoint.FileIndex); i++ {
		f, err := os.OpenFile(filepath.Join(db.baseDir, fmt.Sprintf("%d.dat", i)), os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			panic(err)
		}
		files = append(files, f)
	}
	return &BlockIterator{files: files, count: db.checkpoint.FileIndex + 1, index: 0, rowsPerFile: db.checkpoint.RowsPerFile, fileOffset: db.checkpoint.FileOffset}
}

func (db *ArrayBase) read() {
	for msg := range db.readCh {
		m := make(map[uint64][]uint64)
		for _, index := range msg.Bindexes {
			fileIndex := index / db.checkpoint.RowsPerFile
			m[fileIndex] = append(m[fileIndex], index)
		}
		readChs := make([]chan *ReadAck, 0)
		selectCase := make([]reflect.SelectCase, 0)
		for k, v := range m {
			rmsg := &ReadMsg{Bindexes: v, AckCh: make(chan *ReadAck, 1)}
			db.opchans[k].QueryCh <- rmsg
			readChs = append(readChs, rmsg.AckCh)
			selectCase = append(selectCase, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(rmsg.AckCh)})
		}
		resultAck := new(ReadAck)
		result := make(map[uint64]*Block)
		for i := 0; i < len(selectCase); i++ {
			chosen, recv, recvOk := reflect.Select(selectCase)
			if recvOk {
				ack, _ := recv.Interface().(*ReadAck)
				if ack.Err != nil {
					resultAck.Err = ack.Err
					break
				}
				for k, v := range ack.Blocks {
					result[k] = v
				}
				close(readChs[chosen])
			}
		}
		if resultAck.Err == nil {
			resultAck.Blocks = result
		}
		msg.AckCh <- resultAck
	}
}

func (db *ArrayBase) write(ctx context.Context) {
	for msg := range db.writeCh {
		allChs := make([]chan *NullAck, 0)
		selectCase := make([]reflect.SelectCase, 0)
		wack := new(WriteAck)
		wack.From = db.checkpoint.FileIndex*db.checkpoint.RowsPerFile + db.checkpoint.FileOffset
		//insert
		blocksArr := make([][]*Block, 0)
		if db.checkpoint.FileOffset+uint64(len(msg.Blocks)) >= db.checkpoint.RowsPerFile {
			blocks1 := msg.Blocks[0:int(db.checkpoint.RowsPerFile-db.checkpoint.FileOffset)]
			blocksArr = append(blocksArr, blocks1)
			blocks2 := msg.Blocks[int(db.checkpoint.RowsPerFile-db.checkpoint.FileOffset):]
			fi := db.checkpoint.FileIndex
			for i := 0; i <= len(blocks2); i += int(db.checkpoint.RowsPerFile) {
				fi++
				opch := &FileOpChan{
					QueryCh:  make(chan *ReadMsg),
					UpdateCh: make(chan *UpdateMsg),
					InsertCh: make(chan *InsertMsg),
					DeleteCh: make(chan *DeleteMsg),
					SyncCh:   make(chan *SyncMsg),
				}
				f, err := os.OpenFile(filepath.Join(db.baseDir, fmt.Sprintf("%d.dat", fi)), os.O_RDWR|os.O_CREATE, 0755)
				if err != nil {
					panic(err)
				}
				db.opchans = append(db.opchans, opch)
				go db.processRead(fi, opch, f)
				go db.processWrite(fi, opch, f)
				end := i + int(db.checkpoint.RowsPerFile)
				if end > len(blocks2) {
					end = len(blocks2)
				}
				if end > i {
					blocksArr = append(blocksArr, blocks2[i:end])
				}
			}
		} else {
			if len(msg.Blocks) > 0 {
				blocksArr = append(blocksArr, msg.Blocks)
			}
		}
		offset := db.checkpoint.FileOffset
		for i := 0; i < len(blocksArr); i++ {
			currOffset := offset
			insMsg := &InsertMsg{Blocks: blocksArr[i], Offset: currOffset, AckCh: make(chan *NullAck, 1)}
			db.opchans[db.checkpoint.FileIndex+uint64(i)].InsertCh <- insMsg
			offset = (offset + uint64(len(blocksArr[i]))) % db.checkpoint.RowsPerFile
			allChs = append(allChs, insMsg.AckCh)
			selectCase = append(selectCase, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(insMsg.AckCh)})
		}
		//delete
		md := make(map[uint64][]uint64)
		for _, index := range msg.Deletes {
			fileIndex := index / db.checkpoint.RowsPerFile
			md[fileIndex] = append(md[fileIndex], index)
		}
		for k, v := range md {
			dmsg := &DeleteMsg{Deletes: v, AckCh: make(chan *NullAck, 1)}
			db.opchans[k].DeleteCh <- dmsg
			allChs = append(allChs, dmsg.AckCh)
			selectCase = append(selectCase, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(dmsg.AckCh)})
		}
		//update
		mu := make(map[uint64][]*RebuildMeta)
		for _, rebuild := range msg.Rebuilds {
			fileIndex := rebuild.BIndex / db.checkpoint.RowsPerFile
			mu[fileIndex] = append(mu[fileIndex], rebuild)
		}
		for k, v := range mu {
			umsg := &UpdateMsg{Rebuilds: v, AckCh: make(chan *NullAck, 1)}
			db.opchans[k].UpdateCh <- umsg
			allChs = append(allChs, umsg.AckCh)
			selectCase = append(selectCase, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(umsg.AckCh)})
		}
		//processing acknowledge
		syncFileIndexes := make(map[uint64]bool)
		for i := 0; i < len(selectCase); i++ {
			chosen, recv, recvOk := reflect.Select(selectCase)
			if recvOk {
				ack, _ := recv.Interface().(*NullAck)
				if ack.Err != nil {
					wack.Err = ack.Err
				}
				syncFileIndexes[ack.FIndex] = true
				close(allChs[chosen])
			}
		}
		//submit change
		if wack.Err == nil {
			allChs := make([]chan *NullAck, 0)
			selectCase := make([]reflect.SelectCase, 0)
			for k := range syncFileIndexes {
				smsg := &SyncMsg{AckCh: make(chan *NullAck, 1)}
				db.opchans[k].SyncCh <- smsg
				allChs = append(allChs, smsg.AckCh)
				selectCase = append(selectCase, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(smsg.AckCh)})
			}
			for i := 0; i < len(selectCase); i++ {
				chosen, recv, recvOk := reflect.Select(selectCase)
				if recvOk {
					ack, _ := recv.Interface().(*NullAck)
					if ack.Err != nil {
						wack.Err = ack.Err
					}
					close(allChs[chosen])
				}
			}
			if wack.Err == nil {
				newFileIndex := db.checkpoint.FileIndex + (db.checkpoint.FileOffset+uint64(len(msg.Blocks)))/db.checkpoint.RowsPerFile
				newFileOffset := (db.checkpoint.FileOffset + uint64(len(msg.Blocks))) % db.checkpoint.RowsPerFile
				err := db.UpdateCheckPoint(ctx, newFileIndex, newFileOffset, time.Now().Unix())
				if err != nil {
					continue
				}
				db.checkpoint.FileIndex = newFileIndex
				db.checkpoint.FileOffset = newFileOffset
				wack.To = newFileIndex*db.checkpoint.RowsPerFile + newFileOffset
			}
		}
		msg.AckCh <- wack
	}
}

func (db *ArrayBase) processRead(fileIndex uint64, opch *FileOpChan, f *os.File) {
	for qry := range opch.QueryCh {
		ack := new(ReadAck)
		ack.FIndex = fileIndex
		blocks := make(map[uint64]*Block)
		for _, index := range qry.Bindexes {
			buf := make([]byte, 4096)
			n, err := f.ReadAt(buf, int64(index-fileIndex*db.checkpoint.RowsPerFile)*4096)
			if err != nil {
				ack.Err = err
				break
			}
			if n != 4096 {
				ack.Err = errors.New("length of read data is not 4096")
				break
			}
			block := new(Block)
			err = block.FillBy(buf)
			if err != nil {
				ack.Err = err
				break
			}
			if block.ID == 0 {
				blocks[index] = nil
			} else {
				blocks[index] = block
			}
		}
		if ack.Err == nil {
			ack.Blocks = blocks
		}
		qry.AckCh <- ack
	}
}

func (db *ArrayBase) processWrite(fileIndex uint64, opch *FileOpChan, f *os.File) {
	for {
		select {
		case ins := <-opch.InsertCh:
			ack := new(NullAck)
			ack.FIndex = fileIndex
			if db.checkpoint.FileIndex != fileIndex {
				ack.Err = errors.New("current file is not latest file")
			} else {
				bufs := make([][]byte, 0)
				for _, block := range ins.Blocks {
					bufs = append(bufs, block.ConvertBytes())
				}
				data := bytes.Join(bufs, []byte(""))
				n, err := f.WriteAt(data, int64(ins.Offset)*4096)
				if err != nil {
					ack.Err = err
				} else if n != len(ins.Blocks)*4096 {
					ack.Err = fmt.Errorf("length of write data is not %d", len(ins.Blocks)*4096)
				}
			}
			ins.AckCh <- ack
		case upt := <-opch.UpdateCh:
			ack := new(NullAck)
			ack.FIndex = fileIndex
			for _, meta := range upt.Rebuilds {
				buf := make([]byte, 4096)
				n, err := f.ReadAt(buf, int64(meta.BIndex-fileIndex*db.checkpoint.RowsPerFile)*4096) //bindex换算成当前文件中的偏移量再读取
				if err != nil {
					ack.Err = err
					break
				}
				if n != 4096 {
					ack.Err = errors.New("length of read data is not 4096")
					break
				}
				block := new(Block)
				err = block.FillBy(buf)
				if err != nil {
					ack.Err = err
					break
				}
				if block.ID == 0 {
					//block已被删除
					continue
				}
				for _, t := range meta.Transfers {
					shard := block.Shards[t.Offset]
					if t.SID == 0 && shard.NodeID2 == 0 {
						shard.NodeID2 = t.NID
					} else if t.SID == shard.NodeID && shard.NodeID2 == 0 {
						shard.NodeID = t.NID
					} else if t.SID == shard.NodeID && shard.NodeID == shard.NodeID2 {
						shard.NodeID = t.NID
						shard.NodeID2 = t.NID
					} else if t.SID == shard.NodeID && shard.NodeID2 == t.NID {
						shard.NodeID = t.NID
					} else if t.SID == shard.NodeID2 && shard.NodeID == t.NID {
						shard.NodeID2 = t.NID
					} else if t.SID == shard.NodeID && shard.NodeID2 != 0 {
						shard.NodeID = t.NID
					} else if t.SID == shard.NodeID2 && shard.NodeID != 0 {
						shard.NodeID2 = t.NID
					} else {
						continue
					}
					block.Shards[t.Offset] = shard
				}
				n, err = f.WriteAt(block.ConvertBytes(), int64(meta.BIndex-fileIndex*db.checkpoint.RowsPerFile)*4096)
				if err != nil {
					ack.Err = err
					break
				}
				if n != 4096 {
					ack.Err = errors.New("length of write data is not 4096")
					break
				}
			}
			upt.AckCh <- ack
		case del := <-opch.DeleteCh:
			ack := new(NullAck)
			ack.FIndex = fileIndex
			buf := make([]byte, 4096)
			for _, bindex := range del.Deletes {
				n, err := f.WriteAt(buf, int64(bindex-fileIndex*db.checkpoint.RowsPerFile)*4096)
				if err != nil {
					ack.Err = err
					break
				}
				if n != 4096 {
					ack.Err = errors.New("length of write data is not 4096")
					break
				}
			}
			del.AckCh <- ack
		case syncf := <-opch.SyncCh:
			ack := new(NullAck)
			ack.FIndex = fileIndex
			err := f.Sync()
			if err != nil {
				ack.Err = err
			}
			syncf.AckCh <- ack
		}
	}
}

func (db *ArrayBase) InitTable(ctx context.Context) error {
	sql_table := `
	CREATE TABLE IF NOT EXISTS "checkpoint" (
	"id" INTEGER PRIMARY KEY AUTOINCREMENT,
	"fileIndex" INTEGER NOT NULL,
	"fileOffset" INTEGER NOT NULL,
	"rowsPerFile" INTEGER NOT NULL,
	"updateTime" INTEGER NOT NULL
	);`
	_, err := db.db.ExecContext(ctx, sql_table)
	if err != nil {
		return err
	}
	return nil
}

func (db *ArrayBase) UpdateCheckPoint(ctx context.Context, fileIndex, fileOffset uint64, updateTime int64) error {
	stmt, err := db.db.PrepareContext(ctx, "INSERT OR REPLACE INTO checkpoint (id, fileIndex, fileOffset, rowsPerFile, updateTime) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	_, err = stmt.ExecContext(ctx, 1, fileIndex, fileOffset, db.checkpoint.RowsPerFile, updateTime)
	if err != nil {
		return err
	}
	return nil
}

func (db *ArrayBase) QueryCheckPoint(ctx context.Context) (*CheckPoint, error) {
	var id, fileIndex, fileOffset, rowsPerFile uint64
	var updateTime int64
	row := db.db.QueryRowContext(ctx, "SELECT * FROM checkpoint where id=1")
	err := row.Scan(&id, &fileIndex, &fileOffset, &rowsPerFile, &updateTime)
	if err != nil {
		return nil, err
	}
	return &CheckPoint{FileIndex: fileIndex, FileOffset: fileOffset, RowsPerFile: rowsPerFile, UpdateTime: updateTime}, nil
}

func isExist(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		if os.IsNotExist(err) {
			return false
		}
		fmt.Println(err)
		return false
	}
	return true
}

//file operation

func (db *ArrayBase) Read(bindexes []uint64) (map[uint64]*Block, error) {
	msg := &ReadMsg{Bindexes: bindexes, AckCh: make(chan *ReadAck, 1)}
	select {
	case db.readCh <- msg:
		ack := <-msg.AckCh
		close(msg.AckCh)
		return ack.Blocks, nil
	default:
		return nil, ErrChFullFilled
	}
}

func (db *ArrayBase) Write(blocks []*Block, rebuilds []*ShardRebuildMeta, deletes []uint64) (uint64, uint64, error) {
	rmetas := make([]*RebuildMeta, 0)
	sort.Sort(RebuildSlice(rebuilds))
	var currIndex uint64 = 0xffffffffffffffff
	for _, rebuild := range rebuilds {
		if rebuild.BIndex == currIndex {
			rmetas[len(rmetas)-1].Transfers = append(rmetas[len(rmetas)-1].Transfers, &ShardTransfer{Offset: rebuild.Offset, NID: rebuild.NID, SID: rebuild.SID})
		} else {
			currIndex = rebuild.BIndex
			rmetas = append(rmetas, &RebuildMeta{BIndex: currIndex, Transfers: []*ShardTransfer{{Offset: rebuild.Offset, NID: rebuild.NID, SID: rebuild.SID}}})
		}
	}
	msg := &WriteMsg{Blocks: blocks, Rebuilds: rmetas, Deletes: deletes, AckCh: make(chan *WriteAck, 1)}
	select {
	case db.writeCh <- msg:
		ack := <-msg.AckCh
		close(msg.AckCh)
		return ack.From, ack.To, nil
	default:
		return 0, 0, ErrChFullFilled
	}
}
