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
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type ReadAck struct {
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
	Rebuilds []*ShardRebuildMeta
	Deletes  []uint64
	AckCh    chan *WriteAck
}

type NullAck struct {
	Err error
}

type InsertMsg struct {
	Blocks []*Block
	Offset uint64
	AckCh  chan *NullAck
}

type UpdateMsg struct {
	Rebuilds []*ShardRebuildMeta
	AckCh    chan *NullAck
}

type DeleteMsg struct {
	Deletes []uint64
	AckCh   chan *NullAck
}

type FileOpChan struct {
	QueryCh  chan *ReadMsg
	UpdateCh chan *UpdateMsg
	InsertCh chan *InsertMsg
	DeleteCh chan *DeleteMsg
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
	// rowsPerFile = 1000000
	// baseDir = "D:\\syncdata"
	dbDir := path.Join(baseDir, "db")
	// mask := syscall.Umask(0)
	// defer syscall.Umask(mask)
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
		}
		f, err := os.OpenFile(filepath.Join(arrayBase.baseDir, fmt.Sprintf("%d.dat", i)), os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			panic(err)
		}
		arrayBase.opchans = append(arrayBase.opchans, opch)
		index := uint64(i)
		go arrayBase.process(index, opch, f)
	}
	go arrayBase.read()
	go arrayBase.write(ctx)
	return arrayBase, nil
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
				ack, _ := recv.Interface().(ReadAck)
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
		//var wg sync.WaitGroup
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
				}
				f, err := os.OpenFile(filepath.Join(db.baseDir, fmt.Sprintf("%d.dat", fi)), os.O_RDWR|os.O_CREATE, 0755)
				if err != nil {
					panic(err)
				}
				db.opchans = append(db.opchans, opch)
				go db.process(fi, opch, f)
				end := i + int(db.checkpoint.RowsPerFile)
				if end > len(blocks2) {
					end = len(blocks2)
				}
				if end > i {
					blocksArr = append(blocksArr, blocks2[i:end])
				}
			}
		} else {
			blocksArr = append(blocksArr, msg.Blocks)
		}
		offset := db.checkpoint.FileOffset
		for i := 0; i < len(blocksArr); i++ {
			currOffset := offset
			insMsg := &InsertMsg{Blocks: blocksArr[i], Offset: currOffset, AckCh: make(chan *NullAck, 1)}
			db.opchans[db.checkpoint.FileIndex+uint64(i)].InsertCh <- insMsg
			offset = (offset + uint64(len(blocksArr[i]))) % db.checkpoint.RowsPerFile
			allChs = append(allChs, insMsg.AckCh)
			selectCase = append(selectCase, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(insMsg.AckCh)})
			//wg.Add(1)
		}
		//update
		mu := make(map[uint64][]*ShardRebuildMeta)
		for _, rebuild := range msg.Rebuilds {
			fileIndex := rebuild.BIndex / db.checkpoint.RowsPerFile
			mu[fileIndex] = append(mu[fileIndex], rebuild)
		}
		for k, v := range mu {
			umsg := &UpdateMsg{Rebuilds: v, AckCh: make(chan *NullAck, 1)}
			db.opchans[k].UpdateCh <- umsg
			allChs = append(allChs, umsg.AckCh)
			selectCase = append(selectCase, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(umsg.AckCh)})
			//wg.Add(1)
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
			//wg.Add(1)
		}
		//processing acknowledge
		for i := 0; i < len(selectCase); i++ {
			chosen, recv, recvOk := reflect.Select(selectCase)
			if recvOk {
				ack, _ := recv.Interface().(NullAck)
				if ack.Err != nil {
					wack.Err = ack.Err
				}
				close(allChs[chosen])
			}
		}
		//submit change
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
		msg.AckCh <- wack
	}
}

func (db *ArrayBase) process(fileIndex uint64, opch *FileOpChan, f *os.File) {
	for {
		select {
		case qry := <-opch.QueryCh:
			ack := new(ReadAck)
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
		case ins := <-opch.InsertCh:
			start := time.Now().UnixNano()
			ack := new(NullAck)
			if db.checkpoint.FileIndex != fileIndex {
				ack.Err = errors.New("current file is not latest file")
			} else {
				bufs := make([][]byte, 0)
				for _, block := range ins.Blocks {
					bufs = append(bufs, block.ConvertBytes())
				}
				fmt.Printf("%d blocks1: %dms\n", len(ins.Blocks), time.Duration(time.Now().UnixNano()-start)*time.Nanosecond/time.Millisecond)
				data := bytes.Join(bufs, []byte(""))
				fmt.Printf("%d blocks2: %dms\n", len(ins.Blocks), time.Duration(time.Now().UnixNano()-start)*time.Nanosecond/time.Millisecond)
				n, err := f.WriteAt(data, int64(ins.Offset)*4096)
				if err != nil {
					ack.Err = err
				} else if n != len(ins.Blocks)*4096 {
					ack.Err = fmt.Errorf("length of write data is not %d", len(ins.Blocks)*4096)
				}
				fmt.Printf("%d blocks3: %dms\n", len(ins.Blocks), time.Duration(time.Now().UnixNano()-start)*time.Nanosecond/time.Millisecond)
			}
			ins.AckCh <- ack
		case upt := <-opch.UpdateCh:
			ack := new(NullAck)
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
				shard := block.Shards[meta.Offset]
				if meta.SID == shard.NodeID && shard.NodeID2 == 0 {
					shard.NodeID = meta.NID
				} else if meta.SID == shard.NodeID && shard.NodeID == shard.NodeID2 {
					shard.NodeID = meta.NID
					shard.NodeID2 = meta.NID
				} else if meta.SID == shard.NodeID && shard.NodeID2 == meta.NID {
					shard.NodeID = meta.NID
				} else if meta.SID == shard.NodeID2 && shard.NodeID == meta.NID {
					shard.NodeID2 = meta.NID
				} else if meta.SID == shard.NodeID && shard.NodeID2 != 0 {
					shard.NodeID = meta.NID
				} else if meta.SID == shard.NodeID2 && shard.NodeID != 0 {
					shard.NodeID2 = meta.NID
				} else {
					continue
				}
				block.Shards[meta.Offset] = shard
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
	msg := &WriteMsg{Blocks: blocks, Rebuilds: rebuilds, Deletes: deletes, AckCh: make(chan *WriteAck, 1)}
	select {
	case db.writeCh <- msg:
		ack := <-msg.AckCh
		close(msg.AckCh)
		return ack.From, ack.To, nil
	default:
		return 0, 0, ErrChFullFilled
	}
}