package stateless

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"strings"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv/membatchwithdb"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/rawdb/blockio"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/ledgerwatch/log/v3"
)

const (
	fileSchemeExportfile = "exportfile"
	fileSchemeDB         = "db"
)

type BlockProvider interface {
	io.Closer
	FastFwd(uint64) error
	NextBlock() (*types.Block, error)
	Header(blockNum uint64, blockHash common.Hash) (*types.Header, error)
	DB() kv.RwDB
	BR() services.FullBlockReader
}

// func BlockProviderForURI(uri string, createDBFunc CreateDbFunc) (BlockProvider, error) {
// 	url, err := url.Parse(uri)
// 	if err != nil {
// 		return nil, err
// 	}
// 	switch url.Scheme {
// 	case fileSchemeExportfile:
// 		fmt.Println("Source of blocks: export file @", url.Path)
// 		return NewBlockProviderFromExportFile(url.Path)
// 	case fileSchemeDB:
// 		fallthrough
// 	default:
// 		fmt.Println("Source of blocks: db @", url.Path)
// 		return NewBlockProviderFromDB(url.Path, createDBFunc)
// 	}
// }

func blocksIO(db kv.RoDB, snapshotPath string) (services.FullBlockReader, *blockio.BlockWriter) {
	var histV3 bool
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		histV3, _ = kvcfg.HistoryV3.Enabled(tx)
		return nil
	}); err != nil {
		panic(err)
	}

	snapshotCfg := ethconfig.NewSnapCfg(true, true, false)

	allSnapshots := freezeblocks.NewRoSnapshots(snapshotCfg, snapshotPath, log.New())

	allSnapshots.OptimisticalyReopenWithDB(db)

	br := freezeblocks.NewBlockReader(allSnapshots, nil /* BorSnapshots */)
	bw := blockio.NewBlockWriter(histV3)
	return br, bw
}

type BlockChainBlockProvider struct {
	currentBlock uint64
	br           services.FullBlockReader
	db           kv.RwDB
}

func NewBlockProviderFromDataDir(path string, createDBFunc CreateDbFunc) (BlockProvider, error) {
	chaindata, err := url.JoinPath(path, "chaindata")
	check(err)

	ethDB, err := createDBFunc(chaindata)

	snapshotDir, err := url.JoinPath(path, "snapshots")
	check(err)

	br, _ := blocksIO(ethDB, snapshotDir)

	if err != nil {
		return nil, err
	}

	return &BlockChainBlockProvider{
		br: br,
		db: ethDB,
	}, nil
}

func (p *BlockChainBlockProvider) BR() services.FullBlockReader {
	return p.br
}

func (p *BlockChainBlockProvider) DB() kv.RwDB {
	return p.db
}

func (p *BlockChainBlockProvider) Close() error {
	p.db.Close()
	return nil
}

func (p *BlockChainBlockProvider) FastFwd(to uint64) error {
	p.currentBlock = to
	return nil
}

func (p *BlockChainBlockProvider) NextBlock() (*types.Block, error) {
	tx, err := p.db.BeginRw(context.Background())
	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	block, err := p.br.BlockByNumber(context.Background(), tx, p.currentBlock)
	p.currentBlock++
	return block, nil
}

func (p *BlockChainBlockProvider) Header(blockNum uint64, blockHash common.Hash) (*types.Header, error) {
	tx, err := p.db.BeginRo(context.Background())
	if err != nil {
		panic(fmt.Errorf("error opening tx: %w", err))
	}

	defer tx.Rollback()

	h, err := p.br.HeaderByNumber(context.Background(), tx, blockNum)
	if err != nil {
		h, err := p.br.HeaderByHash(context.Background(), tx, blockHash)
		if err != nil {
			return nil, err
		}
		return h, nil
	}
	return h, err

}

type ExportFileBlockProvider struct {
	stream          *rlp.Stream
	engine          consensus.Engine
	headersDB       kv.RwDB
	batch           kv.RwTx
	fh              *os.File
	reader          io.Reader
	lastBlockNumber int64
}

func NewBlockProviderFromExportFile(fn string) (BlockProvider, error) {
	// Open the file handle and potentially unwrap the gzip stream
	fh, err := os.Open(fn)
	if err != nil {
		return nil, err
	}

	var reader io.Reader = fh
	if strings.HasSuffix(fn, ".gz") {
		if reader, err = gzip.NewReader(reader); err != nil {
			return nil, err
		}
	}
	stream := rlp.NewStream(reader, 0)
	engine := ethash.NewFullFaker()
	// keeping all the past block headers in memory
	headersDB := mdbx.MustOpen(getTempFileName())
	return &ExportFileBlockProvider{stream, engine, headersDB, nil, fh, reader, -1}, nil
}

func (p *ExportFileBlockProvider) BR() services.FullBlockReader {
	return nil
}

func getTempFileName() string {
	tmpfile, err := ioutil.TempFile("", "headers.*")
	if err != nil {
		panic(fmt.Errorf("failed to create a temp file: %w", err))
	}
	tmpfile.Close()
	fmt.Printf("creating a temp headers db @ %s\n", tmpfile.Name())
	return tmpfile.Name()
}

func (p *ExportFileBlockProvider) DB() kv.RwDB {
	return p.headersDB
}

func (p *ExportFileBlockProvider) Close() error {
	return p.fh.Close()
}

func (p *ExportFileBlockProvider) WriteHeader(h *types.Header) {
	if p.batch == nil {
		tx, err := p.headersDB.BeginRw(context.Background())
		if err != nil {
			panic(fmt.Errorf("error opening tx: %w", err))
		}
		defer tx.Rollback()

		batch := membatchwithdb.NewMemoryBatch(tx, "")
		defer batch.Rollback()
		if err != nil {
			panic(fmt.Errorf("error opening batch: %w", err))
		}
		p.batch = batch
	}

	rawdb.WriteHeader(p.batch, h)
}

func (p *ExportFileBlockProvider) Header(blockNum uint64, blockHash common.Hash) (*types.Header, error) {
	panic("implement me")
}

func (p *ExportFileBlockProvider) resetStream() error {
	if _, err := p.fh.Seek(0, 0); err != nil {
		return err
	}
	if p.reader != p.fh {
		if err := p.reader.(*gzip.Reader).Reset(p.fh); err != nil {
			return err
		}
	}
	p.stream = rlp.NewStream(p.reader, 0)
	p.lastBlockNumber = -1
	return nil
}

func (p *ExportFileBlockProvider) FastFwd(to uint64) error {
	if to == 0 {
		fmt.Println("fastfwd: reseting stream")
		if err := p.resetStream(); err != nil {
			return err
		}
	}
	if p.lastBlockNumber == int64(to)-1 {
		fmt.Println("fastfwd: nothing to do there")
		return nil
	} else if p.lastBlockNumber > int64(to)-1 {
		fmt.Println("fastfwd: resetting stream")
		if err := p.resetStream(); err != nil {
			return err
		}
	}
	var b types.Block
	for {
		if err := p.stream.Decode(&b); err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("error fast fwd: %v", err)
		} else {
			p.WriteHeader(b.Header())
			p.lastBlockNumber = int64(b.NumberU64())
			if b.NumberU64() >= to-1 {
				return nil
			}
		}
	}
}

func (p *ExportFileBlockProvider) NextBlock() (*types.Block, error) {
	var b types.Block
	if err := p.stream.Decode(&b); err == io.EOF {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("error fast fwd: %v", err)
	}

	p.lastBlockNumber = int64(b.NumberU64())
	p.WriteHeader(b.Header())
	return &b, nil
}

func (p *ExportFileBlockProvider) Engine() consensus.Engine {
	return p.engine
}

func (p *ExportFileBlockProvider) GetHeader(h common.Hash, i uint64) *types.Header {
	if p.batch == nil {
		tx, err := p.headersDB.BeginRw(context.Background())
		if err != nil {
			panic(fmt.Errorf("error opening tx: %w", err))
		}

		defer tx.Rollback()

		batch := membatchwithdb.NewMemoryBatch(tx, "")
		defer batch.Rollback()
		if err != nil {
			panic(fmt.Errorf("error opening batch: %w", err))
		}
		p.batch = batch
	}

	return rawdb.ReadHeader(p.batch, h, i)
}
