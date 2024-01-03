package stateless

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/clique"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/consensus/merge"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/ledgerwatch/erigon/visual"
	"github.com/ledgerwatch/log/v3"
	"github.com/wcharczuk/go-chart"
	"github.com/wcharczuk/go-chart/drawing"
)

var chartColors = []drawing.Color{
	chart.ColorBlack,
	chart.ColorRed,
	chart.ColorBlue,
	chart.ColorYellow,
	chart.ColorOrange,
	chart.ColorGreen,
}

func runBlock(bp BlockProvider, ibs *state.IntraBlockState, txnWriter state.StateWriter, blockWriter state.StateWriter, chainConfig *chain.Config, block *types.Block, vmConfig vm.Config, engine consensus.Engine) (types.Receipts, error) {
	header := block.Header()

	getHeader := func(hash common.Hash, number uint64) *types.Header {
		h, _ := bp.Header(number, hash)
		return h
	}

	getHashFn := core.GetHashFn(block.Header(), getHeader)

	vmConfig.TraceJumpDest = true
	gp := new(core.GasPool).AddGas(block.GasLimit())
	usedGas := new(uint64)
	var receipts types.Receipts

	rTx, err := bp.DB().BeginRo(context.Background())

	if err != nil {
		fmt.Printf("Error opening tx: %v\n", err)
		return nil, err
	}

	chainReader := stagedsync.ChainReader{Cfg: *chainConfig, Db: rTx, BlockReader: bp.BR()}

	if err := core.InitializeBlockExecution(engine, chainReader, block.Header(), chainConfig, ibs, txnWriter, nil); err != nil {
		return nil, err
	}

	rTx.Rollback()

	for _, tx := range block.Transactions() {
		receipt, _, err := core.ApplyTransaction(chainConfig, getHashFn, engine, nil, gp, ibs, txnWriter, header, tx, usedGas, nil, vmConfig)
		if err != nil {
			return nil, fmt.Errorf("tx %x failed: %v", tx.Hash(), err)
		}
		receipts = append(receipts, receipt)
	}

	if !vmConfig.ReadOnly {
		if _, _, _, err := engine.FinalizeAndAssemble(chainConfig, block.Header(), ibs, block.Transactions(), block.Uncles(), receipts, block.Withdrawals(), nil, nil, nil, nil); err != nil {
			return nil, err
		}

		rules := chainConfig.Rules(block.NumberU64(), header.Time)

		ibs.FinalizeTx(rules, txnWriter)

		if err := ibs.CommitBlock(rules, blockWriter); err != nil {
			return nil, fmt.Errorf("committing block %d failed: %v", block.NumberU64(), err)
		}
	}

	return receipts, nil
}

func statePicture(t *trie.Trie, number uint64) error {
	filename := fmt.Sprintf("state_%d.dot", number)
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	indexColors := visual.HexIndexColors
	fontColors := visual.HexFontColors
	visual.StartGraph(f, false)
	trie.Visual(t, f, &trie.VisualOpts{
		Highlights:     nil,
		IndexColors:    indexColors,
		FontColors:     fontColors,
		Values:         true,
		CutTerminals:   0,
		CodeCompressed: false,
		ValCompressed:  false,
		ValHex:         true,
	})
	visual.EndGraph(f)
	if err := f.Close(); err != nil {
		return err
	}
	return nil
}

func parseStarkBlockFile(starkBlocksFile string) (map[uint64]struct{}, error) {
	dat, err := ioutil.ReadFile(starkBlocksFile)
	if err != nil {
		return nil, err
	}
	blockStrs := strings.Split(string(dat), "\n")
	m := make(map[uint64]struct{})
	for _, blockStr := range blockStrs {
		if len(blockStr) == 0 {
			continue
		}
		if b, err1 := strconv.ParseUint(blockStr, 10, 64); err1 == nil {
			m[b] = struct{}{}
		} else {
			return nil, err1
		}

	}
	return m, nil
}

func starkData(witness *trie.Witness, starkStatsBase string, blockNum uint64) error {
	filename := fmt.Sprintf("%s_%d.txt", starkStatsBase, blockNum)
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	if err = trie.StarkStats(witness, f, false); err != nil {
		return err
	}
	return nil
}

type CreateDbFunc func(string) (kv.RwDB, error)

func Stateless(
	ctx context.Context,
	blockNum uint64,
	stopBlock uint64,
	datadir string,
	statefile string,
	triesize uint32,
	tryPreRoot bool,
	interval uint64,
	ignoreOlderThan uint64,
	witnessThreshold uint64,
	statsfile string,
	verifySnapshot bool,
	binary bool,
	createDb CreateDbFunc,
	starkBlocksFile string,
	starkStatsBase string,
	useStatelessResolver bool,
	witnessDatabasePath string,
	writeHistory bool,
	genesis *types.Genesis,
	chain string,
) {
	ethconfig.EnableStateless = true
	state.MaxTrieCacheSize = uint64(triesize)
	startTime := time.Now()
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()

	timeFF, err := os.Create("timings.txt")
	if err != nil {
		panic(err)
	}
	defer timeFF.Close()
	timeF := bufio.NewWriter(timeFF)
	defer timeF.Flush()
	fmt.Fprintf(timeF, "blockNr,exec,resolve,stateless_exec,calc_root,mod_root\n")

	stats, err := NewStatsFile(statsfile)
	check(err)
	defer stats.Close()

	blockProvider, err := NewBlockProviderFromDataDir(datadir, createDb)
	check(err)
	defer blockProvider.Close()

	var starkBlocks map[uint64]struct{}
	if starkBlocksFile != "" {
		starkBlocks, err = parseStarkBlockFile(starkBlocksFile)
		check(err)
	}

	db, err := createDb(statefile)
	check(err)
	defer db.Close()

	chainConfig := genesis.Config

	var preRoot common.Hash
	if blockNum == 1 {
		var gb *types.Block
		chainConfig, gb, err = core.CommitGenesisBlock(db, genesis, "", log.New())
		check(err)
		preRoot = gb.Header().Root
	} else if blockNum > 1 {
		if errBc := blockProvider.FastFwd(blockNum - 1); errBc != nil {
			check(errBc)
		}
		block, errBc := blockProvider.NextBlock()
		check(errBc)
		fmt.Printf("Block number: %d\n", blockNum-1)
		fmt.Printf("Block root hash: %x\n", block.Root())
		preRoot = block.Root()
	}

	vmConfig := vm.Config{}

	fmt.Printf("extra data %s\n", hex.EncodeToString(genesis.ExtraData))

	fmt.Printf("Preroot %s\n", preRoot.String())

	rwTx, err := db.BeginRw(context.Background())

	check(err)
	defer rwTx.Rollback()

	defer rwTx.Commit()

	// batch := memdb.NewMemoryBatch(rwTx, "")

	// defer batch.Rollback()

	// defer func() {
	// 	if err = batch.Commit(); err != nil {
	// 		fmt.Printf("Failed to commit batch: %v\n", err)
	// 	}
	// }()

	// defer batch.Flush(rwTx)

	tds := state.NewTrieDbState(preRoot, rwTx, blockNum-1, nil)

	tds.SetResolveReads(false)
	tds.SetNoHistory(!writeHistory)
	interrupt := false
	var blockWitness []byte
	var bw *trie.Witness

	processed := 0
	blockProcessingStartTime := time.Now()

	defer func() { fmt.Printf("stoppped at block number: %d\n", blockNum) }()

	var witnessDBWriter *WitnessDBWriter
	var witnessDBReader *WitnessDBReader

	if useStatelessResolver && witnessDatabasePath == "" {
		panic("to use stateless resolver, set the witness DB path")
	}

	statsFilePath := fmt.Sprintf("%v.stats.csv", witnessDatabasePath)

	var file *os.File
	file, err = os.OpenFile(statsFilePath, os.O_RDWR|os.O_CREATE, os.ModePerm)
	check(err)
	defer file.Close()

	statsFileCsv := csv.NewWriter(file)
	defer statsFileCsv.Flush()

	witnessDBWriter, err = NewWitnessDBWriter(blockProvider.DB(), statsFileCsv)
	check(err)
	fmt.Printf("witnesses will be stored to a db at path: %s\n\tstats: %s\n", witnessDatabasePath, statsFilePath)

	err = blockProvider.FastFwd(blockNum)
	check(err)

	for !interrupt {
		select {
		case <-ctx.Done():
			panic(ctx.Err())
		default:
		}

		if blockNum == stopBlock {
			break
		}

		fmt.Printf("Block number: %d, root: %x\n", blockNum-1, preRoot)

		trace := blockNum == 50492 // false // blockNum == 545080
		tds.SetResolveReads(true)
		block, err := blockProvider.NextBlock()
		check(err)
		if block == nil {
			break
		}
		if block.NumberU64() != blockNum {
			check(fmt.Errorf("block number mismatch (want=%v got=%v)", blockNum, block.NumberU64()))
		}

		var engine consensus.Engine

		if chain == "mainnet" {
			engine = ethash.NewFullFaker()
			if chainConfig.IsShanghai(block.Time()) {
				engine = merge.New(ethash.NewFullFaker())
			}
			// engine = ethash.NewFullFaker()
		} else {
			engine = clique.New(chainConfig, params.CliqueSnapshot, memdb.New(""), log.New())
		}

		execStart := time.Now()
		statedb := state.New(tds)
		gp := new(core.GasPool).AddGas(block.GasLimit())
		usedGas := new(uint64)
		header := block.Header()
		tds.StartNewBuffer()
		var receipts types.Receipts
		trieStateWriter := tds.TrieStateWriter()

		tx, err := blockProvider.DB().BeginRo(context.Background())

		if err != nil {
			fmt.Printf("Error opening tx: %v\n", err)
			return
		}

		chainReader := stagedsync.ChainReader{Cfg: *chainConfig, Db: tx, BlockReader: blockProvider.BR()}

		if err := core.InitializeBlockExecution(engine, chainReader, block.Header(), chainConfig, statedb, trieStateWriter, nil); err != nil {
			fmt.Printf("Error initializing block %d: %v\n", blockNum, err)
			return
		}

		tx.Rollback()

		// When a block is empty, nothing will be read from statedb in some consensus, e.g. clique.
		// The trie remains empty in memory, so it will yield a wrong root hash. Reading an arbitrary account from statedb will populate trie and make everything right.
		if len(block.Transactions()) == 0 {
			statedb.GetBalance(common.HexToAddress("0x1234"))
		}

		fmt.Printf("current block number=%d hash=%s root=%s\n", block.Number().Uint64(), block.Hash().String(), block.Header().Root.String())

		getHeader := func(hash common.Hash, number uint64) *types.Header {
			h, _ := blockProvider.Header(number, hash)
			return h
		}

		getHashFn := core.GetHashFn(block.Header(), getHeader)

		for _, tx := range block.Transactions() {
			receipt, _, err := core.ApplyTransaction(chainConfig, getHashFn, engine, nil, gp, statedb, trieStateWriter, header, tx, usedGas, nil, vmConfig)
			if err != nil {
				check(fmt.Errorf("tx %x failed: %v", tx.Hash(), err))
				return
			}

			if !chainConfig.IsByzantium(block.NumberU64()) {
				tds.StartNewBuffer()
			}

			receipts = append(receipts, receipt)
		}

		if _, _, _, err = engine.FinalizeAndAssemble(chainConfig, block.Header(), statedb, block.Transactions(), block.Uncles(), receipts, block.Withdrawals(), nil, nil, nil, nil); err != nil {
			fmt.Printf("Finalize of block %d failed: %v\n", blockNum, err)
			return
		}

		statedb.FinalizeTx(chainConfig.Rules(block.NumberU64(), header.Time), trieStateWriter)

		if witnessDBReader != nil {
			tds.SetBlockNr(blockNum)
			err = tds.ResolveStateTrieStateless(witnessDBReader)
			if err != nil {
				fmt.Printf("Failed to statelessly resolve state trie: %v\n", err)
				return
			}
		} else {
			var resolveWitnesses []*trie.Witness
			if resolveWitnesses, err = tds.ResolveStateTrie(false, false); err != nil {
				fmt.Printf("Failed to resolve state trie: %v\n", err)
				return
			}
			if len(resolveWitnesses) > 0 && blockNum >= witnessThreshold {
				fmt.Printf("Upserting witnesses for block %d\n", blockNum)
				witnessDBWriter.MustUpsert(blockNum, uint32(state.MaxTrieCacheSize), resolveWitnesses)
			}
		}
		execTime2 := time.Since(execStart)
		blockWitness = nil

		// Witness has to be extracted before the state trie is modified
		var blockWitnessStats *trie.BlockWitnessStats
		bw, err = tds.ExtractWitness(trace, binary /* is binary */)
		if err != nil {
			fmt.Printf("error extracting witness for block %d: %v\n", blockNum, err)
			return
		}

		if blockNum >= witnessThreshold {
			witnessDBWriter.MustUpsertOneWitness(blockNum, bw)
		}

		var buf bytes.Buffer
		blockWitnessStats, err = bw.WriteInto(&buf)
		if err != nil {
			fmt.Printf("error extracting witness for block %d: %v\n", blockNum, err)
			return
		}
		blockWitness = buf.Bytes()
		err = stats.AddRow(blockNum, blockWitnessStats)
		check(err)

		finalRootFail := false
		execStart = time.Now()

		if blockWitness != nil { // blockWitness == nil means the extraction fails

			var s *state.Stateless
			var w *trie.Witness
			w, err = trie.NewWitnessFromReader(bytes.NewReader(blockWitness), false)
			bw.WriteDiff(w, os.Stdout)
			if err != nil {
				fmt.Printf("error deserializing witness for block %d: %v\n", blockNum, err)
				return
			}
			if _, ok := starkBlocks[blockNum-1]; ok {
				err = starkData(w, starkStatsBase, blockNum-1)
				check(err)
			}
			s, err = state.NewStateless(preRoot, w, blockNum-1, trace, binary /* is binary */)
			if err != nil {
				fmt.Printf("Error making stateless2 for block %d: %v\n", blockNum, err)
				filename := fmt.Sprintf("right_%d.txt", blockNum-1)
				f, err1 := os.Create(filename)
				if err1 == nil {
					defer f.Close()
					tds.PrintTrie(f)
				}
				return
			}
			if _, ok := starkBlocks[blockNum-1]; ok {
				err = statePicture(s.GetTrie(), blockNum-1)
				check(err)
			}
			ibs := state.New(s)
			ibs.SetTrace(trace)
			s.SetBlockNr(blockNum)
			if _, err = runBlock(blockProvider, ibs, s, s, chainConfig, block, vmConfig, engine); err != nil {
				fmt.Printf("Error running block %d through stateless2: %v\n", blockNum, err)
				finalRootFail = true
			} else if !binary {
				if err = s.CheckRoot(header.Root); err != nil {
					fmt.Printf("Wrong block hash %x in block %d\n", block.Hash(), blockNum)
					finalRootFail = true
				}
			}
		}
		execTime3 := time.Since(execStart)
		execStart = time.Now()
		var preCalculatedRoot common.Hash
		if tryPreRoot {
			preCalculatedRoot, err = tds.CalcTrieRoots(blockNum == 1)
			if err != nil {
				fmt.Printf("failed to calculate preRoot for block %d: %v\n", blockNum, err)
				return
			}
		}
		execTime4 := time.Since(execStart)
		execStart = time.Now()
		roots, err := tds.UpdateStateTrie()
		if err != nil {
			fmt.Printf("failed to calculate IntermediateRoot: %v\n", err)
			return
		}
		execTime5 := time.Since(execStart)
		if tryPreRoot && tds.LastRoot() != preCalculatedRoot {
			filename := fmt.Sprintf("right_%d.txt", blockNum)
			f, err1 := os.Create(filename)
			if err1 == nil {
				defer f.Close()
				tds.PrintTrie(f)
			}
			fmt.Printf("block %d, preCalculatedRoot %x != lastRoot %x\n", blockNum, preCalculatedRoot, tds.LastRoot())
			return
		}
		if finalRootFail {
			filename := fmt.Sprintf("right_%d.txt", blockNum)
			f, err1 := os.Create(filename)
			if err1 == nil {
				defer f.Close()
				tds.PrintTrie(f)
			}
			return
		}

		nextRoot := roots[len(roots)-1]
		if nextRoot != block.Root() {
			fmt.Printf("Root hash does not match for block %d, expected %x, was %x\n", blockNum, block.Root(), nextRoot)
			return
		}
		tds.SetBlockNr(blockNum)

		blockWriter := tds.DbStateWriter()
		err = statedb.CommitBlock(chainConfig.Rules(blockNum, header.Time), blockWriter)
		if err != nil {
			fmt.Printf("Committing block %d failed: %v", blockNum, err)
			return
		}
		if writeHistory {
			if err = blockWriter.WriteChangeSets(); err != nil {
				fmt.Printf("Writing changesets for block %d failed: %v", blockNum, err)
				return
			}
			if err = blockWriter.WriteHistory(); err != nil {
				fmt.Printf("Writing history for block %d failed: %v", blockNum, err)
				return
			}
		}

		// willSnapshot := interval > 0 && blockNum > 0 && blockNum >= ignoreOlderThan && blockNum%interval == 0

		// if willSnapshot {
		// 	tds.EvictTries(false)
		// }

		preRoot = header.Root
		blockNum++
		processed++

		if blockNum%1000 == 0 {
			// overwrite terminal line, if no snapshot was made and not the first line
			if blockNum > 0 {
				fmt.Printf("\r")
			}

			secondsSinceStart := time.Since(blockProcessingStartTime) / time.Second
			if secondsSinceStart < 1 {
				secondsSinceStart = 1
			}
			blocksPerSecond := float64(processed) / float64(secondsSinceStart)

			fmt.Printf("Processed %d blocks (%v blocks/sec)", blockNum, blocksPerSecond)
		}
		fmt.Fprintf(timeF, "%d,%d,%d,%d,%d\n", blockNum,
			execTime2.Nanoseconds(),
			execTime3.Nanoseconds(),
			execTime4.Nanoseconds(),
			execTime5.Nanoseconds())
		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			fmt.Println("interrupted, please wait for cleanup...")
		default:
		}
	}
	fmt.Printf("Processed %d blocks\n", blockNum)
	fmt.Printf("Next time specify --block %d\n", blockNum)
	fmt.Printf("Stateless client analysis took %s\n", time.Since(startTime))
}
