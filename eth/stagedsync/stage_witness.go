package stagedsync

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/ledgerwatch/log/v3"
)

type WitnessCfg struct {
	db                          kv.RwDB
	chainConfig                 *chain.Config
	engine                      consensus.Engine
	blockReader                 services.FullBlockReader
	MaxGetProofRewindBlockCount int
}

func StageWitnessCfg(db kv.RwDB, chainConfig *chain.Config, engine consensus.Engine, blockReader services.FullBlockReader, MaxGetProofRewindBlockCount int) WitnessCfg {
	return WitnessCfg{
		db:                          db,
		chainConfig:                 chainConfig,
		engine:                      engine,
		blockReader:                 blockReader,
		MaxGetProofRewindBlockCount: MaxGetProofRewindBlockCount,
	}
}

func SpawnWitnessStage(s *StageState, tx kv.RwTx, cfg WitnessCfg, ctx context.Context, logger log.Logger) error {
	// TODO: Handle case for erigon v3

	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	logPrefix := s.LogPrefix()
	to, err := s.ExecutionAt(tx)
	if err != nil {
		return err
	}

	if s.BlockNumber >= to {
		// We already did witness generation for this block
		return nil
	}

	if to > s.BlockNumber+16 {
		logger.Info(fmt.Sprintf("[%s] Promoting plain state", logPrefix), "from", s.BlockNumber, "to", to)
	}

	blockNr := s.BlockNumber
	block, err := cfg.blockReader.BlockByNumber(ctx, tx, blockNr)
	if err != nil {
		return err
	}
	if block == nil {
		return fmt.Errorf("block %d not found while generating witness", blockNr)
	}

	prevHeader, err := cfg.blockReader.HeaderByNumber(ctx, tx, blockNr-1)
	if err != nil {
		return err
	}

	reader, err := rpchelper.CreateHistoryStateReader(tx, blockNr, 0, false, cfg.chainConfig.ChainName)
	if err != nil {
		return err
	}

	// TODO: Handle case with max proof rewind block count
	regenerate_hash := false

	rl := trie.NewRetainList(0)
	tds := state.NewTrieDbState(prevHeader.Root, tx, blockNr-1, reader)
	tds.SetRetainList(rl)
	tds.SetResolveReads(true)

	tds.StartNewBuffer()
	trieStateWriter := tds.TrieStateWriter()

	statedb := state.New(tds)

	usedGas := new(uint64)
	usedBlobGas := new(uint64)
	gp := new(core.GasPool).AddGas(block.GasLimit()).AddBlobGas(cfg.chainConfig.GetMaxBlobGasPerBlock())
	var receipts types.Receipts

	chainReader := NewChainReaderImpl(cfg.chainConfig, tx, cfg.blockReader, logger)
	if err := core.InitializeBlockExecution(cfg.engine, chainReader, block.Header(), cfg.chainConfig, statedb, trieStateWriter, nil); err != nil {
		return err
	}

	if len(block.Transactions()) == 0 {
		statedb.GetBalance(libcommon.HexToAddress("0x1234"))
	}

	vmConfig := vm.Config{}

	getHeader := func(hash libcommon.Hash, number uint64) *types.Header {
		h, e := cfg.blockReader.Header(ctx, tx, hash, number)
		if e != nil {
			log.Error("getHeader error", "number", number, "hash", hash, "err", e)
		}
		return h
	}
	getHashFn := core.GetHashFn(block.Header(), getHeader)

	for i, txn := range block.Transactions() {
		statedb.SetTxContext(txn.Hash(), block.Hash(), i)
		receipt, _, err := core.ApplyTransaction(cfg.chainConfig, getHashFn, cfg.engine, nil, gp, statedb, trieStateWriter, block.Header(), txn, usedGas, usedBlobGas, vmConfig)
		if err != nil {
			return err
		}

		if !cfg.chainConfig.IsByzantium(block.NumberU64()) {
			tds.StartNewBuffer()
		}

		receipts = append(receipts, receipt)
	}

	if _, _, _, err = cfg.engine.FinalizeAndAssemble(cfg.chainConfig, block.Header(), statedb, block.Transactions(), block.Uncles(), receipts, block.Withdrawals(), nil, nil, nil, nil); err != nil {
		fmt.Printf("Finalize of block %d failed: %v\n", blockNr, err)
		return err
	}

	statedb.FinalizeTx(cfg.chainConfig.Rules(block.NumberU64(), block.Header().Time), trieStateWriter)

	triePreroot := tds.LastRoot()

	if !bytes.Equal(prevHeader.Root[:], triePreroot[:]) {
		return fmt.Errorf("mismatch in expected state root computed %v vs %v indicates bug in witness implementation", prevHeader.Root, triePreroot)
	}

	loadFunc := func(loader *trie.SubTrieLoader, rl *trie.RetainList, dbPrefixes [][]byte, fixedbits []int, accountNibbles [][]byte) (trie.SubTries, error) {
		rl.Rewind()
		receiver := trie.NewSubTrieAggregator(nil, nil, false)
		receiver.SetRetainList(rl)
		pr := trie.NewMultiAccountProofRetainer(rl)
		pr.AccHexKeys = accountNibbles
		receiver.SetProofRetainer(pr)

		loaderRl := rl
		if regenerate_hash {
			loaderRl = trie.NewRetainList(0)
		}
		subTrieloader := trie.NewFlatDBTrieLoader[trie.SubTries]("eth_getWitness", loaderRl, nil, nil, false, receiver)
		subTries, err := subTrieloader.Result(tx, nil)

		rl.Rewind()

		if err != nil {
			return receiver.EmptyResult(), err
		}

		err = trie.AttachRequestedCode(tx, loader.CodeRequests())

		if err != nil {
			return receiver.EmptyResult(), err
		}

		// Reverse the subTries.Hashes and subTries.roots
		for i, j := 0, len(subTries.Hashes)-1; i < j; i, j = i+1, j-1 {
			subTries.Hashes[i], subTries.Hashes[j] = subTries.Hashes[j], subTries.Hashes[i]
			subTries.Roots()[i], subTries.Roots()[j] = subTries.Roots()[j], subTries.Roots()[i]
		}

		return subTries, nil
	}

	if err := tds.ResolveStateTrieWithFunc(loadFunc); err != nil {
		return err
	}

	w, err := tds.ExtractWitness(false, false)
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	_, err = w.WriteInto(&buf)
	if err != nil {
		return err
	}

	nw, err := trie.NewWitnessFromReader(bytes.NewReader(buf.Bytes()), false)
	if err != nil {
		return err
	}

	stateless, err := state.NewStateless(prevHeader.Root, nw, blockNr-1, false, false /* is binary */)
	if err != nil {
		return err
	}
	ibs := state.New(stateless)
	stateless.SetBlockNr(blockNr)

	gp = new(core.GasPool).AddGas(block.GasLimit()).AddBlobGas(cfg.chainConfig.GetMaxBlobGasPerBlock())
	usedGas = new(uint64)
	usedBlobGas = new(uint64)
	receipts = types.Receipts{}

	if err := core.InitializeBlockExecution(cfg.engine, chainReader, block.Header(), cfg.chainConfig, ibs, stateless, nil); err != nil {
		return err
	}
	header := block.Header()

	for i, txn := range block.Transactions() {
		ibs.SetTxContext(txn.Hash(), block.Hash(), i)
		receipt, _, err := core.ApplyTransaction(cfg.chainConfig, getHashFn, cfg.engine, nil, gp, ibs, stateless, header, txn, usedGas, usedBlobGas, vmConfig)
		if err != nil {
			return fmt.Errorf("tx %x failed: %v", txn.Hash(), err)
		}
		receipts = append(receipts, receipt)
	}

	receiptSha := types.DeriveSha(receipts)
	if !vmConfig.StatelessExec && cfg.chainConfig.IsByzantium(block.NumberU64()) && !vmConfig.NoReceipts && receiptSha != block.ReceiptHash() {
		return fmt.Errorf("mismatched receipt headers for block %d (%s != %s)", block.NumberU64(), receiptSha.Hex(), block.ReceiptHash().Hex())
	}

	if !vmConfig.StatelessExec && *usedGas != header.GasUsed {
		return fmt.Errorf("gas used by execution: %d, in header: %d", *usedGas, header.GasUsed)
	}

	if header.BlobGasUsed != nil && *usedBlobGas != *header.BlobGasUsed {
		return fmt.Errorf("blob gas used by execution: %d, in header: %d", *usedBlobGas, *header.BlobGasUsed)
	}

	var bloom types.Bloom
	if !vmConfig.NoReceipts {
		bloom = types.CreateBloom(receipts)
		if !vmConfig.StatelessExec && bloom != header.Bloom {
			return fmt.Errorf("bloom computed by execution: %x, in header: %x", bloom, header.Bloom)
		}
	}

	if !vmConfig.ReadOnly {
		_, _, _, err := cfg.engine.FinalizeAndAssemble(cfg.chainConfig, block.Header(), ibs, block.Transactions(), block.Uncles(), receipts, block.Withdrawals(), nil, nil, nil, nil)
		if err != nil {
			return err
		}

		rules := cfg.chainConfig.Rules(block.NumberU64(), header.Time)

		ibs.FinalizeTx(rules, stateless)

		if err := ibs.CommitBlock(rules, stateless); err != nil {
			return fmt.Errorf("committing block %d failed: %v", block.NumberU64(), err)
		}
	}

	if err = stateless.CheckRoot(header.Root); err != nil {
		return err
	}

	roots, err := tds.UpdateStateTrie()
	if err != nil {
		return err
	}

	if roots[len(roots)-1] != block.Root() {
		return fmt.Errorf("mismatch in expected state root computed %v vs %v indicates bug in witness implementation", roots[len(roots)-1], block.Root())
	}

	logger.Info("Witness generated successfully", "block", blockNr, "witness", buf.Bytes())

	// TODO: Persist witness

	return nil
}

// TODO: Implement
func UnwindWitnessStage() error {
	return nil
}

// TODO: Implement
func PruneWitnessStage() error {
	return nil
}
