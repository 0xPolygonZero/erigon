package jsonrpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/common/hexutil"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv/membatchwithdb"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	txpool_proto "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv"
	types2 "github.com/ledgerwatch/erigon-lib/types"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/tracers/logger"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	ethapi2 "github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/transactions"
	"github.com/ledgerwatch/erigon/turbo/trie"
)

var latestNumOrHash = rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)

// Call implements eth_call. Executes a new message call immediately without creating a transaction on the block chain.
func (api *APIImpl) Call(ctx context.Context, args ethapi2.CallArgs, blockNrOrHash rpc.BlockNumberOrHash, overrides *ethapi2.StateOverrides) (hexutility.Bytes, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}
	engine := api.engine()

	if args.Gas == nil || uint64(*args.Gas) == 0 {
		args.Gas = (*hexutil.Uint64)(&api.GasCap)
	}

	blockNumber, hash, _, err := rpchelper.GetCanonicalBlockNumber(blockNrOrHash, tx, api.filters) // DoCall cannot be executed on non-canonical blocks
	if err != nil {
		return nil, err
	}
	block, err := api.blockWithSenders(tx, hash, blockNumber)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}

	stateReader, err := rpchelper.CreateStateReader(ctx, tx, blockNrOrHash, 0, api.filters, api.stateCache, api.historyV3(tx), chainConfig.ChainName)
	if err != nil {
		return nil, err
	}
	header := block.HeaderNoCopy()
	result, err := transactions.DoCall(ctx, engine, args, tx, blockNrOrHash, header, overrides, api.GasCap, chainConfig, stateReader, api._blockReader, api.evmCallTimeout)
	if err != nil {
		return nil, err
	}

	if len(result.ReturnData) > api.ReturnDataLimit {
		return nil, fmt.Errorf("call returned result on length %d exceeding --rpc.returndata.limit %d", len(result.ReturnData), api.ReturnDataLimit)
	}

	// If the result contains a revert reason, try to unpack and return it.
	if len(result.Revert()) > 0 {
		return nil, ethapi2.NewRevertError(result)
	}

	return result.Return(), result.Err
}

// headerByNumberOrHash - intent to read recent headers only, tries from the lru cache before reading from the db
func headerByNumberOrHash(ctx context.Context, tx kv.Tx, blockNrOrHash rpc.BlockNumberOrHash, api *APIImpl) (*types.Header, error) {
	_, bNrOrHashHash, _, err := rpchelper.GetCanonicalBlockNumber(blockNrOrHash, tx, api.filters)
	if err != nil {
		return nil, err
	}
	block := api.tryBlockFromLru(bNrOrHashHash)
	if block != nil {
		return block.Header(), nil
	}

	blockNum, _, _, err := rpchelper.GetBlockNumber(blockNrOrHash, tx, api.filters)
	if err != nil {
		return nil, err
	}
	header, err := api._blockReader.HeaderByNumber(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}
	// header can be nil
	return header, nil
}

// EstimateGas implements eth_estimateGas. Returns an estimate of how much gas is necessary to allow the transaction to complete. The transaction will not be added to the blockchain.
func (api *APIImpl) EstimateGas(ctx context.Context, argsOrNil *ethapi2.CallArgs, blockNrOrHash *rpc.BlockNumberOrHash) (hexutil.Uint64, error) {
	var args ethapi2.CallArgs
	// if we actually get CallArgs here, we use them
	if argsOrNil != nil {
		args = *argsOrNil
	}

	dbtx, err := api.db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer dbtx.Rollback()

	// Binary search the gas requirement, as it may be higher than the amount used
	var (
		lo     = params.TxGas - 1
		hi     uint64
		gasCap uint64
	)
	// Use zero address if sender unspecified.
	if args.From == nil {
		args.From = new(libcommon.Address)
	}

	bNrOrHash := rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber)
	if blockNrOrHash != nil {
		bNrOrHash = *blockNrOrHash
	}

	// Determine the highest gas limit can be used during the estimation.
	if args.Gas != nil && uint64(*args.Gas) >= params.TxGas {
		hi = uint64(*args.Gas)
	} else {
		// Retrieve the block to act as the gas ceiling
		h, err := headerByNumberOrHash(ctx, dbtx, bNrOrHash, api)
		if err != nil {
			return 0, err
		}
		if h == nil {
			// if a block number was supplied and there is no header return 0
			if blockNrOrHash != nil {
				return 0, nil
			}

			// block number not supplied, so we haven't found a pending block, read the latest block instead
			h, err = headerByNumberOrHash(ctx, dbtx, latestNumOrHash, api)
			if err != nil {
				return 0, err
			}
			if h == nil {
				return 0, nil
			}
		}
		hi = h.GasLimit
	}

	var feeCap *big.Int
	if args.GasPrice != nil && (args.MaxFeePerGas != nil || args.MaxPriorityFeePerGas != nil) {
		return 0, errors.New("both gasPrice and (maxFeePerGas or maxPriorityFeePerGas) specified")
	} else if args.GasPrice != nil {
		feeCap = args.GasPrice.ToInt()
	} else if args.MaxFeePerGas != nil {
		feeCap = args.MaxFeePerGas.ToInt()
	} else {
		feeCap = libcommon.Big0
	}
	// Recap the highest gas limit with account's available balance.
	if feeCap.Sign() != 0 {
		cacheView, err := api.stateCache.View(ctx, dbtx)
		if err != nil {
			return 0, err
		}
		stateReader := state.NewCachedReader2(cacheView, dbtx)
		state := state.New(stateReader)
		if state == nil {
			return 0, fmt.Errorf("can't get the current state")
		}

		balance := state.GetBalance(*args.From) // from can't be nil
		available := balance.ToBig()
		if args.Value != nil {
			if args.Value.ToInt().Cmp(available) >= 0 {
				return 0, errors.New("insufficient funds for transfer")
			}
			available.Sub(available, args.Value.ToInt())
		}
		allowance := new(big.Int).Div(available, feeCap)

		// If the allowance is larger than maximum uint64, skip checking
		if allowance.IsUint64() && hi > allowance.Uint64() {
			transfer := args.Value
			if transfer == nil {
				transfer = new(hexutil.Big)
			}
			log.Warn("Gas estimation capped by limited funds", "original", hi, "balance", balance,
				"sent", transfer.ToInt(), "maxFeePerGas", feeCap, "fundable", allowance)
			hi = allowance.Uint64()
		}
	}

	// Recap the highest gas allowance with specified gascap.
	if hi > api.GasCap {
		log.Warn("Caller gas above allowance, capping", "requested", hi, "cap", api.GasCap)
		hi = api.GasCap
	}
	gasCap = hi

	chainConfig, err := api.chainConfig(dbtx)
	if err != nil {
		return 0, err
	}
	engine := api.engine()

	latestCanBlockNumber, latestCanHash, isLatest, err := rpchelper.GetCanonicalBlockNumber(latestNumOrHash, dbtx, api.filters) // DoCall cannot be executed on non-canonical blocks
	if err != nil {
		return 0, err
	}

	// try and get the block from the lru cache first then try DB before failing
	block := api.tryBlockFromLru(latestCanHash)
	if block == nil {
		block, err = api.blockWithSenders(dbtx, latestCanHash, latestCanBlockNumber)
		if err != nil {
			return 0, err
		}
	}
	if block == nil {
		return 0, fmt.Errorf("could not find latest block in cache or db")
	}

	stateReader, err := rpchelper.CreateStateReaderFromBlockNumber(ctx, dbtx, latestCanBlockNumber, isLatest, 0, api.stateCache, api.historyV3(dbtx), chainConfig.ChainName)
	if err != nil {
		return 0, err
	}
	header := block.HeaderNoCopy()

	caller, err := transactions.NewReusableCaller(engine, stateReader, nil, header, args, api.GasCap, latestNumOrHash, dbtx, api._blockReader, chainConfig, api.evmCallTimeout)
	if err != nil {
		return 0, err
	}

	// Create a helper to check if a gas allowance results in an executable transaction
	executable := func(gas uint64) (bool, *core.ExecutionResult, error) {
		result, err := caller.DoCallWithNewGas(ctx, gas)
		if err != nil {
			if errors.Is(err, core.ErrIntrinsicGas) {
				// Special case, raise gas limit
				return true, nil, nil
			}

			// Bail out
			return true, nil, err
		}
		return result.Failed(), result, nil
	}

	// Execute the binary search and hone in on an executable gas limit
	for lo+1 < hi {
		mid := (hi + lo) / 2
		failed, _, err := executable(mid)
		// If the error is not nil(consensus error), it means the provided message
		// call or transaction will never be accepted no matter how much gas it is
		// assigened. Return the error directly, don't struggle any more.
		if err != nil {
			return 0, err
		}
		if failed {
			lo = mid
		} else {
			hi = mid
		}
	}

	// Reject the transaction as invalid if it still fails at the highest allowance
	if hi == gasCap {
		failed, result, err := executable(hi)
		if err != nil {
			return 0, err
		}
		if failed {
			if result != nil && !errors.Is(result.Err, vm.ErrOutOfGas) {
				if len(result.Revert()) > 0 {
					return 0, ethapi2.NewRevertError(result)
				}
				return 0, result.Err
			}
			// Otherwise, the specified gas cap is too low
			return 0, fmt.Errorf("gas required exceeds allowance (%d)", gasCap)
		}
	}
	return hexutil.Uint64(hi), nil
}

// maxGetProofRewindBlockCount limits the number of blocks into the past that
// GetProof will allow computing proofs.  Because we must rewind the hash state
// and re-compute the state trie, the further back in time the request, the more
// computationally intensive the operation becomes.  The staged sync code
// assumes that if more than 100_000 blocks are skipped, that the entire trie
// should be re-computed. Re-computing the entire trie will currently take ~15
// minutes on mainnet.  The current limit has been chosen arbitrarily as
// 'useful' without likely being overly computationally intense.

// GetProof is partially implemented; no Storage proofs, and proofs must be for
// blocks within maxGetProofRewindBlockCount blocks of the head.
func (api *APIImpl) GetProof(ctx context.Context, address libcommon.Address, storageKeys []libcommon.Hash, blockNrOrHash rpc.BlockNumberOrHash) (*accounts.AccProofResult, error) {

	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if api.historyV3(tx) {
		return nil, fmt.Errorf("not supported by Erigon3")
	}

	blockNr, _, _, err := rpchelper.GetBlockNumber(blockNrOrHash, tx, api.filters)
	if err != nil {
		return nil, err
	}

	header, err := api._blockReader.HeaderByNumber(ctx, tx, blockNr)
	if err != nil {
		return nil, err
	}

	latestBlock, err := rpchelper.GetLatestBlockNumber(tx)
	if err != nil {
		return nil, err
	}

	if latestBlock < blockNr {
		// shouldn't happen, but check anyway
		return nil, fmt.Errorf("block number is in the future latest=%d requested=%d", latestBlock, blockNr)
	}

	rl := trie.NewRetainList(0)
	var loader *trie.FlatDBTrieLoader[libcommon.Hash]
	if blockNr < latestBlock {
		if latestBlock-blockNr > uint64(api.MaxGetProofRewindBlockCount) {
			return nil, fmt.Errorf("requested block is too old, block must be within %d blocks of the head block number (currently %d)", uint64(api.MaxGetProofRewindBlockCount), latestBlock)
		}
		batch := membatchwithdb.NewMemoryBatch(tx, api.dirs.Tmp)
		defer batch.Rollback()

		unwindState := &stagedsync.UnwindState{UnwindPoint: blockNr}
		stageState := &stagedsync.StageState{BlockNumber: latestBlock}

		hashStageCfg := stagedsync.StageHashStateCfg(nil, api.dirs, api.historyV3(batch))
		if err := stagedsync.UnwindHashStateStage(unwindState, stageState, batch, hashStageCfg, ctx, api.logger); err != nil {
			return nil, err
		}

		interHashStageCfg := stagedsync.StageTrieCfg(nil, false, false, false, api.dirs.Tmp, api._blockReader, nil, api.historyV3(batch), api._agg)
		loader, err = stagedsync.UnwindIntermediateHashesForTrieLoader("eth_getProof", rl, unwindState, stageState, batch, interHashStageCfg, nil, nil, ctx.Done(), api.logger)
		if err != nil {
			return nil, err
		}
		tx = batch
	} else {
		loader = trie.NewFlatDBTrieLoader[libcommon.Hash]("eth_getProof", rl, nil, nil, false, trie.NewRootHashAggregator(nil, nil, false))
	}

	reader, err := rpchelper.CreateStateReader(ctx, tx, blockNrOrHash, 0, api.filters, api.stateCache, api.historyV3(tx), "")
	if err != nil {
		return nil, err
	}
	a, err := reader.ReadAccountData(address)
	if err != nil {
		return nil, err
	}
	if a == nil {
		a = &accounts.Account{}
	}
	pr, err := trie.NewProofRetainer(address, a, storageKeys, rl)
	if err != nil {
		return nil, err
	}

	loader.Receiver().(*trie.RootHashAggregator).SetProofRetainer(pr)
	root, err := loader.Result(tx, nil)
	if err != nil {
		return nil, err
	}

	if root != header.Root {
		return nil, fmt.Errorf("mismatch in expected state root computed %v vs %v indicates bug in proof implementation", root, header.Root)
	}
	return pr.ProofResult()
}

func (api *APIImpl) GetWitness(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (hexutility.Bytes, error) {
	return api.getWitness(ctx, api.db, blockNrOrHash, api.logger)
}

func (api *BaseAPI) getWitness(ctx context.Context, db kv.RoDB, blockNrOrHash rpc.BlockNumberOrHash, logger log.Logger) (hexutility.Bytes, error) {
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if api.historyV3(tx) {
		return nil, fmt.Errorf("not supported by Erigon3")
	}

	blockNr, hash, _, err := rpchelper.GetCanonicalBlockNumber(blockNrOrHash, tx, api.filters) // DoCall cannot be executed on non-canonical blocks

	if err != nil {
		return nil, err
	}

	// Witness for genesis block is empty
	if blockNr == 0 {
		w := trie.NewWitness(make([]trie.WitnessOperator, 0))

		var buf bytes.Buffer
		_, err = w.WriteInto(&buf)
		if err != nil {
			return nil, err
		}

		return buf.Bytes(), nil
	}

	block, err := api.blockWithSenders(tx, hash, blockNr)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}

	prevHeader, err := api._blockReader.HeaderByNumber(ctx, tx, blockNr-1)
	if err != nil {
		return nil, err
	}

	latestBlock, err := rpchelper.GetLatestBlockNumber(tx)
	if err != nil {
		return nil, err
	}

	if latestBlock < blockNr {
		// shouldn't happen, but check anyway
		return nil, fmt.Errorf("block number is in the future latest=%d requested=%d", latestBlock, blockNr)
	}

	rl := trie.NewRetainList(0)

	if blockNr-1 < latestBlock {
		batch := membatchwithdb.NewMemoryBatch(tx, api.dirs.Tmp)
		defer batch.Rollback()

		unwindState := &stagedsync.UnwindState{UnwindPoint: blockNr - 1}
		stageState := &stagedsync.StageState{BlockNumber: latestBlock}

		hashStageCfg := stagedsync.StageHashStateCfg(nil, api.dirs, api.historyV3(batch))
		if err := stagedsync.UnwindHashStateStage(unwindState, stageState, batch, hashStageCfg, ctx, logger); err != nil {
			return nil, err
		}

		interHashStageCfg := stagedsync.StageTrieCfg(nil, false, false, false, api.dirs.Tmp, api._blockReader, nil, api.historyV3(batch), api._agg)
		err = stagedsync.UnwindIntermediateHashes("eth_getWitness", rl, unwindState, stageState, batch, interHashStageCfg, ctx.Done(), logger)
		if err != nil {
			return nil, err
		}
		tx = batch
	}

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	header := block.Header()

	reader, err := rpchelper.CreateHistoryStateReader(tx, blockNr, 0, false, chainConfig.ChainName)
	if err != nil {
		return nil, err
	}

	tds := state.NewTrieDbState(prevHeader.Root, tx, blockNr-1, reader)
	tds.SetRetainList(rl)
	tds.SetResolveReads(true)

	tds.StartNewBuffer()
	trieStateWriter := tds.TrieStateWriter()

	statedb := state.New(tds)

	// var slotV uint256.Int
	// var slotV2 uint256.Int
	// slot := libcommon.HexToHash("0xe0ab2193e40c80a14d05812b70d9e17b2722b1b15a9a2b7ce4860cd58844cdea")
	// slot2 := libcommon.HexToHash("0xabd7b398c2237712843e3e780dcd40dfb99446b30666f04c025da4efa5ce5177")
	// statedb.GetState(libcommon.HexToAddress("0xec59ea1acb9fc9f630b2dce73790ed8be0ac036e"), &slot, &slotV)
	// statedb.GetState(libcommon.HexToAddress("0xec59ea1acb9fc9f630b2dce73790ed8be0ac036e"), &slot2, &slotV2)

	// fmt.Printf("slot %x %x\n", slot, slotV)
	// fmt.Printf("slot %x %x\n", slot2, slotV2)

	getHeader := func(hash libcommon.Hash, number uint64) *types.Header {
		h, e := api._blockReader.Header(ctx, tx, hash, number)
		if e != nil {
			log.Error("getHeader error", "number", number, "hash", hash, "err", e)
		}
		return h
	}

	getHashFn := core.GetHashFn(block.Header(), getHeader)

	usedGas := new(uint64)
	usedBlobGas := new(uint64)
	gp := new(core.GasPool).AddGas(block.GasLimit()).AddBlobGas(chainConfig.GetMaxBlobGasPerBlock())
	var receipts types.Receipts

	engine, ok := api.engine().(consensus.Engine)

	if !ok {
		return nil, fmt.Errorf("engine is not consensus.Engine")
	}

	chainReader := stagedsync.NewChainReaderImpl(chainConfig, tx, nil, nil)

	if err := core.InitializeBlockExecution(engine, chainReader, block.Header(), chainConfig, statedb, trieStateWriter, nil); err != nil {
		if err != nil {
			return nil, err
		}
	}

	if len(block.Transactions()) == 0 {
		statedb.GetBalance(libcommon.HexToAddress("0x1234"))
	}

	vmConfig := vm.Config{}

	for i, txn := range block.Transactions() {
		statedb.SetTxContext(txn.Hash(), block.Hash(), i)
		receipt, _, err := core.ApplyTransaction(chainConfig, getHashFn, api.engine(), nil, gp, statedb, trieStateWriter, block.Header(), txn, usedGas, usedBlobGas, vmConfig)
		if err != nil {
			return nil, err
		}

		if !chainConfig.IsByzantium(block.NumberU64()) {
			tds.StartNewBuffer()
		}

		receipts = append(receipts, receipt)
	}

	if _, _, _, err = engine.FinalizeAndAssemble(chainConfig, block.Header(), statedb, block.Transactions(), block.Uncles(), receipts, block.Withdrawals(), nil, nil, nil, nil); err != nil {
		fmt.Printf("Finalize of block %d failed: %v\n", blockNr, err)
		return nil, err
	}

	statedb.FinalizeTx(chainConfig.Rules(block.NumberU64(), block.Header().Time), trieStateWriter)

	loadFunc := func(loader *trie.SubTrieLoader, rl *trie.RetainList, dbPrefixes [][]byte, fixedbits []int, accountNibbles [][]byte) (trie.SubTries, error) {
		rl.Rewind()
		receiver := trie.NewSubTrieAggregator(nil, nil, false)
		receiver.SetRetainList(rl)
		pr := trie.NewMultiAccountProofRetainer(rl)
		pr.AccHexKeys = accountNibbles
		receiver.SetProofRetainer(pr)
		subTrieloader := trie.NewFlatDBTrieLoader[trie.SubTries]("eth_getWitness", rl, nil, nil, false, receiver)
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

	triePreroot := tds.LastRoot()

	if !bytes.Equal(prevHeader.Root[:], triePreroot[:]) {
		return nil, fmt.Errorf("mismatch in expected state root computed %v vs %v indicates bug in witness implementation", prevHeader.Root, triePreroot)
	}

	if err := tds.ResolveStateTrieWithFunc(loadFunc); err != nil {
		return nil, err
	}

	w, err := tds.ExtractWitness(false, false)

	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	_, err = w.WriteInto(&buf)
	if err != nil {
		return nil, err
	}

	nw, err := trie.NewWitnessFromReader(bytes.NewReader(buf.Bytes()), false)

	if err != nil {
		return nil, err
	}

	s, err := state.NewStateless(prevHeader.Root, nw, blockNr-1, false, false /* is binary */)
	if err != nil {
		return nil, err
	}
	ibs := state.New(s)
	s.SetBlockNr(blockNr)

	gp = new(core.GasPool).AddGas(block.GasLimit()).AddBlobGas(chainConfig.GetMaxBlobGasPerBlock())
	usedGas = new(uint64)
	usedBlobGas = new(uint64)
	receipts = types.Receipts{}

	if err := core.InitializeBlockExecution(engine, chainReader, block.Header(), chainConfig, ibs, s, nil); err != nil {
		return nil, err
	}

	for i, txn := range block.Transactions() {
		ibs.SetTxContext(txn.Hash(), block.Hash(), i)
		receipt, _, err := core.ApplyTransaction(chainConfig, getHashFn, engine, nil, gp, ibs, s, header, txn, usedGas, usedBlobGas, vmConfig)
		if err != nil {
			return nil, fmt.Errorf("tx %x failed: %v", txn.Hash(), err)
		}
		receipts = append(receipts, receipt)
	}

	receiptSha := types.DeriveSha(receipts)
	if !vmConfig.StatelessExec && chainConfig.IsByzantium(header.Number.Uint64()) && !vmConfig.NoReceipts && receiptSha != block.ReceiptHash() {
		return nil, fmt.Errorf("mismatched receipt headers for block %d (%s != %s)", block.NumberU64(), receiptSha.Hex(), block.ReceiptHash().Hex())
	}

	if !vmConfig.StatelessExec && *usedGas != header.GasUsed {
		return nil, fmt.Errorf("gas used by execution: %d, in header: %d", *usedGas, header.GasUsed)
	}

	if header.BlobGasUsed != nil && *usedBlobGas != *header.BlobGasUsed {
		return nil, fmt.Errorf("blob gas used by execution: %d, in header: %d", *usedBlobGas, *header.BlobGasUsed)
	}

	var bloom types.Bloom
	if !vmConfig.NoReceipts {
		bloom = types.CreateBloom(receipts)
		if !vmConfig.StatelessExec && bloom != header.Bloom {
			return nil, fmt.Errorf("bloom computed by execution: %x, in header: %x", bloom, header.Bloom)
		}
	}

	if !vmConfig.ReadOnly {
		_, _, _, err := engine.FinalizeAndAssemble(chainConfig, block.Header(), ibs, block.Transactions(), block.Uncles(), receipts, block.Withdrawals(), nil, nil, nil, nil)
		if err != nil {
			return nil, err
		}

		rules := chainConfig.Rules(block.NumberU64(), header.Time)

		ibs.FinalizeTx(rules, s)

		if err := ibs.CommitBlock(rules, s); err != nil {
			return nil, fmt.Errorf("committing block %d failed: %v", block.NumberU64(), err)
		}
	}

	if err = s.CheckRoot(header.Root); err != nil {
		return nil, err
	}

	roots, err := tds.UpdateStateTrie()

	if err != nil {
		return nil, err
	}

	if roots[len(roots)-1] != block.Root() {
		return nil, fmt.Errorf("mismatch in expected state root computed %v vs %v indicates bug in witness implementation", roots[len(roots)-1], block.Root())
	}

	return buf.Bytes(), nil
}

func (api *APIImpl) tryBlockFromLru(hash libcommon.Hash) *types.Block {
	var block *types.Block
	if api.blocksLRU != nil {
		if it, ok := api.blocksLRU.Get(hash); ok && it != nil {
			block = it
		}
	}
	return block
}

// accessListResult returns an optional accesslist
// Its the result of the `eth_createAccessList` RPC call.
// It contains an error if the transaction itself failed.
type accessListResult struct {
	Accesslist *types2.AccessList `json:"accessList"`
	Error      string             `json:"error,omitempty"`
	GasUsed    hexutil.Uint64     `json:"gasUsed"`
}

// CreateAccessList implements eth_createAccessList. It creates an access list for the given transaction.
// If the accesslist creation fails an error is returned.
// If the transaction itself fails, an vmErr is returned.
func (api *APIImpl) CreateAccessList(ctx context.Context, args ethapi2.CallArgs, blockNrOrHash *rpc.BlockNumberOrHash, optimizeGas *bool) (*accessListResult, error) {
	bNrOrHash := rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber)
	if blockNrOrHash != nil {
		bNrOrHash = *blockNrOrHash
	}

	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}
	engine := api.engine()

	blockNumber, hash, latest, err := rpchelper.GetCanonicalBlockNumber(bNrOrHash, tx, api.filters) // DoCall cannot be executed on non-canonical blocks
	if err != nil {
		return nil, err
	}
	block, err := api.blockWithSenders(tx, hash, blockNumber)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	var stateReader state.StateReader
	if latest {
		cacheView, err := api.stateCache.View(ctx, tx)
		if err != nil {
			return nil, err
		}
		stateReader = state.NewCachedReader2(cacheView, tx)
	} else {
		stateReader, err = rpchelper.CreateHistoryStateReader(tx, blockNumber+1, 0, api.historyV3(tx), chainConfig.ChainName)
		if err != nil {
			return nil, err
		}
	}

	header := block.Header()
	// If the gas amount is not set, extract this as it will depend on access
	// lists and we'll need to reestimate every time
	nogas := args.Gas == nil

	var to libcommon.Address
	if args.To != nil {
		to = *args.To
	} else {
		// Require nonce to calculate address of created contract
		if args.Nonce == nil {
			var nonce uint64
			reply, err := api.txPool.Nonce(ctx, &txpool_proto.NonceRequest{
				Address: gointerfaces.ConvertAddressToH160(*args.From),
			}, &grpc.EmptyCallOption{})
			if err != nil {
				return nil, err
			}
			if reply.Found {
				nonce = reply.Nonce + 1
			}
			args.Nonce = (*hexutil.Uint64)(&nonce)
		}
		to = crypto.CreateAddress(*args.From, uint64(*args.Nonce))
	}

	if args.From == nil {
		args.From = &libcommon.Address{}
	}

	// Retrieve the precompiles since they don't need to be added to the access list
	precompiles := vm.ActivePrecompiles(chainConfig.Rules(blockNumber, header.Time))
	excl := make(map[libcommon.Address]struct{})
	for _, pc := range precompiles {
		excl[pc] = struct{}{}
	}

	// Create an initial tracer
	prevTracer := logger.NewAccessListTracer(nil, excl, nil)
	if args.AccessList != nil {
		prevTracer = logger.NewAccessListTracer(*args.AccessList, excl, nil)
	}
	for {
		state := state.New(stateReader)
		// Retrieve the current access list to expand
		accessList := prevTracer.AccessList()
		log.Trace("Creating access list", "input", accessList)

		// If no gas amount was specified, each unique access list needs it's own
		// gas calculation. This is quite expensive, but we need to be accurate
		// and it's convered by the sender only anyway.
		if nogas {
			args.Gas = nil
		}
		// Set the accesslist to the last al
		args.AccessList = &accessList

		var msg types.Message

		var baseFee *uint256.Int = nil
		// check if EIP-1559
		if header.BaseFee != nil {
			baseFee, _ = uint256.FromBig(header.BaseFee)
		}

		msg, err = args.ToMessage(api.GasCap, baseFee)
		if err != nil {
			return nil, err
		}

		// Apply the transaction with the access list tracer
		tracer := logger.NewAccessListTracer(accessList, excl, state)
		config := vm.Config{Tracer: tracer, Debug: true, NoBaseFee: true}
		blockCtx := transactions.NewEVMBlockContext(engine, header, bNrOrHash.RequireCanonical, tx, api._blockReader)
		txCtx := core.NewEVMTxContext(msg)

		evm := vm.NewEVM(blockCtx, txCtx, state, chainConfig, config)
		gp := new(core.GasPool).AddGas(msg.Gas()).AddBlobGas(msg.BlobGas())
		res, err := core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */)
		if err != nil {
			return nil, err
		}
		if tracer.Equal(prevTracer) {
			var errString string
			if res.Err != nil {
				errString = res.Err.Error()
			}
			accessList := &accessListResult{Accesslist: &accessList, Error: errString, GasUsed: hexutil.Uint64(res.UsedGas)}
			if optimizeGas == nil || *optimizeGas { // optimize gas unless explicitly told not to
				optimizeWarmAddrInAccessList(accessList, *args.From)
				optimizeWarmAddrInAccessList(accessList, to)
				optimizeWarmAddrInAccessList(accessList, header.Coinbase)
				for addr := range tracer.CreatedContracts() {
					if !tracer.UsedBeforeCreation(addr) {
						optimizeWarmAddrInAccessList(accessList, addr)
					}
				}
			}
			return accessList, nil
		}
		prevTracer = tracer
	}
}

// some addresses (like sender, recipient, block producer, and created contracts)
// are considered warm already, so we can save by adding these to the access list
// only if we are adding a lot of their respective storage slots as well
func optimizeWarmAddrInAccessList(accessList *accessListResult, addr libcommon.Address) {
	indexToRemove := -1

	for i := 0; i < len(*accessList.Accesslist); i++ {
		entry := (*accessList.Accesslist)[i]
		if entry.Address != addr {
			continue
		}

		// https://eips.ethereum.org/EIPS/eip-2930#charging-less-for-accesses-in-the-access-list
		accessListSavingPerSlot := params.ColdSloadCostEIP2929 - params.WarmStorageReadCostEIP2929 - params.TxAccessListStorageKeyGas

		numSlots := uint64(len(entry.StorageKeys))
		if numSlots*accessListSavingPerSlot <= params.TxAccessListAddressGas {
			indexToRemove = i
		}
	}

	if indexToRemove >= 0 {
		*accessList.Accesslist = removeIndex(*accessList.Accesslist, indexToRemove)
	}
}

func removeIndex(s types2.AccessList, index int) types2.AccessList {
	return append(s[:index], s[index+1:]...)
}
