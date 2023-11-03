package native

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"

	"sync/atomic"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/log/v3"
)

//go:generate go run github.com/fjl/gencodec -type account -field-override accountMarshaling -out gen_account_json.go

func init() {
	register("zeroTracer", newZeroTracer)
}

type zeroTracer struct {
	noopTracer // stub struct to mock not used interface methods
	env        vm.VMInterface
	tx         types.TxnInfo
	gasLimit   uint64      // Amount of gas bought for the whole tx
	interrupt  atomic.Bool // Atomic flag to signal execution interruption
	reason     error       // Textual reason for the interruption
	ctx        *tracers.Context
	to         *libcommon.Address
	txStatus   uint64
}

func newZeroTracer(ctx *tracers.Context, cfg json.RawMessage) (tracers.Tracer, error) {
	return &zeroTracer{
		tx: types.TxnInfo{
			Traces: make(map[libcommon.Address]*types.TxnTrace),
		},
		ctx: ctx,
	}, nil
}

// CaptureStart implements the EVMLogger interface to initialize the tracing operation.
func (t *zeroTracer) CaptureStart(env vm.VMInterface, from libcommon.Address, to libcommon.Address, precompile, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.to = &to
	t.env = env
	t.tx.Meta.ByteCode = input

	t.addAccountToTrace(from, false)
	t.addAccountToTrace(to, false)
	t.addAccountToTrace(env.Context().Coinbase, false)

	// The recipient balance includes the value transferred.
	toBal := new(big.Int).Sub(t.tx.Traces[to].Balance.ToBig(), value.ToBig())
	t.tx.Traces[to].Balance = uint256.MustFromBig(toBal)

	// The sender balance is after reducing: value and gasLimit.
	// We need to re-add them to get the pre-tx balance.
	fromBal := new(big.Int).Set(t.tx.Traces[from].Balance.ToBig())
	gasPrice := env.TxContext().GasPrice
	consumedGas := new(big.Int).Mul(gasPrice.ToBig(), new(big.Int).SetUint64(t.gasLimit))
	fromBal.Add(fromBal, new(big.Int).Add(value.ToBig(), consumedGas))
	t.tx.Traces[from].Balance = uint256.MustFromBig(fromBal)
	if t.tx.Traces[from].Nonce.Cmp(uint256.NewInt(0)) > 0 {
		t.tx.Traces[from].Nonce.Sub(t.tx.Traces[from].Nonce, uint256.NewInt(1))
	}
}

func (t *zeroTracer) CaptureTxStart(gasLimit uint64) {
	t.gasLimit = gasLimit
}

// CaptureState implements the EVMLogger interface to trace a single step of VM execution.
func (t *zeroTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	if err != nil {
		return
	}

	// Skip if tracing was interrupted
	if t.interrupt.Load() {
		return
	}

	stack := scope.Stack
	stackData := stack.Data
	stackLen := len(stackData)
	caller := scope.Contract.Address()

	switch {
	case stackLen >= 1 && op == vm.SLOAD:
		slot := libcommon.Hash(stackData[stackLen-1].Bytes32())
		t.addSLOADToAccount(caller, slot)
	case stackLen >= 1 && op == vm.SSTORE:
		slot := libcommon.Hash(stackData[stackLen-1].Bytes32())
		t.addSSTOREToAccount(caller, slot, stackData[stackLen-2].Clone())
	case stackLen >= 1 && (op == vm.EXTCODECOPY || op == vm.EXTCODEHASH || op == vm.EXTCODESIZE || op == vm.BALANCE || op == vm.SELFDESTRUCT):
		addr := libcommon.Address(stackData[stackLen-1].Bytes20())
		t.addAccountToTrace(addr, false)
	case stackLen >= 5 && (op == vm.DELEGATECALL || op == vm.CALL || op == vm.STATICCALL || op == vm.CALLCODE):
		addr := libcommon.Address(stackData[stackLen-2].Bytes20())
		t.addAccountToTrace(addr, false)
	case op == vm.CREATE:
		nonce := t.env.IntraBlockState().GetNonce(caller)
		addr := crypto.CreateAddress(caller, nonce)
		t.addAccountToTrace(addr, true)
	case stackLen >= 4 && op == vm.CREATE2:
		offset := stackData[stackLen-2]
		size := stackData[stackLen-3]
		init, err := GetMemoryCopyPadded(scope.Memory, int64(offset.Uint64()), int64(size.Uint64()))
		if err != nil {
			log.Warn("failed to copy CREATE2 input", "err", err, "tracer", "prestateTracer", "offset", offset, "size", size)
			return
		}
		inithash := crypto.Keccak256(init)
		salt := stackData[stackLen-4]
		addr := crypto.CreateAddress2(caller, salt.Bytes32(), inithash)
		t.addAccountToTrace(addr, true)
	}
}

const (
	memoryPadLimit = 1024 * 1024
)

// GetMemoryCopyPadded returns offset + size as a new slice.
// It zero-pads the slice if it extends beyond memory bounds.
func GetMemoryCopyPadded(m *vm.Memory, offset, size int64) ([]byte, error) {
	if offset < 0 || size < 0 {
		return nil, errors.New("offset or size must not be negative")
	}
	if int(offset+size) < m.Len() { // slice fully inside memory
		return m.GetCopy(offset, size), nil
	}
	paddingNeeded := int(offset+size) - m.Len()
	if paddingNeeded > memoryPadLimit {
		return nil, fmt.Errorf("reached limit for padding memory slice: %d", paddingNeeded)
	}
	cpy := make([]byte, size)
	if overlap := int64(m.Len()) - offset; overlap > 0 {
		copy(cpy, m.GetPtr(offset, overlap))
	}
	return cpy, nil
}

func (t *zeroTracer) CaptureTxEnd(restGas uint64) {
	t.tx.Meta.GasUsed = t.gasLimit - restGas
	*t.ctx.CumulativeGasUsed += t.tx.Meta.GasUsed

	toPop := make([]libcommon.Address, 0)

	for addr := range t.tx.Traces {
		trace := t.tx.Traces[addr]
		newBalance := t.env.IntraBlockState().GetBalance(addr)
		newNonce := uint256.NewInt(t.env.IntraBlockState().GetNonce(addr))
		codeHash := t.env.IntraBlockState().GetCodeHash(addr)
		code := t.env.IntraBlockState().GetCode(addr)

		changed := false

		if newBalance.Cmp(trace.Balance) != 0 {
			trace.Balance = newBalance
			changed = true
		} else {
			trace.Balance = nil
		}

		if newNonce.Cmp(trace.Nonce) != 0 {
			trace.Nonce = newNonce
			changed = true
		} else {
			trace.Nonce = nil
		}

		if len(trace.StorageReadMap) > 0 {
			trace.StorageRead = make([]libcommon.Hash, 0, len(trace.StorageReadMap))
			for k := range trace.StorageReadMap {
				trace.StorageRead = append(trace.StorageRead, k)
			}
			changed = true
		} else {
			trace.StorageRead = nil
		}

		if len(trace.StorageWritten) == 0 {
			trace.StorageWritten = nil
		} else {
			changed = true
		}

		if !bytes.Equal(codeHash[:], trace.CodeUsage.Read[:]) {
			trace.CodeUsage.Read = nil
			trace.CodeUsage.Write = code
			changed = true
		} else if code != nil {
			trace.CodeUsage.Read = &codeHash
		}

		if trace.CodeUsage.Read != nil && code == nil {
			trace.CodeUsage = nil
		}

		if !changed {
			toPop = append(toPop, addr)
		}
	}

	for _, addr := range toPop {
		delete(t.tx.Traces, addr)
	}

	receipt := &types.Receipt{Type: types.LegacyTxType, CumulativeGasUsed: *t.ctx.CumulativeGasUsed}
	receipt.Status = t.txStatus
	receipt.TxHash = t.ctx.Txn.Hash()
	receipt.GasUsed = t.tx.Meta.GasUsed

	// if the transaction created a contract, store the creation address in the receipt.
	if t.to == nil {
		receipt.ContractAddress = crypto.CreateAddress(t.env.TxContext().Origin, t.ctx.Txn.GetNonce())
	}
	// Set the receipt logs and create a bloom for filtering
	receipt.Logs = t.env.IntraBlockState().GetLogs(t.ctx.Txn.Hash())
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockNumber = big.NewInt(0).SetUint64(t.ctx.BlockNum)
	receipt.TransactionIndex = uint(t.ctx.TxIndex)

	receiptBuffer := &bytes.Buffer{}
	encodeErr := receipt.EncodeRLP(receiptBuffer)

	if encodeErr != nil {
		log.Error("failed to encode receipt", "err", encodeErr)
		return
	}

	t.tx.Meta.NewReceiptTrieNode = receiptBuffer.Bytes()

	txBuffer := &bytes.Buffer{}
	encodeErr = t.ctx.Txn.EncodeRLP(txBuffer)

	if encodeErr != nil {
		log.Error("failed to encode transaction", "err", encodeErr)
		return
	}

	t.tx.Meta.NewTxnTrieNode = txBuffer.Bytes()
}

func (t *zeroTracer) CaptureEnd(output []byte, gasUsed uint64, err error) {
	if err != nil {
		t.txStatus = types.ReceiptStatusFailed
	} else {
		t.txStatus = types.ReceiptStatusSuccessful
	}
}

// GetResult returns the json-encoded nested list of call traces, and any
// error arising from the encoding or forceful termination (via `Stop`).
func (t *zeroTracer) GetResult() (json.RawMessage, error) {
	var res []byte
	var err error
	res, err = json.Marshal(t.tx)

	if err != nil {
		return nil, err
	}

	return json.RawMessage(res), t.reason
}

func (t *zeroTracer) GetTxnInfo() types.TxnInfo {
	return t.tx
}

// Stop terminates execution of the tracer at the first opportune moment.
func (t *zeroTracer) Stop(err error) {
	t.reason = err
	t.interrupt.Store(true)
}

func (t *zeroTracer) addAccountToTrace(addr libcommon.Address, created bool) {
	if _, ok := t.tx.Traces[addr]; ok {
		return
	}

	nonce := uint256.NewInt(t.env.IntraBlockState().GetNonce(addr))
	codeHash := t.env.IntraBlockState().GetCodeHash(addr)

	t.tx.Traces[addr] = &types.TxnTrace{
		Balance:        t.env.IntraBlockState().GetBalance(addr),
		Nonce:          nonce,
		CodeUsage:      &types.ContractCodeUsage{Read: &codeHash},
		StorageWritten: make(map[libcommon.Hash]*uint256.Int),
		StorageRead:    make([]libcommon.Hash, 0),
		StorageReadMap: make(map[libcommon.Hash]struct{}),
	}
}

func (t *zeroTracer) addSLOADToAccount(addr libcommon.Address, key libcommon.Hash) {
	var value uint256.Int
	t.env.IntraBlockState().GetState(addr, &key, &value)
	t.tx.Traces[addr].StorageReadMap[key] = struct{}{}
}

func (t *zeroTracer) addSSTOREToAccount(addr libcommon.Address, key libcommon.Hash, value *uint256.Int) {
	t.tx.Traces[addr].StorageWritten[key] = value
}
