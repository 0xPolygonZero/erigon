package state

import (
	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/dbutils"
	"github.com/ledgerwatch/erigon-lib/metrics"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/ledgerwatch/log/v3"
)

var (
	InsertCounter = metrics.GetOrCreateCounter("db_ih_insert")
	DeleteCounter = metrics.GetOrCreateCounter("db_ih_delete")
)

const keyBufferSize = 64

type IntermediateHashes struct {
	trie.NoopObserver // make sure that we don't need to subscribe to unnecessary methods
	putter            kv.Putter
	deleter           kv.Deleter
}

func NewIntermediateHashes(putter kv.Putter, deleter kv.Deleter) *IntermediateHashes {
	return &IntermediateHashes{putter: putter, deleter: deleter}
}

func (ih *IntermediateHashes) WillUnloadBranchNode(prefixAsNibbles []byte, nodeHash libcommon.Hash, incarnation uint64) {
	// only put to bucket prefixes with even number of nibbles
	if len(prefixAsNibbles) == 0 || len(prefixAsNibbles)%2 == 1 {
		return
	}

	InsertCounter.Inc()

	buf := make([]byte, keyBufferSize)
	hexutil.CompressNibbles(prefixAsNibbles, &buf)

	var key []byte
	if len(buf) >= length.Hash {
		key = dbutils.GenerateCompositeStoragePrefix(buf[:length.Hash], incarnation, buf[length.Hash:])
	} else {
		key = common.CopyBytes(buf)
	}

	if err := ih.putter.Put(kv.IntermediateTrieHash, key, common.CopyBytes(nodeHash[:])); err != nil {
		log.Warn("could not put intermediate trie hash", "err", err)
	}
}

func (ih *IntermediateHashes) BranchNodeLoaded(prefixAsNibbles []byte, incarnation uint64) {
	// only put to bucket prefixes with even number of nibbles
	if len(prefixAsNibbles) == 0 || len(prefixAsNibbles)%2 == 1 {
		return
	}
	DeleteCounter.Inc()

	buf := make([]byte, keyBufferSize)

	var key []byte
	if len(buf) >= length.Hash {
		key = dbutils.GenerateCompositeStoragePrefix(buf[:length.Hash], incarnation, buf[length.Hash:])
	} else {
		key = common.CopyBytes(buf)
	}

	if err := ih.deleter.Delete(kv.IntermediateTrieHash, key); err != nil {
		log.Warn("could not delete intermediate trie hash", "err", err)
	}
}
