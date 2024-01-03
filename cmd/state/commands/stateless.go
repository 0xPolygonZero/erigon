package commands

import (
	"context"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cmd/state/stateless"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

var (
	statefile         string
	triesize          uint32
	stopBlock         uint64
	preroot           bool
	snapshotInterval  uint64
	snapshotFrom      uint64
	witnessInterval   uint64
	noverify          bool
	bintries          bool
	starkBlocksFile   string
	starkStatsBase    string
	statelessResolver bool
	witnessDatabase   string
	writeHistory      bool
	blockSource       string
	genesisFile       string
)

func withBlocksource(cmd *cobra.Command) {
	cmd.Flags().StringVar(&blockSource, "blockSource", "", "Path to the block source: `db:///path/to/chaindata` or `exportfile:///path/to/my/exportfile`")
	if err := cmd.MarkFlagRequired("blockSource"); err != nil {
		panic(err)
	}
}

func init() {
	withStatsfile(statelessCmd)
	withBlock(statelessCmd)
	// withBlocksource(statelessCmd)
	withChain(statelessCmd)
	withDataDir(statelessCmd)

	statelessCmd.Flags().StringVar(&statefile, "statefile", "state", "path to the file where the state will be periodically written during the analysis")
	statelessCmd.Flags().Uint64Var(&stopBlock, "stopBlock", 0, "block number to stop at (0 - do not stop)")
	statelessCmd.Flags().Uint32Var(&triesize, "triesize", 4*1024*1024, "maximum size of a trie in bytes")
	statelessCmd.Flags().BoolVar(&preroot, "preroot", false, "Attempt to compute hash of the trie without modifying it")
	statelessCmd.Flags().Uint64Var(&snapshotInterval, "snapshotInterval", 1, "how often to take snapshots (0 - never, 1 - every block, 1000 - every 1000th block, etc)")
	statelessCmd.Flags().Uint64Var(&snapshotFrom, "snapshotFrom", 0, "from which block to start snapshots")
	statelessCmd.Flags().Uint64Var(&witnessInterval, "witnessInterval", 1, "after which block to extract witness (put a large number like 10000000 to disable)")
	statelessCmd.Flags().BoolVar(&noverify, "noVerify", false, "skip snapshot verification on loading")
	statelessCmd.Flags().BoolVar(&bintries, "bintries", false, "use binary tries instead of hexary to generate/load block witnesses")
	statelessCmd.Flags().StringVar(&starkBlocksFile, "starkBlocksFile", "", "file with the list of blocks for which to produce stark data")
	statelessCmd.Flags().StringVar(&starkStatsBase, "starkStatsBase", "stark_stats", "template for names of the files to write stark stats in")
	statelessCmd.Flags().BoolVar(&statelessResolver, "statelessResolver", false, "use a witness DB instead of the state when resolving tries")
	statelessCmd.Flags().StringVar(&witnessDatabase, "witnessDbFile", "", "optional path to a database where to store witnesses (empty string -- do not store witnesses")
	statelessCmd.Flags().BoolVar(&writeHistory, "writeHistory", false, "write history buckets and changeset buckets into the statefile")
	// statelessCmd.Flags().StringVar(&genesisFile, "genesisFile", "", "A genesis file that was used to init the chain")
	if err := statelessCmd.MarkFlagFilename("witnessDbFile", ""); err != nil {
		panic(err)
	}

	rootCmd.AddCommand(statelessCmd)

}

var statelessCmd = &cobra.Command{
	Use:   "stateless",
	Short: "Stateless Ethereum prototype",
	RunE: func(cmd *cobra.Command, args []string) error {
		createDb := func(path string) (kv.RwDB, error) {
			db, err := mdbx.NewMDBX(log.New()).
				Path(path).
				WriteMap().
				MapSize(4 * datasize.TB).
				Open(context.Background())
			return db, err
		}
		ctx := rootContext()

		stateless.Stateless(
			ctx,
			block,
			stopBlock,
			datadirCli,
			statefile,
			triesize,
			preroot,
			snapshotInterval,
			snapshotFrom,
			witnessInterval,
			statsfile,
			!noverify,
			bintries,
			createDb,
			starkBlocksFile,
			starkStatsBase,
			statelessResolver,
			witnessDatabase,
			writeHistory,
			genesis,
			chain,
		)

		return nil
	},
}
