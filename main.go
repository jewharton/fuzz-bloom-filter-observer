package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"time"

	"github.com/spf13/cobra"
	"github.com/zeebo/errs"
	"go.uber.org/zap"

	"storj.io/common/memory"
	"storj.io/common/storj"
	"storj.io/common/testrand"
	"storj.io/storj/satellite/gc/bloomfilter"
	"storj.io/storj/satellite/metabase"
	"storj.io/storj/satellite/metabase/rangedloop"
)

var (
	numNodes            int
	maxForks            int
	maxSegments         int
	maxPiecesPerSegment uint16

	//useSyncObserver   bool
	maxFilterSize     int64
	falsePositiveRate float64
	seed              int64

	duration time.Duration
)

var cmd = &cobra.Command{
	Use:           "fuzz-bloom-filter-observer",
	SilenceErrors: true,
	SilenceUsage:  true,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		nodeIDs := make([]storj.NodeID, 0, numNodes)
		segments := make([]rangedloop.Segment, 0, maxSegments)
		pieces := make([]metabase.Piece, 0, maxSegments*int(maxPiecesPerSegment))
		pieceCounts := make(mockOverlay, numNodes)
		forkSegmentCounts := make([]int, 0, maxForks)

		if seed == 0 {
			seed = time.Now().Unix()
		}
		rand.Seed(seed)

		fmt.Printf("Running... (seed: %d)\n", seed)

		now := time.Now()
		for i := int64(0); time.Now().Before(now.Add(duration)); i++ {
			for nodeNum := 0; nodeNum < numNodes; nodeNum++ {
				nodeIDs = append(nodeIDs, testrand.NodeID())
			}

			numForks := 1 + rand.Intn(maxForks)
			numSegments := 1 + rand.Intn(maxSegments)
			if numSegments < numForks {
				numSegments = numForks
			}

			piecesOffs := 0
			for segNum := 0; segNum < numSegments; segNum++ {
				numPieces := 1 + rand.Intn(int(maxPiecesPerSegment))
				for pn := uint16(0); pn < uint16(numPieces); pn++ {
					nodeID := nodeIDs[rand.Intn(numNodes)]
					pieces = append(pieces, metabase.Piece{
						Number:      pn,
						StorageNode: nodeID,
					})
					pieceCounts[nodeID]++
				}
				segments = append(segments, rangedloop.Segment{
					RootPieceID: storj.NewPieceID(),
					Pieces:      pieces[piecesOffs : piecesOffs+numPieces],
				})
				piecesOffs += numPieces
			}

			// Shuffle the segments to ensure that each fork consumes
			// a random set of segments.
			for j := range segments {
				k := rand.Intn(j + 1)
				segments[j], segments[k] = segments[k], segments[j]
			}

			observer := bloomfilter.NewObserver(zap.NewNop(), bloomfilter.Config{
				InitialPieces:      int64(maxSegments * int(maxPiecesPerSegment)),
				FalsePositiveRate:  falsePositiveRate,
				MaxBloomFilterSize: memory.Size(maxFilterSize),
				AccessGrant:        "---",
				Bucket:             "---",
			}, &pieceCounts)

			if err := observer.Start(ctx, time.Now()); err != nil {
				return errs.Wrap(err)
			}

			tempForkSegmentCounts := forkSegmentCounts[:numForks]
			RandomSequenceWithSum(numSegments, tempForkSegmentCounts)

			segmentsOffs := 0
			for forkNum := 0; forkNum < numForks; forkNum++ {
				numForkSegments := tempForkSegmentCounts[forkNum]
				forkSegments := segments[segmentsOffs : segmentsOffs+numForkSegments]
				segmentsOffs += numForkSegments

				partial, err := observer.Fork(ctx)
				if err != nil {
					return errs.Wrap(err)
				}

				if err = partial.Process(ctx, forkSegments); err != nil {
					return errs.Wrap(err)
				}

				if err = observer.Join(ctx, partial); err != nil {
					return errs.Wrap(err)
				}

				for _, segment := range forkSegments {
					for _, piece := range segment.Pieces {
						retainInfo, ok := observer.TestingRetainInfos().Load(piece.StorageNode)
						if !ok {
							var nodeIDs []string
							for nodeID := range observer.TestingRetainInfos().AsMap() {
								nodeIDs = append(nodeIDs, nodeID.String())
							}
							return errs.New("storage node ID %s not found in retain infos (found: %v)", piece.StorageNode.String(), nodeIDs)
						}
						pieceID := segment.RootPieceID.Derive(piece.StorageNode, int32(piece.Number))
						if !retainInfo.Filter.Contains(pieceID) {
							fmt.Printf("Piece not found in bloom filter (piece ID: %v)\n", pieceID)
						}
					}
				}
			}

			nodeIDs = nodeIDs[:0]
			segments = segments[:0]
			pieces = pieces[:0]
			for nodeID := range pieceCounts {
				delete(pieceCounts, nodeID)
			}
		}

		return nil
	},
}

func main() {
	cmd.Flags().IntVar(&numNodes, "nodes", 3, "number of nodes among which pieces will be distributed")
	cmd.Flags().IntVar(&maxForks, "max-forks", 100, "maximum number of observer forks")
	cmd.Flags().IntVar(&maxSegments, "max-segments", 1000, "maximum number of segments")
	cmd.Flags().Uint16Var(&maxPiecesPerSegment, "max-pieces-per-segment", 100, "maximum number of pieces per segment")

	//cmd.Flags().BoolVar(&useSyncObserver, "use-sync-observer", false, "whether to use the sync observer")
	cmd.Flags().Int64Var(&maxFilterSize, "max-filter-size", int64(2*memory.MB), "maximum size, in bytes, of a single bloom filter")
	cmd.Flags().Float64Var(&falsePositiveRate, "false-positive-rate", 0.1, "the false positive rate of bloom filters")
	cmd.Flags().Int64Var(&seed, "seed", 0, "the seed used to initialize the default RNG to a deterministic state (0 is random)")

	cmd.Flags().DurationVar(&duration, "duration", 24*time.Hour, "total runtime for the fuzzing process")

	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
	}
}

var _ bloomfilter.Overlay = (*mockOverlay)(nil)

type mockOverlay map[storj.NodeID]int64

func (c *mockOverlay) ActiveNodesPieceCounts(ctx context.Context) (map[storj.NodeID]int64, error) {
	return *c, nil
}

// RandomSequenceWithSum fills buf with positive integers summing to num.
func RandomSequenceWithSum(num int, buf []int) {
	count := len(buf)
	buf[0] = num - count
	for i := 1; i < count; i++ {
		buf[i] = rand.Intn(num - count + 1)
	}

	slices.Sort(buf)

	var previous int
	remaining := num
	for i := 0; i < count; i++ {
		tmp := buf[i]
		buf[i] = buf[i] - previous + 1
		remaining -= buf[i]
		previous = tmp
	}
}
