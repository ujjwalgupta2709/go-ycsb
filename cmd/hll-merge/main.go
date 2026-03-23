// hll-merge merges HyperLogLog sketch files exported by the Cassandra YCSB driver
// and prints the combined unique key estimate.
//
// Usage:
//
//	go run ./cmd/hll-merge node1.bin node2.bin node3.bin node4.bin
package main

import (
	"fmt"
	"os"

	"github.com/axiomhq/hyperloglog"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: hll-merge <sketch1.bin> <sketch2.bin> ...\n")
		os.Exit(1)
	}

	merged := hyperloglog.New16()

	for _, path := range os.Args[1:] {
		data, err := os.ReadFile(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR reading %s: %v\n", path, err)
			os.Exit(1)
		}
		sk := hyperloglog.New16()
		if err := sk.UnmarshalBinary(data); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR unmarshaling %s: %v\n", path, err)
			os.Exit(1)
		}
		fmt.Printf("  %s: estimated_unique_keys=%d\n", path, sk.Estimate())
		merged.Merge(sk)
	}

	fmt.Printf("\n=== CLUSTER TOTAL ===\n")
	fmt.Printf("estimated_unique_keys=%d  (across %d nodes)\n",
		merged.Estimate(), len(os.Args)-1)
}
