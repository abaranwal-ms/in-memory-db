package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	addr := flag.String("addr", "http://localhost:8080", "server address")
	totalBytes := flag.Int64("bytes", 3*1024*1024*1024, "total bytes of values to write (default 3 GB)")
	workers := flag.Int("workers", 16, "concurrent workers")
	keySpace := flag.Int("keys", 500_000, "number of unique keys (overwrites create stale data)")
	valSize := flag.Int("vsize", 200, "value size in bytes")
	deleteRatio := flag.Float64("delete", 0.05, "fraction of ops that are deletes")
	flag.Parse()

	log.Printf("seed: target=%d bytes, workers=%d, keys=%d, valSize=%d, deleteRatio=%.2f",
		*totalBytes, *workers, *keySpace, *valSize, *deleteRatio)

	var written atomic.Int64
	var ops atomic.Int64
	start := time.Now()

	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Timeout: 10 * time.Second}
			val := strings.Repeat("x", *valSize)

			for written.Load() < *totalBytes {
				n, _ := rand.Int(rand.Reader, big.NewInt(int64(*keySpace)))
				key := fmt.Sprintf("key-%d", n.Int64())
				url := fmt.Sprintf("%s/kv/%s", *addr, key)

				r, _ := rand.Int(rand.Reader, big.NewInt(100))
				if float64(r.Int64())/100.0 < *deleteRatio {
					req, _ := http.NewRequest(http.MethodDelete, url, nil)
					resp, err := client.Do(req)
					if err != nil {
						log.Printf("DELETE %s: %v", key, err)
						continue
					}
					resp.Body.Close()
				} else {
					req, _ := http.NewRequest(http.MethodPut, url, strings.NewReader(val))
					resp, err := client.Do(req)
					if err != nil {
						log.Printf("PUT %s: %v", key, err)
						continue
					}
					resp.Body.Close()
					written.Add(int64(*valSize))
				}
				ops.Add(1)
			}
		}()
	}

	// Progress reporter.
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			w := written.Load()
			o := ops.Load()
			elapsed := time.Since(start).Seconds()
			log.Printf("progress: %.2f GB written, %d ops, %.0f ops/sec",
				float64(w)/(1024*1024*1024), o, float64(o)/elapsed)
			if w >= *totalBytes {
				return
			}
		}
	}()

	wg.Wait()

	elapsed := time.Since(start)
	totalW := written.Load()
	totalO := ops.Load()
	log.Printf("done: %.2f GB written, %d ops in %s (%.0f ops/sec)",
		float64(totalW)/(1024*1024*1024), totalO, elapsed.Round(time.Millisecond), float64(totalO)/elapsed.Seconds())
}
