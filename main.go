package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const maxValueSize = 1 << 20 // 1 MB

func main() {
	dataDir := "data"
	if len(os.Args) > 1 {
		dataDir = os.Args[1]
	}

	store, err := NewStore(dataDir)
	if err != nil {
		log.Fatalf("failed to open store: %v", err)
	}
	defer store.Close()

	mux := http.NewServeMux()

	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		value, ok, err := store.Get(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !ok {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}
		w.Write([]byte(value))
	})

	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		r.Body = http.MaxBytesReader(w, r.Body, maxValueSize)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		if len(body) == 0 {
			http.Error(w, "value cannot be empty", http.StatusBadRequest)
			return
		}
		if err := store.Put(key, string(body)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("DELETE /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if err := store.Delete(key); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("POST /compact", func(w http.ResponseWriter, r *http.Request) {
		if err := store.Compact(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	addr := ":8080"
	log.Printf("kv-inmem listening on %s", addr)

	// Background compaction every 15 seconds.
	compactDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := store.Compact(); err != nil {
					log.Printf("compaction error: %v", err)
				} else {
					log.Println("compaction complete")
				}
			case <-compactDone:
				return
			}
		}
	}()

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		fmt.Println("\nshutting down...")
		close(compactDone)
		store.Close()
		os.Exit(0)
	}()

	log.Fatal(http.ListenAndServe(addr, mux))
}
