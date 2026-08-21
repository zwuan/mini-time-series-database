package main

import (
	"flag"
	"log"
	"net/http"

	"minitsdb/internal/api"
	"minitsdb/internal/storage"
)

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	walPath := flag.String("wal", "data/wal.log", "path to the write-ahead log")
	flushThreshold := flag.Int("flush", storage.DefaultFlushThreshold,
		"number of samples buffered in memory before flushing to a block")
	flag.Parse()

	store, err := storage.NewMemoryStorage(*walPath, *flushThreshold)

	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	handler := api.NewHandler(store)

	mux := http.NewServeMux()
	handler.Register(mux)

	log.Printf("MiniTSDB listening on %s (flush threshold %d)", *addr, *flushThreshold)

	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatal(err)
	}
}
