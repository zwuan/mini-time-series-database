package main

import (
	"log"
	"net/http"

	"minitsdb/internal/api"
	"minitsdb/internal/storage"
)

func main() {
	store, err := storage.NewMemoryStorage("data/wal.log")
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	handler := api.NewHandler(store)

	mux := http.NewServeMux()
	handler.Register(mux)

	addr := ":8080"
	log.Printf("MiniTSDB listening on %s", addr)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
