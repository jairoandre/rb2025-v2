package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"rinha/client"
	"rinha/handler"
	"rinha/repository"
	"rinha/worker"
	"sync"
	"syscall"
	"time"
)

func readEnv(envName string, defaultValue string) string {
	envValue, exists := os.LookupEnv(envName)
	if exists {
		return envValue
	}
	return defaultValue
}

func main() {
	var wg sync.WaitGroup
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	port := readEnv("SERVER_PORT", "9999")
	defaultUrl := readEnv("DEFAULT_URL", "http://localhost:8001")
	fallbackUrl := readEnv("FALLBACK_URL", "http://localhost:8002")
	otherBackend := readEnv("OTHER_BACKEND", "")
	client := client.NewClient(defaultUrl, fallbackUrl, otherBackend)
	repository := repository.NewRepository()
	workerContext, workerCancel := context.WithCancel(context.Background())
	worker := worker.NewWorker(workerContext, &wg, repository, client, 1000, 5000)
	handler := handler.NewHandler(worker, repository, client)
	mux := http.NewServeMux()
	mux.HandleFunc("/payments", handler.HandlePayments)
	mux.HandleFunc("/payments-summary", handler.HandleSummaryNormal)
	mux.HandleFunc("/payments-summary-single", handler.HandleSummarySingle)
	mux.HandleFunc("/purge-payments", handler.HandlePurge)
	server := &http.Server{
		Addr:    fmt.Sprintf(":%s", port),
		Handler: mux,
	}

	worker.Start()

	go func() {
		log.Printf("Listening on port %s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()
	<-ctx.Done()
	workerCancel()
	log.Println("Shutdown signal received")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
	}
	log.Println("Application closed")
	wg.Wait()

}
