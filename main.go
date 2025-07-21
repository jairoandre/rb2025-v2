package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"rinha/client"
	"rinha/handler"
	"rinha/repository"
	"rinha/worker"
	"sync"
	"syscall"
	"time"

	"github.com/valyala/fasthttp"
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
	healthUrl := readEnv("HEALTH_URL", "http://localhost:9001")
	otherBackend := readEnv("OTHER_BACKEND", "")
	client := client.NewClient(defaultUrl, fallbackUrl, otherBackend, healthUrl)
	repository := repository.NewRepository()
	workerContext, workerCancel := context.WithCancel(context.Background())
	worker := worker.NewWorker(workerContext, &wg, repository, client, 500, 5000)
	handler := handler.NewHandler(worker, repository, client)

	worker.Start()

	server := &fasthttp.Server{
		Handler: handler.FasthttpHandler,
	}

	go func() {
		log.Printf("Listening on port %s", port)
		if err := server.ListenAndServe(fmt.Sprintf(":%s", port)); err != nil {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()
	<-ctx.Done()
	workerCancel()
	log.Println("Shutdown signal received")
	_, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
	}
	log.Println("Application closed")
	wg.Wait()

}
