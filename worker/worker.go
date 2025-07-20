package worker

import (
	"context"
	"log"
	"rinha/client"
	"rinha/model"
	"rinha/repository"
	"sync"
	"time"
)

type Worker struct {
	Jobs         chan model.PaymentEvent
	PriorityJobs chan model.PaymentEvent
	Wg           *sync.WaitGroup
	NumWorkers   int
	Ctx          context.Context
	Repository   *repository.Repository
	Client       *client.Client
	Suspended    bool
	SuspendedMux sync.Mutex
}

func NewWorker(
	ctx context.Context,
	wg *sync.WaitGroup,
	repository *repository.Repository,
	processor *client.Client,
	numWorkers, bufferSize int) *Worker {
	jobs := make(chan model.PaymentEvent, bufferSize)
	priorityJobs := make(chan model.PaymentEvent, bufferSize)
	return &Worker{
		Ctx:          ctx,
		Wg:           wg,
		NumWorkers:   numWorkers,
		Repository:   repository,
		Client:       processor,
		Jobs:         jobs,
		PriorityJobs: priorityJobs}
}

func (w *Worker) SuspendJobs() {
	w.SuspendedMux.Lock()

}

func (w *Worker) HandleEvent(event model.PaymentEvent) {
	processor, err := w.Client.SendPayment(event)
	if err != nil {
		w.PriorityJobs <- event
		return
	}
	requestedAt, _ := time.Parse(time.RFC3339Nano, event.RequestedAt)
	w.Repository.Add(model.Payment{
		CorrelationID: event.CorrelationID,
		Amount:        event.Amount,
		RequestedAt:   requestedAt,
		Processor:     processor})
}

func (w *Worker) HandleJob(ctx context.Context) {
	defer w.Wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case evt := <-w.PriorityJobs:
			w.HandleEvent(evt)
		case evt := <-w.Jobs:
			w.HandleEvent(evt)
		}
	}
}

func (w *Worker) Start() {
	log.Println("Preparing workers")
	ctxWithCancel, cancelJobs := context.WithCancel(w.Ctx)
	for range w.NumWorkers {
		w.Wg.Add(1)
		go w.HandleJob(ctxWithCancel)
	}
	go func() {
		<-w.Ctx.Done()
		cancelJobs()
	}()
}
