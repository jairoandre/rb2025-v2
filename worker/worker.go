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
	Jobs          chan model.PaymentEvent
	PriorityJobs  chan model.PaymentEvent
	Wg            *sync.WaitGroup
	NumWorkers    int
	Ctx           context.Context
	Repository    *repository.Repository
	Client        *client.Client
	suspendCh     chan struct{}
	suspended     bool
	suspendedMux  sync.Mutex
	serviceHealth model.ServiceHealthResponse
}

func NewWorker(
	ctx context.Context,
	wg *sync.WaitGroup,
	repository *repository.Repository,
	processor *client.Client,
	numWorkers, bufferSize int) *Worker {
	jobs := make(chan model.PaymentEvent, bufferSize)
	priorityJobs := make(chan model.PaymentEvent, bufferSize)
	suspendCh := make(chan struct{})
	return &Worker{
		Ctx:          ctx,
		Wg:           wg,
		NumWorkers:   numWorkers,
		Repository:   repository,
		Client:       processor,
		Jobs:         jobs,
		PriorityJobs: priorityJobs,
		suspendCh:    suspendCh}
}

func (w *Worker) SuspendJobs() {
	log.Println("Suspend job")
	w.suspendedMux.Lock()
	if w.suspended {
		log.Println("Skip supended")
		w.suspendedMux.Unlock()
		return
	} else {
		w.suspended = true
	}
	w.suspendedMux.Unlock()
	go func() {
		for {
			if w.serviceHealth.DefaultHealth || w.serviceHealth.FallbackHealth {
				w.ResumeJobs()
				return
			}
			time.Sleep(time.Duration(w.serviceHealth.NextCheck) * time.Millisecond)
		}
	}()
}

func (w *Worker) ResumeJobs() {
	log.Println("Resume job")
	w.suspendedMux.Lock()
	w.suspended = false
	w.suspendedMux.Unlock()
	close(w.suspendCh)
	w.suspendCh = make(chan struct{})
}

func (w *Worker) HandleEvent(event model.PaymentEvent) {
	processor, err := w.Client.SendPayment(event, w.serviceHealth)
	if err != nil {
		if !w.suspended {
			w.SuspendJobs()
		}
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
		w.suspendedMux.Lock()
		suspended := w.suspended
		ch := w.suspendCh
		w.suspendedMux.Unlock()
		if suspended {
			<-ch
			continue
		}
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
	ctx, cancelJobs := context.WithCancel(w.Ctx)
	for range w.NumWorkers {
		w.Wg.Add(1)
		go w.HandleJob(ctx)
	}
	go func() {
		<-w.Ctx.Done()
		cancelJobs()
	}()
	go func() {
		log.Println("Starting service health monitor")
		for {
			select {
			case <-w.Ctx.Done():
				log.Println("Stop service health monitor")
				return
			default:
				serviceHealth, err := w.Client.ServiceHealth()
				if err != nil {
					log.Println("Error retrieving service health")
				} else {
					w.serviceHealth = serviceHealth
					time.Sleep(time.Duration(serviceHealth.NextCheck) * time.Millisecond)
				}
			}
		}
	}()
}
