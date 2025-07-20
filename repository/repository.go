package repository

import (
	"math"
	"rinha/model"
	"sync"
	"time"
)

type Repository struct {
	Payments *sync.Map
}

func NewRepository() *Repository {
	payments := new(sync.Map)
	return &Repository{Payments: payments}
}

func (r *Repository) Add(payment model.Payment) {
	r.Payments.Store(payment.CorrelationID, payment)
}

func (r *Repository) GetSummary(from, to time.Time) model.SummaryResponse {
	var defaultSummary, fallbackSummary model.Summary
	r.Payments.Range(func(key, value any) bool {
		payment := value.(model.Payment)
		if payment.RequestedAt.Before(from) || payment.RequestedAt.After(to) {
			return true
		}
		switch payment.Processor {
		case 0:
			defaultSummary.TotalAmount += payment.Amount
			defaultSummary.TotalRequests += 1
		case 1:
			fallbackSummary.TotalAmount += payment.Amount
			fallbackSummary.TotalRequests += 1
		}
		defaultSummary.TotalAmount = math.Round(defaultSummary.TotalAmount*10) / 10
		fallbackSummary.TotalAmount = math.Round(fallbackSummary.TotalAmount*10) / 10
		return true
	})
	return model.SummaryResponse{Default: defaultSummary, Fallback: fallbackSummary}
}

func (r *Repository) PurgePayments() {
	r.Payments.Clear()
}
