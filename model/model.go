package model

import "time"

type PaymentRequest struct {
	CorrelationID string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
}

type PaymentEvent struct {
	CorrelationID string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
	RequestedAt   string  `json:"requestedAt"`
}

type Summary struct {
	TotalRequests int     `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
}

type SummaryResponse struct {
	Default  Summary `json:"default"`
	Fallback Summary `json:"fallback"`
}

type ProcessorHealthResponse struct {
	Failing         bool `json:"failing"`
	MinResponseTime int  `json:"minResponseTime"`
}

type Payment struct {
	CorrelationID string    `json:"correlationId"`
	Amount        float64   `json:"amount"`
	RequestedAt   time.Time `json:"requestedAt"`
	Processor     int       `json:"processor"`
}

type ServiceHealthResponse struct {
	DefaultHealth       bool `json:"defaultHeath"`
	FallbackHealth      bool `json:"fallbackHealth"`
	DefaultMinResponse  int  `json:"defaultMinResponse"`
	FallbackMinResponse int  `json:"fallbackMinResponse"`
	NextCheck           int  `json:"nextCheck"`
}
