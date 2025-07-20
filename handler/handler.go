package handler

import (
	"net/http"
	"rinha/client"
	"rinha/model"
	"rinha/repository"
	"rinha/worker"
	"time"

	"github.com/mailru/easyjson"
)

type Handler struct {
	Worker     *worker.Worker
	Repository *repository.Repository
	Client     *client.Client
}

func NewHandler(worker *worker.Worker, repository *repository.Repository, client *client.Client) *Handler {
	return &Handler{
		Worker:     worker,
		Repository: repository,
		Client:     client,
	}
}

func (h *Handler) HandlePayments(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var req model.PaymentRequest
	err := easyjson.UnmarshalFromReader(r.Body, &req)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	event := model.PaymentEvent{
		CorrelationID: req.CorrelationID,
		Amount:        req.Amount,
		RequestedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}

	go func() {
		h.Worker.Jobs <- event
	}()

	w.WriteHeader(http.StatusCreated)
}

func (h *Handler) HandleSummaryNormal(w http.ResponseWriter, r *http.Request) {
	h.HandleSummary(w, r, true)
}

func (h *Handler) HandleSummarySingle(w http.ResponseWriter, r *http.Request) {
	h.HandleSummary(w, r, false)
}

func (h *Handler) HandleSummary(w http.ResponseWriter, r *http.Request, checkOther bool) {
	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")

	from, err1 := time.Parse(time.RFC3339Nano, fromStr)
	if err1 != nil {
		from = time.Now().UTC().Add(-24 * time.Hour)
	}
	to, err2 := time.Parse(time.RFC3339Nano, toStr)
	if err2 != nil {
		to = time.Now().UTC()
	}

	localSummary := h.Repository.GetSummary(from, to)

	if checkOther && h.Client.OtherBackend != "" {
		otherSummary := h.Client.GetSummarySingle(fromStr, toStr)
		localSummary.Default.TotalRequests += otherSummary.Default.TotalRequests
		localSummary.Default.TotalAmount += otherSummary.Default.TotalAmount
		localSummary.Fallback.TotalRequests += otherSummary.Fallback.TotalRequests
		localSummary.Fallback.TotalAmount += otherSummary.Fallback.TotalAmount
	}

	w.Header().Set("Content-Type", "application/json")
	_, _, err := easyjson.MarshalToHTTPResponseWriter(&localSummary, w)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

func (h *Handler) HandlePurge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	h.Repository.PurgePayments()
	w.WriteHeader(http.StatusAccepted)
}
