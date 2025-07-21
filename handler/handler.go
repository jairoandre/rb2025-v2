package handler

import (
	"rinha/client"
	"rinha/model"
	"rinha/repository"
	"rinha/worker"
	"time"

	"github.com/mailru/easyjson"
	"github.com/valyala/fasthttp"
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

func (h *Handler) FasthttpHandler(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Path()) {
	case "/payments":
		h.HandlePayments(ctx)
	case "/payments-summary":
		h.HandleSummaryNormal(ctx)
	case "/payments-summary-single":
		h.HandleSummarySingle(ctx)
	case "/purge-payments":
		h.HandlePurge(ctx)
	default:
		ctx.Error("Unsupported path", fasthttp.StatusNotFound)
	}
}

func (h *Handler) HandlePayments(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() {
		ctx.Error("Method Not Allowed", fasthttp.StatusMethodNotAllowed)
		return
	}

	var req model.PaymentRequest
	err := easyjson.Unmarshal(ctx.PostBody(), &req)
	if err != nil {
		ctx.Error("Bad Request", fasthttp.StatusBadRequest)
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

	ctx.SetStatusCode(fasthttp.StatusCreated)
}

func (h *Handler) HandleSummaryNormal(ctx *fasthttp.RequestCtx) {
	h.HandleSummary(ctx, true)
}

func (h *Handler) HandleSummarySingle(ctx *fasthttp.RequestCtx) {
	h.HandleSummary(ctx, false)
}

func (h *Handler) HandleSummary(ctx *fasthttp.RequestCtx, checkOther bool) {
	if !ctx.IsGet() {
		ctx.Error("Method Not Allowed", fasthttp.StatusMethodNotAllowed)
		return
	}
	fromStr := string(ctx.QueryArgs().Peek("from"))
	toStr := string(ctx.QueryArgs().Peek("to"))

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

	ctx.Response.Header.Set("Content-Type", "application/json")
	if _, err := easyjson.MarshalToWriter(&localSummary, ctx); err != nil {
		ctx.Error("Internal Server Error", fasthttp.StatusInternalServerError)
	}
}

func (h *Handler) HandlePurge(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() {
		ctx.Error("Method Not Allowed", fasthttp.StatusMethodNotAllowed)
		return
	}
	h.Repository.PurgePayments()
	ctx.SetStatusCode(fasthttp.StatusAccepted)
}
