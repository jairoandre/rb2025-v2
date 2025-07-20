package client

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"net/http"
	"rinha/model"
	"time"

	"github.com/mailru/easyjson"
)

type Client struct {
	DefaultUrl   string
	FallbackUrl  string
	OtherBackend string
	Client       *http.Client
}

func NewClient(defaultUrl, fallbackUrl, otherBackend string) *Client {
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        500,
			MaxIdleConnsPerHost: 500,
			IdleConnTimeout:     90 * time.Second,
			DisableKeepAlives:   false,
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
		Timeout: 10 * time.Second,
	}
	return &Client{DefaultUrl: defaultUrl, FallbackUrl: fallbackUrl, Client: client, OtherBackend: otherBackend}
}

// SendPayment tries default first, then fallback
func (c *Client) SendPayment(event model.PaymentEvent) (int, error) {
	if c.PostJSON(c.DefaultUrl, event) {
		return 0, nil
	}
	if c.PostJSON(c.FallbackUrl, event) {
		return 1, nil
	}
	return -1, ErrBothFailed
}

var ErrBothFailed = &ProcessorError{"Both endpoints failed"}

type ProcessorError struct {
	Message string
}

func (e *ProcessorError) Error() string {
	return e.Message
}

// Internal POST logic
func (c *Client) PostJSON(url string, event model.PaymentEvent) bool {
	body, err := easyjson.Marshal(event)
	if err != nil {
		log.Printf("JSON marshal error: %v", err)
		return false
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/payments", url), bytes.NewBuffer(body))
	if err != nil {
		log.Printf("Request creation error: %v", err)
		return false
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

func (c *Client) DefaultHealth() (bool, int) {
	return c.ServiceHealth(c.DefaultUrl)
}

func (c *Client) FallbackHealth() (bool, int) {
	return c.ServiceHealth(c.FallbackUrl)
}

func (c *Client) ServiceHealth(url string) (bool, int) {
	resp, err := c.Client.Get(fmt.Sprintf("%s/payments/service-health/", url))
	if err != nil {
		log.Printf("Service health error: %v", err)
		return true, 0
	}
	defer resp.Body.Close()

	var data model.ProcessorHealthResponse
	err = easyjson.UnmarshalFromReader(resp.Body, &data)
	if err != nil {
		return true, 0
	}
	log.Printf("Checking service health %s: %v", url, data)
	return data.Failing, data.MinResponseTime
}

func (c *Client) GetSummarySingle(from, to string) *model.SummaryResponse {
	resp, err := c.Client.Get(fmt.Sprintf("%s/payments-summary-single?from=%s&to=%s", c.OtherBackend, from, to))
	if err != nil {
		log.Printf("Service health error: %v", err)
		return nil
	}
	defer resp.Body.Close()

	var data model.SummaryResponse
	err = easyjson.UnmarshalFromReader(resp.Body, &data)
	if err != nil {
		return nil
	}
	return &data
}
