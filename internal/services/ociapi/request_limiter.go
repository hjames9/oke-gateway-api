package ociapi

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"go.uber.org/dig"
)

const defaultMaxConcurrentRequests = 4
const defaultMaxConcurrentMutatingRequests = 1
const requestLimiterWaitLogThreshold = 10 * time.Millisecond

type ociRequestLimiter struct {
	logger                      *slog.Logger
	maxConcurrentRequests       int
	maxConcurrentMutating       int
	serviceRequestLimiters      map[string]*ociServiceRequestLimiters
	serviceRequestLimitersMutex sync.Mutex
}

type ociServiceRequestLimiters struct {
	requests chan struct{}
	mutating chan struct{}
}

type OCIRequestLimiterDeps struct {
	dig.In

	RootLogger                    *slog.Logger
	MaxConcurrentRequests         int `name:"config.ociapi.max-concurrent-requests"`
	MaxConcurrentMutatingRequests int `name:"config.ociapi.max-concurrent-mutating-requests"`
}

func newOCIRequestLimiter(deps OCIRequestLimiterDeps) *ociRequestLimiter {
	maxConcurrentRequests := deps.MaxConcurrentRequests
	if maxConcurrentRequests == 0 {
		maxConcurrentRequests = defaultMaxConcurrentRequests
	}
	if maxConcurrentRequests < 0 {
		return nil
	}
	maxConcurrentMutatingRequests := deps.MaxConcurrentMutatingRequests
	if maxConcurrentMutatingRequests == 0 {
		maxConcurrentMutatingRequests = defaultMaxConcurrentMutatingRequests
	}
	logger := slog.Default()
	if deps.RootLogger != nil {
		logger = deps.RootLogger
	}
	return &ociRequestLimiter{
		logger:                 logger.WithGroup("oci-request-limiter"),
		maxConcurrentRequests:  maxConcurrentRequests,
		maxConcurrentMutating:  maxConcurrentMutatingRequests,
		serviceRequestLimiters: map[string]*ociServiceRequestLimiters{},
	}
}

func (l *ociRequestLimiter) forService(serviceName string) *ociServiceRequestLimiters {
	l.serviceRequestLimitersMutex.Lock()
	defer l.serviceRequestLimitersMutex.Unlock()

	limiters, ok := l.serviceRequestLimiters[serviceName]
	if ok {
		return limiters
	}

	limiters = &ociServiceRequestLimiters{
		requests: make(chan struct{}, l.maxConcurrentRequests),
	}
	if l.maxConcurrentMutating >= 0 {
		limiters.mutating = make(chan struct{}, l.maxConcurrentMutating)
	}
	l.serviceRequestLimiters[serviceName] = limiters
	return limiters
}

func configureOCIRequestLimiter(
	client *common.BaseClient,
	serviceName string,
	limiter *ociRequestLimiter,
) {
	if limiter == nil || client.HTTPClient == nil {
		return
	}
	client.HTTPClient = &rateLimitedDispatcher{
		serviceName: serviceName,
		delegate:    client.HTTPClient,
		limiter:     limiter,
	}
}

type rateLimitedDispatcher struct {
	serviceName string
	delegate    common.HTTPRequestDispatcher
	limiter     *ociRequestLimiter
}

func (d *rateLimitedDispatcher) Do(req *http.Request) (*http.Response, error) {
	ctx := context.Background()
	if req != nil {
		ctx = req.Context()
	}
	limiters := d.limiter.forService(d.serviceName)

	startedAt := time.Now()
	select {
	case limiters.requests <- struct{}{}:
	case <-ctx.Done():
		return nil, fmt.Errorf("failed waiting for OCI %s request slot: %w", d.serviceName, ctx.Err())
	}
	defer func() {
		<-limiters.requests
	}()

	if req != nil && isMutatingOCIRequest(req.Method) && limiters.mutating != nil {
		select {
		case limiters.mutating <- struct{}{}:
		case <-ctx.Done():
			return nil, fmt.Errorf("failed waiting for OCI %s mutating request slot: %w", d.serviceName, ctx.Err())
		}
		defer func() {
			<-limiters.mutating
		}()
	}

	if waited := time.Since(startedAt); waited >= requestLimiterWaitLogThreshold {
		operationKind := "mutating"
		if req == nil || !isMutatingOCIRequest(req.Method) {
			operationKind = "read"
		}
		requestPath := ""
		requestMethod := ""
		if req != nil {
			requestMethod = req.Method
			requestPath = req.URL.Path
		}
		d.limiter.logger.DebugContext(
			ctx,
			"Acquired OCI request slot",
			slog.String("serviceName", d.serviceName),
			slog.String("operationKind", operationKind),
			slog.String("httpMethod", requestMethod),
			slog.String("requestPath", requestPath),
			slog.Duration("waitDuration", waited),
		)
	}
	return d.delegate.Do(req)
}

func isMutatingOCIRequest(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	default:
		return true
	}
}
