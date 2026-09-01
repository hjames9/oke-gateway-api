package ociapi

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gemyago/oke-gateway-api/internal/diag"
)

type trackingDispatcher struct {
	started chan struct{}
	release chan struct{}

	mu            sync.Mutex
	active        int
	maxConcurrent int
}

func newTrackingDispatcher() *trackingDispatcher {
	return &trackingDispatcher{
		started: make(chan struct{}, 10),
		release: make(chan struct{}),
	}
}

func (d *trackingDispatcher) Do(_ *http.Request) (*http.Response, error) {
	d.mu.Lock()
	d.active++
	if d.active > d.maxConcurrent {
		d.maxConcurrent = d.active
	}
	d.mu.Unlock()

	d.started <- struct{}{}
	<-d.release

	d.mu.Lock()
	d.active--
	d.mu.Unlock()
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
}

func TestOCIRequestLimiter(t *testing.T) {
	t.Run("limits concurrent requests", func(t *testing.T) {
		limiter := newOCIRequestLimiter(OCIRequestLimiterDeps{
			RootLogger:            diag.RootTestLogger(),
			MaxConcurrentRequests: 1,
		})
		dispatcher := newTrackingDispatcher()
		baseClient := commonBaseClientWithDispatcher(dispatcher)
		configureOCIRequestLimiter(baseClient, "testService", limiter)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com", nil)
		require.NoError(t, err)

		errs := make(chan error, 2)
		go func() {
			_, callErr := baseClient.HTTPClient.Do(req)
			errs <- callErr
		}()
		<-dispatcher.started

		go func() {
			_, callErr := baseClient.HTTPClient.Do(req)
			errs <- callErr
		}()
		select {
		case <-dispatcher.started:
			t.Fatal("second request started before first request released")
		case <-time.After(10 * time.Millisecond):
		}

		dispatcher.release <- struct{}{}
		require.NoError(t, <-errs)
		<-dispatcher.started
		dispatcher.release <- struct{}{}
		require.NoError(t, <-errs)

		assert.Equal(t, 1, dispatcher.maxConcurrent)
	})

	t.Run("limits requests separately per service", func(t *testing.T) {
		limiter := newOCIRequestLimiter(OCIRequestLimiterDeps{
			RootLogger:            diag.RootTestLogger(),
			MaxConcurrentRequests: 1,
		})
		firstDispatcher := newTrackingDispatcher()
		firstClient := commonBaseClientWithDispatcher(firstDispatcher)
		configureOCIRequestLimiter(firstClient, "firstService", limiter)
		secondDispatcher := newTrackingDispatcher()
		secondClient := commonBaseClientWithDispatcher(secondDispatcher)
		configureOCIRequestLimiter(secondClient, "secondService", limiter)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com", nil)
		require.NoError(t, err)

		errs := make(chan error, 2)
		go func() {
			_, callErr := firstClient.HTTPClient.Do(req)
			errs <- callErr
		}()
		<-firstDispatcher.started

		go func() {
			_, callErr := secondClient.HTTPClient.Do(req)
			errs <- callErr
		}()
		<-secondDispatcher.started

		firstDispatcher.release <- struct{}{}
		secondDispatcher.release <- struct{}{}
		require.NoError(t, <-errs)
		require.NoError(t, <-errs)

		assert.Equal(t, 1, firstDispatcher.maxConcurrent)
		assert.Equal(t, 1, secondDispatcher.maxConcurrent)
	})

	t.Run("limits mutating requests more strictly than read requests", func(t *testing.T) {
		limiter := newOCIRequestLimiter(OCIRequestLimiterDeps{
			RootLogger:                    diag.RootTestLogger(),
			MaxConcurrentRequests:         3,
			MaxConcurrentMutatingRequests: 1,
		})
		dispatcher := newTrackingDispatcher()
		baseClient := commonBaseClientWithDispatcher(dispatcher)
		configureOCIRequestLimiter(baseClient, "testService", limiter)
		writeReq, err := http.NewRequestWithContext(t.Context(), http.MethodPut, "https://example.com", nil)
		require.NoError(t, err)
		readReq, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com", nil)
		require.NoError(t, err)

		errs := make(chan error, 3)
		go func() {
			_, callErr := baseClient.HTTPClient.Do(writeReq)
			errs <- callErr
		}()
		<-dispatcher.started

		go func() {
			_, callErr := baseClient.HTTPClient.Do(writeReq)
			errs <- callErr
		}()
		select {
		case <-dispatcher.started:
			t.Fatal("second mutating request started before first mutating request released")
		case <-time.After(10 * time.Millisecond):
		}

		go func() {
			_, callErr := baseClient.HTTPClient.Do(readReq)
			errs <- callErr
		}()
		<-dispatcher.started

		dispatcher.release <- struct{}{}
		require.NoError(t, <-errs)
		dispatcher.release <- struct{}{}
		require.NoError(t, <-errs)
		<-dispatcher.started
		dispatcher.release <- struct{}{}
		require.NoError(t, <-errs)

		assert.Equal(t, 2, dispatcher.maxConcurrent)
	})

	t.Run("returns original dispatcher when disabled", func(t *testing.T) {
		limiter := newOCIRequestLimiter(OCIRequestLimiterDeps{
			RootLogger:            diag.RootTestLogger(),
			MaxConcurrentRequests: -1,
		})
		dispatcher := newTrackingDispatcher()
		baseClient := commonBaseClientWithDispatcher(dispatcher)

		configureOCIRequestLimiter(baseClient, "testService", limiter)

		assert.Same(t, dispatcher, baseClient.HTTPClient)
	})

	t.Run("returns context error while waiting for slot", func(t *testing.T) {
		limiter := newOCIRequestLimiter(OCIRequestLimiterDeps{
			RootLogger:            diag.RootTestLogger(),
			MaxConcurrentRequests: 1,
		})
		dispatcher := newTrackingDispatcher()
		baseClient := commonBaseClientWithDispatcher(dispatcher)
		configureOCIRequestLimiter(baseClient, "testService", limiter)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com", nil)
		require.NoError(t, err)

		errs := make(chan error, 1)
		go func() {
			_, callErr := baseClient.HTTPClient.Do(req)
			errs <- callErr
		}()
		<-dispatcher.started

		cancelledCtx, cancel := context.WithCancel(t.Context())
		cancel()
		cancelledReq, err := http.NewRequestWithContext(cancelledCtx, http.MethodGet, "https://example.com", nil)
		require.NoError(t, err)

		_, callErr := baseClient.HTTPClient.Do(cancelledReq)

		require.ErrorIs(t, callErr, context.Canceled)
		require.ErrorContains(t, callErr, "failed waiting for OCI testService request slot")
		dispatcher.release <- struct{}{}
		require.NoError(t, <-errs)
	})

	t.Run("returns context error while waiting for mutating slot", func(t *testing.T) {
		limiter := newOCIRequestLimiter(OCIRequestLimiterDeps{
			RootLogger:                    diag.RootTestLogger(),
			MaxConcurrentRequests:         2,
			MaxConcurrentMutatingRequests: 1,
		})
		dispatcher := newTrackingDispatcher()
		baseClient := commonBaseClientWithDispatcher(dispatcher)
		configureOCIRequestLimiter(baseClient, "testService", limiter)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPatch, "https://example.com", nil)
		require.NoError(t, err)

		errs := make(chan error, 1)
		go func() {
			_, callErr := baseClient.HTTPClient.Do(req)
			errs <- callErr
		}()
		<-dispatcher.started

		cancelledCtx, cancel := context.WithCancel(t.Context())
		defer cancel()
		cancelledReq, err := http.NewRequestWithContext(cancelledCtx, http.MethodPatch, "https://example.com", nil)
		require.NoError(t, err)

		callErrs := make(chan error, 1)
		go func() {
			_, callErr := baseClient.HTTPClient.Do(cancelledReq)
			callErrs <- callErr
		}()
		select {
		case <-dispatcher.started:
			t.Fatal("second mutating request started before first mutating request released")
		case <-time.After(10 * time.Millisecond):
		}
		cancel()

		callErr := <-callErrs
		require.ErrorIs(t, callErr, context.Canceled)
		require.ErrorContains(t, callErr, "failed waiting for OCI testService mutating request slot")
		dispatcher.release <- struct{}{}
		require.NoError(t, <-errs)
	})

	t.Run("propagates delegate errors", func(t *testing.T) {
		wantErr := errors.New("delegate failed")
		dispatcher := roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, wantErr
		})
		limiter := newOCIRequestLimiter(OCIRequestLimiterDeps{
			RootLogger:            diag.RootTestLogger(),
			MaxConcurrentRequests: 1,
		})
		baseClient := commonBaseClientWithDispatcher(dispatcher)
		configureOCIRequestLimiter(baseClient, "testService", limiter)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com", nil)
		require.NoError(t, err)

		_, callErr := baseClient.HTTPClient.Do(req)

		require.ErrorIs(t, callErr, wantErr)
	})

	t.Run("classifies OCI mutating request methods", func(t *testing.T) {
		assert.False(t, isMutatingOCIRequest(http.MethodGet))
		assert.False(t, isMutatingOCIRequest(http.MethodHead))
		assert.False(t, isMutatingOCIRequest(http.MethodOptions))
		assert.True(t, isMutatingOCIRequest(http.MethodPost))
		assert.True(t, isMutatingOCIRequest(http.MethodPut))
		assert.True(t, isMutatingOCIRequest(http.MethodPatch))
		assert.True(t, isMutatingOCIRequest(http.MethodDelete))
	})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) Do(req *http.Request) (*http.Response, error) {
	return f(req)
}

func commonBaseClientWithDispatcher(dispatcher common.HTTPRequestDispatcher) *common.BaseClient {
	return &common.BaseClient{HTTPClient: dispatcher}
}
