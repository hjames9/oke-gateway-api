package ociapi

import (
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gemyago/oke-gateway-api/internal/diag"
)

type testConfigurationProvider struct {
	key *rsa.PrivateKey
}

func (p testConfigurationProvider) PrivateRSAKey() (*rsa.PrivateKey, error) {
	return p.key, nil
}

func (testConfigurationProvider) KeyID() (string, error) {
	return "ocid1.tenancy.oc1..example/ocid1.user.oc1..example/example-fingerprint", nil
}

func (testConfigurationProvider) TenancyOCID() (string, error) {
	return "ocid1.tenancy.oc1..example", nil
}

func (testConfigurationProvider) UserOCID() (string, error) {
	return "ocid1.user.oc1..example", nil
}

func (testConfigurationProvider) KeyFingerprint() (string, error) {
	return "example-fingerprint", nil
}

func (testConfigurationProvider) Region() (string, error) {
	return "us-ashburn-1", nil
}

func (testConfigurationProvider) AuthType() (common.AuthConfig, error) {
	return common.AuthConfig{AuthType: common.UserPrincipal}, nil
}

func TestNoopClients(t *testing.T) {
	deps := LoadBalancerConfigDeps{
		RootLogger: diag.RootTestLogger(),
		Noop:       true,
	}

	_, err := newLoadBalancerClient(deps)
	require.NoError(t, err)

	_, err = newNetworkLoadBalancerClient(deps)
	require.NoError(t, err)

	_, err = newCertificatesManagementClient(deps)
	require.NoError(t, err)
}

func TestClients(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	require.NoError(t, err)
	deps := LoadBalancerConfigDeps{
		RootLogger:       diag.RootTestLogger(),
		ConfigProvider:   testConfigurationProvider{key: key},
		RetryEnabled:     true,
		RetryMaxAttempts: 5,
		RetryMaxSleep:    7 * time.Second,
	}

	t.Run("creates load balancer client with retry policy", func(t *testing.T) {
		client, clientErr := newLoadBalancerClient(deps)

		require.NoError(t, clientErr)
		require.NotNil(t, client.RetryPolicy())
		assert.Equal(t, uint(5), client.RetryPolicy().MaximumNumberAttempts)
		assert.InDelta(t, deps.RetryMaxSleep.Seconds(), client.RetryPolicy().MaxSleepBetween, 0)
	})

	t.Run("creates network load balancer client with retry policy", func(t *testing.T) {
		client, clientErr := newNetworkLoadBalancerClient(deps)

		require.NoError(t, clientErr)
		require.NotNil(t, client.RetryPolicy())
		assert.Equal(t, uint(5), client.RetryPolicy().MaximumNumberAttempts)
		assert.InDelta(t, deps.RetryMaxSleep.Seconds(), client.RetryPolicy().MaxSleepBetween, 0)
	})

	t.Run("creates certificates management client with retry policy", func(t *testing.T) {
		client, clientErr := newCertificatesManagementClient(deps)

		require.NoError(t, clientErr)
		require.NotNil(t, client.RetryPolicy())
		assert.Equal(t, uint(5), client.RetryPolicy().MaximumNumberAttempts)
		assert.InDelta(t, deps.RetryMaxSleep.Seconds(), client.RetryPolicy().MaxSleepBetween, 0)
	})

	t.Run("creates clients without custom retry policy when disabled", func(t *testing.T) {
		disabledDeps := deps
		disabledDeps.RetryEnabled = false

		lbClient, clientErr := newLoadBalancerClient(disabledDeps)
		require.NoError(t, clientErr)
		assert.Nil(t, lbClient.RetryPolicy())

		nlbClient, clientErr := newNetworkLoadBalancerClient(disabledDeps)
		require.NoError(t, clientErr)
		assert.Nil(t, nlbClient.RetryPolicy())

		certClient, clientErr := newCertificatesManagementClient(disabledDeps)
		require.NoError(t, clientErr)
		assert.Nil(t, certClient.RetryPolicy())
	})

	t.Run("wraps clients with request limiter", func(t *testing.T) {
		limiterDeps := deps
		limiterDeps.RequestLimiter = newOCIRequestLimiter(OCIRequestLimiterDeps{
			RootLogger:                    diag.RootTestLogger(),
			MaxConcurrentRequests:         2,
			MaxConcurrentMutatingRequests: 1,
		})

		lbClient, clientErr := newLoadBalancerClient(limiterDeps)
		require.NoError(t, clientErr)
		assert.IsType(t, &rateLimitedDispatcher{}, lbClient.HTTPClient)

		nlbClient, clientErr := newNetworkLoadBalancerClient(limiterDeps)
		require.NoError(t, clientErr)
		assert.IsType(t, &rateLimitedDispatcher{}, nlbClient.HTTPClient)

		certClient, clientErr := newCertificatesManagementClient(limiterDeps)
		require.NoError(t, clientErr)
		assert.IsType(t, &rateLimitedDispatcher{}, certClient.HTTPClient)
	})
}

func TestOCIRetryPolicy(t *testing.T) {
	t.Run("retries throttled OCI service errors", func(t *testing.T) {
		policy := makeOCIRetryPolicy("testService", LoadBalancerConfigDeps{
			RootLogger:       diag.RootTestLogger(),
			RetryEnabled:     true,
			RetryMaxAttempts: 5,
			RetryMaxSleep:    7 * time.Second,
		})
		throttleErr := NewRandomServiceError(
			RandomServiceErrorWithStatusCode(http.StatusTooManyRequests),
			RandomServiceErrorWithCode("TooManyRequests"),
		)

		shouldRetry := policy.ShouldRetryOperation(common.NewOCIOperationResponse(nil, throttleErr, 1))

		assert.True(t, shouldRetry)
		assert.Equal(t, uint(5), policy.MaximumNumberAttempts)
		assert.InDelta(t, (7 * time.Second).Seconds(), policy.MaxSleepBetween, 0)
	})

	t.Run("retries transient OCI server errors", func(t *testing.T) {
		policy := makeOCIRetryPolicy("testService", LoadBalancerConfigDeps{
			RootLogger:   diag.RootTestLogger(),
			RetryEnabled: true,
		})
		serverErr := NewRandomServiceError(
			RandomServiceErrorWithStatusCode(http.StatusInternalServerError),
			RandomServiceErrorWithCode("InternalError"),
		)

		shouldRetry := policy.ShouldRetryOperation(common.NewOCIOperationResponse(nil, serverErr, 1))

		assert.True(t, shouldRetry)
	})

	t.Run("does not retry validation errors", func(t *testing.T) {
		policy := makeOCIRetryPolicy("testService", LoadBalancerConfigDeps{
			RootLogger:   diag.RootTestLogger(),
			RetryEnabled: true,
		})
		validationErr := NewRandomServiceError(
			RandomServiceErrorWithStatusCode(http.StatusBadRequest),
			RandomServiceErrorWithCode("InvalidParameter"),
		)

		shouldRetry := policy.ShouldRetryOperation(common.NewOCIOperationResponse(nil, validationErr, 1))

		assert.False(t, shouldRetry)
	})

	t.Run("does not retry authorization errors", func(t *testing.T) {
		policy := makeOCIRetryPolicy("testService", LoadBalancerConfigDeps{
			RootLogger:   diag.RootTestLogger(),
			RetryEnabled: true,
		})
		authErr := NewRandomServiceError(
			RandomServiceErrorWithStatusCode(http.StatusForbidden),
			RandomServiceErrorWithCode("NotAuthorizedOrNotFound"),
		)

		shouldRetry := policy.ShouldRetryOperation(common.NewOCIOperationResponse(nil, authErr, 1))

		assert.False(t, shouldRetry)
	})
}

func TestClientConfigErrors(t *testing.T) {
	deps := LoadBalancerConfigDeps{
		RootLogger:     diag.RootTestLogger(),
		ConfigProvider: common.NewRawConfigurationProvider("", "", "", "", "", nil),
		Noop:           false,
	}

	_, err := newLoadBalancerClient(deps)
	require.ErrorContains(t, err, "failed to create load balancer client")

	_, err = newNetworkLoadBalancerClient(deps)
	require.ErrorContains(t, err, "failed to create network load balancer client")

	_, err = newCertificatesManagementClient(deps)
	require.ErrorContains(t, err, "failed to create certificates management client")
}
