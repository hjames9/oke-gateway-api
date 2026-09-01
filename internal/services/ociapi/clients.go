package ociapi

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/oracle/oci-go-sdk/v65/certificatesmanagement"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/loadbalancer"
	"github.com/oracle/oci-go-sdk/v65/networkloadbalancer"
	"go.uber.org/dig"
)

const ociRetryExponentialBackoffBase = 2.0

type LoadBalancerConfigDeps struct {
	dig.In

	RootLogger *slog.Logger

	ConfigProvider common.ConfigurationProvider

	// This can be set via APP_OCIAPI_NOOP env variable
	Noop bool `name:"config.ociapi.noop"`

	RetryEnabled     bool          `name:"config.ociapi.retry.enabled"`
	RetryMaxAttempts int           `name:"config.ociapi.retry.max-attempts"`
	RetryMaxSleep    time.Duration `name:"config.ociapi.retry.max-sleep"`

	RequestLimiter *ociRequestLimiter
}

func newLoadBalancerClient(
	deps LoadBalancerConfigDeps,
) (loadbalancer.LoadBalancerClient, error) {
	if deps.Noop {
		deps.RootLogger.Warn("OCI API client is in noop mode")
		return loadbalancer.LoadBalancerClient{}, nil
	}

	client, err := loadbalancer.NewLoadBalancerClientWithConfigurationProvider(deps.ConfigProvider)
	if err != nil {
		return loadbalancer.LoadBalancerClient{}, fmt.Errorf("failed to create load balancer client: %w", err)
	}
	configureOCIRetryPolicy(&client.BaseClient, "loadBalancer", deps)
	configureOCIRequestLimiter(&client.BaseClient, "loadBalancer", deps.RequestLimiter)
	return client, nil
}

func newNetworkLoadBalancerClient(
	deps LoadBalancerConfigDeps,
) (networkloadbalancer.NetworkLoadBalancerClient, error) {
	if deps.Noop {
		deps.RootLogger.Warn("OCI API client is in noop mode")
		return networkloadbalancer.NetworkLoadBalancerClient{}, nil
	}

	client, err := networkloadbalancer.NewNetworkLoadBalancerClientWithConfigurationProvider(deps.ConfigProvider)
	if err != nil {
		return networkloadbalancer.NetworkLoadBalancerClient{}, fmt.Errorf(
			"failed to create network load balancer client: %w",
			err,
		)
	}
	configureOCIRetryPolicy(&client.BaseClient, "networkLoadBalancer", deps)
	configureOCIRequestLimiter(&client.BaseClient, "networkLoadBalancer", deps.RequestLimiter)
	return client, nil
}

func newCertificatesManagementClient(
	deps LoadBalancerConfigDeps,
) (certificatesmanagement.CertificatesManagementClient, error) {
	if deps.Noop {
		deps.RootLogger.Warn("OCI API client is in noop mode")
		return certificatesmanagement.CertificatesManagementClient{}, nil
	}

	client, err := certificatesmanagement.NewCertificatesManagementClientWithConfigurationProvider(deps.ConfigProvider)
	if err != nil {
		return certificatesmanagement.CertificatesManagementClient{}, fmt.Errorf(
			"failed to create certificates management client: %w",
			err,
		)
	}
	configureOCIRetryPolicy(&client.BaseClient, "certificatesManagement", deps)
	configureOCIRequestLimiter(&client.BaseClient, "certificatesManagement", deps.RequestLimiter)
	return client, nil
}

func configureOCIRetryPolicy(client *common.BaseClient, serviceName string, deps LoadBalancerConfigDeps) {
	if !deps.RetryEnabled {
		return
	}

	retryPolicy := makeOCIRetryPolicy(serviceName, deps)
	client.SetCustomClientConfiguration(common.CustomClientConfiguration{
		RetryPolicy: &retryPolicy,
	})
}

func makeOCIRetryPolicy(serviceName string, deps LoadBalancerConfigDeps) common.RetryPolicy {
	retryPolicy := common.DefaultRetryPolicy()
	shouldRetry := retryPolicy.ShouldRetryOperation
	options := []common.RetryPolicyOption{
		common.ReplaceWithValuesFromRetryPolicy(retryPolicy),
		common.WithShouldRetryOperation(func(response common.OCIOperationResponse) bool {
			retryable := shouldRetry(response)
			if retryable && deps.RootLogger != nil {
				logRetryableOCIError(deps.RootLogger, serviceName, response)
			}
			return retryable
		}),
	}
	if deps.RetryMaxAttempts > 0 {
		options = append(options, common.WithMaximumNumberAttempts(uint(deps.RetryMaxAttempts)))
	}
	if deps.RetryMaxSleep > 0 {
		options = append(options, common.WithExponentialBackoff(deps.RetryMaxSleep, ociRetryExponentialBackoffBase))
	}
	return common.NewRetryPolicyWithOptions(options...)
}

func logRetryableOCIError(logger *slog.Logger, serviceName string, response common.OCIOperationResponse) {
	attrs := []any{
		slog.String("serviceName", serviceName),
		slog.Int("attemptNumber", int(response.AttemptNumber)),
	}

	var serviceErr common.ServiceError
	if errors.As(response.Error, &serviceErr) {
		attrs = append(attrs,
			slog.Int("statusCode", serviceErr.GetHTTPStatusCode()),
			slog.String("errorCode", serviceErr.GetCode()),
			slog.String("opcRequestID", serviceErr.GetOpcRequestID()),
		)
	}

	if response.Error != nil {
		attrs = append(attrs, slog.String("error", response.Error.Error()))
	}

	logger.WithGroup("oci-retry").Warn(
		"Retrying OCI request after retryable error",
		attrs...,
	)
}
