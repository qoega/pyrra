package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bufbuild/connect-go"
	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/pyrra-dev/pyrra/clickhouse"
	objectivesv1alpha1 "github.com/pyrra-dev/pyrra/proto/objectives/v1alpha1"
	"github.com/pyrra-dev/pyrra/slo"

	chmodule "github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

// setupCHClient creates a ClickHouse container, runs migrations,
// and returns a connected client. Skips in short mode.
func setupCHClient(t *testing.T) *clickhouse.Client {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	const (
		testUser     = "pyrra"
		testPassword = "pyrra_test_pass"
		testDatabase = "pyrra_svc_test"
	)

	container, err := chmodule.Run(ctx,
		"clickhouse:26.1",
		chmodule.WithDatabase(testDatabase),
		chmodule.WithUsername(testUser),
		chmodule.WithPassword(testPassword),
	)
	if err != nil {
		t.Fatalf("failed to start ClickHouse container: %v", err)
	}

	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatalf("failed to get mapped native port: %v", err)
	}

	config := clickhouse.Config{
		Addresses:         []string{fmt.Sprintf("%s:%s", host, port.Port())},
		Database:          testDatabase,
		Username:          testUser,
		Password:          testPassword,
		Protocol:          "native",
		MaxOpenConns:      5,
		MaxIdleConns:      3,
		ConnMaxLifetime:   10 * time.Minute,
		DialTimeout:       10 * time.Second,
		QueryTimeout:      30 * time.Second,
		MVRefreshInterval: 5 * time.Second,
	}

	client, err := clickhouse.NewClient(config)
	if err != nil {
		t.Fatalf("failed to create ClickHouse client: %v", err)
	}
	t.Cleanup(func() { client.Close() })

	migrator := clickhouse.NewMigrator(client, nil)
	if err := migrator.RunMigrations(ctx); err != nil {
		t.Fatalf("failed to run migrations: %v", err)
	}

	return client
}

// setupObjectiveService creates a ClickHouse-backed ObjectiveService for testing.
// It creates a container, runs migrations, inserts test data, and returns the service.
func setupObjectiveService(t *testing.T) (*clickhouseObjectiveService, time.Time) {
	t.Helper()

	client := setupCHClient(t)
	now := time.Now().Truncate(time.Minute)

	// Create test objectives
	objectives := &Objectives{objectives: map[string]slo.Objective{}}

	// Ratio SLO (like http-errors)
	ratioObj := slo.Objective{
		Labels: labels.FromStrings(labels.MetricName, "http-errors", "namespace", "monitoring"),
		Target: 0.995,
		Window: model.Duration(28 * 24 * time.Hour), // 4 weeks
		Indicator: slo.Indicator{
			Ratio: &slo.RatioIndicator{
				Errors:   slo.Metric{Name: "http_errors_total"},
				Total:    slo.Metric{Name: "http_requests_total"},
				Grouping: []string{"handler"},
			},
		},
	}
	objectives.Set(ratioObj)

	// BoolGauge SLO (like probe-success)
	boolGaugeObj := slo.Objective{
		Labels: labels.FromStrings(labels.MetricName, "probe-success", "namespace", "monitoring"),
		Target: 0.999,
		Window: model.Duration(28 * 24 * time.Hour),
		Indicator: slo.Indicator{
			BoolGauge: &slo.BoolGaugeIndicator{
				Metric:   slo.Metric{Name: "probe_success"},
				Grouping: []string{"instance"},
			},
		},
	}
	objectives.Set(boolGaugeObj)

	// Insert test data into slo_recordings
	ctx := context.Background()

	// Insert data at multiple timestamps for range queries
	for i := 0; i < 10; i++ {
		ts := now.Add(-time.Duration(i) * 30 * time.Minute)
		batch, err := client.PrepareBatch(ctx, "INSERT INTO slo_recordings (timestamp, metric_name, labels, value)")
		require.NoError(t, err)

		// Ratio SLO data
		require.NoError(t, batch.Append(ts, "http_requests:increase4w",
			map[string]string{"slo": "http-errors", "handler": "/api"}, 100000.0+float64(i)*1000))
		require.NoError(t, batch.Append(ts, "http_errors:error_increase4w",
			map[string]string{"slo": "http-errors", "handler": "/api"}, 500.0+float64(i)*10))
		require.NoError(t, batch.Append(ts, "http_requests:burnrate5m",
			map[string]string{"slo": "http-errors", "handler": "/api"}, 0.005+float64(i)*0.001))
		require.NoError(t, batch.Append(ts, "http_requests:burnrate1h",
			map[string]string{"slo": "http-errors", "handler": "/api"}, 0.004+float64(i)*0.0005))

		// BoolGauge SLO data
		require.NoError(t, batch.Append(ts, "probe_success:count4w",
			map[string]string{"slo": "probe-success", "instance": "https://api.example.com"}, 50000.0+float64(i)*500))
		require.NoError(t, batch.Append(ts, "probe_success:sum4w",
			map[string]string{"slo": "probe-success", "instance": "https://api.example.com"}, 49990.0+float64(i)*500))
		require.NoError(t, batch.Append(ts, "probe_success:burnrate5m",
			map[string]string{"slo": "probe-success", "instance": "https://api.example.com"}, 0.001+float64(i)*0.0001))

		require.NoError(t, batch.Send())
	}

	svc := &clickhouseObjectiveService{
		logger:     log.NewNopLogger(),
		client:     client,
		objectives: objectives,
	}

	return svc, now
}

func TestNativeObjectiveService_List(t *testing.T) {
	svc, _ := setupObjectiveService(t)

	t.Run("list all", func(t *testing.T) {
		resp, err := svc.List(context.Background(), connect.NewRequest(&objectivesv1alpha1.ListRequest{}))
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(resp.Msg.Objectives), 2)

		// Queries field is no longer populated (deprecated)
		for _, o := range resp.Msg.Objectives {
			assert.Nil(t, o.Queries, "Queries should not be populated for %s", o.Labels)
		}
	})

	t.Run("list with expr", func(t *testing.T) {
		resp, err := svc.List(context.Background(), connect.NewRequest(&objectivesv1alpha1.ListRequest{
			Expr: `{__name__="http-errors"}`,
		}))
		require.NoError(t, err)
		require.Len(t, resp.Msg.Objectives, 1)
	})

	t.Run("list with no match", func(t *testing.T) {
		_, err := svc.List(context.Background(), connect.NewRequest(&objectivesv1alpha1.ListRequest{
			Expr: `{__name__="nonexistent"}`,
		}))
		require.Error(t, err)
	})
}

func TestNativeObjectiveService_GetStatus_Ratio(t *testing.T) {
	svc, _ := setupObjectiveService(t)

	resp, err := svc.GetStatus(context.Background(), connect.NewRequest(&objectivesv1alpha1.GetStatusRequest{
		Expr: `{__name__="http-errors"}`,
	}))
	require.NoError(t, err)
	require.NotEmpty(t, resp.Msg.Status, "should return at least one status")

	status := resp.Msg.Status[0]
	assert.NotNil(t, status.Availability)
	assert.Greater(t, status.Availability.Total, 0.0, "total should be > 0")
	assert.Greater(t, status.Availability.Errors, 0.0, "errors should be > 0")
	assert.Greater(t, status.Availability.Percentage, 0.0, "percentage should be > 0")
	assert.Less(t, status.Availability.Percentage, 1.01, "percentage should be <= 1")

	assert.NotNil(t, status.Budget)
	assert.Greater(t, status.Budget.Total, 0.0, "budget total should be > 0")

	t.Logf("Ratio SLO status: total=%.0f errors=%.0f availability=%.6f budget_remaining=%.6f",
		status.Availability.Total, status.Availability.Errors,
		status.Availability.Percentage, status.Budget.Remaining)
}

func TestNativeObjectiveService_GetStatus_BoolGauge(t *testing.T) {
	svc, _ := setupObjectiveService(t)

	resp, err := svc.GetStatus(context.Background(), connect.NewRequest(&objectivesv1alpha1.GetStatusRequest{
		Expr: `{__name__="probe-success"}`,
	}))
	require.NoError(t, err)
	require.NotEmpty(t, resp.Msg.Status, "should return at least one status")

	status := resp.Msg.Status[0]
	assert.NotNil(t, status.Availability)
	assert.Greater(t, status.Availability.Total, 0.0, "total should be > 0")
	assert.Greater(t, status.Availability.Percentage, 0.0, "percentage should be > 0")

	t.Logf("BoolGauge SLO status: total=%.0f errors=%.0f availability=%.6f",
		status.Availability.Total, status.Availability.Errors,
		status.Availability.Percentage)
}

func TestNativeObjectiveService_GetAlerts(t *testing.T) {
	svc, _ := setupObjectiveService(t)

	t.Run("with inactive", func(t *testing.T) {
		resp, err := svc.GetAlerts(context.Background(), connect.NewRequest(&objectivesv1alpha1.GetAlertsRequest{
			Expr:     `{__name__="http-errors"}`,
			Inactive: true,
		}))
		require.NoError(t, err)
		require.NotEmpty(t, resp.Msg.Alerts, "should have alerts when inactive=true")

		for _, alert := range resp.Msg.Alerts {
			assert.NotEmpty(t, alert.Labels, "alert should have labels")
			assert.NotEmpty(t, alert.Severity, "alert should have severity")
			assert.NotNil(t, alert.Short, "alert should have short burnrate")
			assert.NotNil(t, alert.Long, "alert should have long burnrate")
			assert.Greater(t, alert.Factor, 0.0, "alert should have positive factor")

			t.Logf("Alert: severity=%s short_window=%s short_current=%.4f long_window=%s long_current=%.4f state=%s",
				alert.Severity,
				alert.Short.Window.AsDuration(),
				alert.Short.Current,
				alert.Long.Window.AsDuration(),
				alert.Long.Current,
				alert.State)
		}
	})

	t.Run("without inactive", func(t *testing.T) {
		resp, err := svc.GetAlerts(context.Background(), connect.NewRequest(&objectivesv1alpha1.GetAlertsRequest{
			Expr:     `{__name__="http-errors"}`,
			Inactive: false,
		}))
		require.NoError(t, err)
		// May or may not have alerts depending on burnrate values vs thresholds
		t.Logf("Active alerts: %d", len(resp.Msg.Alerts))
	})
}

func TestNativeObjectiveService_GraphErrorBudget(t *testing.T) {
	svc, now := setupObjectiveService(t)

	start := now.Add(-5 * time.Hour)
	resp, err := svc.GraphErrorBudget(context.Background(), connect.NewRequest(&objectivesv1alpha1.GraphErrorBudgetRequest{
		Expr:  `{__name__="http-errors"}`,
		Start: timestamppb.New(start),
		End:   timestamppb.New(now),
	}))
	require.NoError(t, err)
	require.NotNil(t, resp.Msg.Timeseries, "should have timeseries data")
	require.NotEmpty(t, resp.Msg.Timeseries.Series, "should have series")

	// First series should be timestamps, second should be values
	require.GreaterOrEqual(t, len(resp.Msg.Timeseries.Series), 2, "should have timestamps + at least one value series")
	timestamps := resp.Msg.Timeseries.Series[0].Values
	values := resp.Msg.Timeseries.Series[1].Values
	assert.Equal(t, len(timestamps), len(values), "timestamps and values should have same length")
	assert.Greater(t, len(timestamps), 0, "should have data points")

	t.Logf("Error budget graph: %d data points, query=%s", len(timestamps), resp.Msg.Timeseries.Query)
	for i, ts := range timestamps {
		t.Logf("  t=%.0f budget=%.6f", ts, values[i])
	}
}

func TestNativeObjectiveService_GraphRate(t *testing.T) {
	svc, now := setupObjectiveService(t)

	start := now.Add(-5 * time.Hour)
	resp, err := svc.GraphRate(context.Background(), connect.NewRequest(&objectivesv1alpha1.GraphRateRequest{
		Expr:  `{__name__="http-errors"}`,
		Start: timestamppb.New(start),
		End:   timestamppb.New(now),
	}))
	require.NoError(t, err)
	require.NotNil(t, resp.Msg.Timeseries, "should have timeseries data")
	require.NotEmpty(t, resp.Msg.Timeseries.Series, "should have series")

	t.Logf("Rate graph: %d series, query=%s",
		len(resp.Msg.Timeseries.Series), resp.Msg.Timeseries.Query)
}

func TestNativeObjectiveService_GraphErrors(t *testing.T) {
	svc, now := setupObjectiveService(t)

	start := now.Add(-5 * time.Hour)
	resp, err := svc.GraphErrors(context.Background(), connect.NewRequest(&objectivesv1alpha1.GraphErrorsRequest{
		Expr:  `{__name__="http-errors"}`,
		Start: timestamppb.New(start),
		End:   timestamppb.New(now),
	}))
	require.NoError(t, err)
	require.NotNil(t, resp.Msg.Timeseries, "should have timeseries data")
	require.NotEmpty(t, resp.Msg.Timeseries.Series, "should have series")

	t.Logf("Errors graph: %d series, query=%s",
		len(resp.Msg.Timeseries.Series), resp.Msg.Timeseries.Query)
}

func TestNativeObjectiveService_GraphDuration(t *testing.T) {
	svc, now := setupObjectiveService(t)

	start := now.Add(-5 * time.Hour)
	resp, err := svc.GraphDuration(context.Background(), connect.NewRequest(&objectivesv1alpha1.GraphDurationRequest{
		Expr:  `{__name__="http-errors"}`,
		Start: timestamppb.New(start),
		End:   timestamppb.New(now),
	}))
	require.NoError(t, err)
	// Duration is expected to return empty since it requires raw histogram data
	assert.Nil(t, resp.Msg.Timeseries, "duration should return nil timeseries without raw data")
}

func TestNativeObjectiveService_GetStatus_WithGrouping(t *testing.T) {
	svc, _ := setupObjectiveService(t)

	resp, err := svc.GetStatus(context.Background(), connect.NewRequest(&objectivesv1alpha1.GetStatusRequest{
		Expr:     `{__name__="http-errors"}`,
		Grouping: fmt.Sprintf(`{handler="/api"}`),
	}))
	require.NoError(t, err)
	require.NotEmpty(t, resp.Msg.Status, "should return status with grouping filter")

	status := resp.Msg.Status[0]
	assert.Greater(t, status.Availability.Total, 0.0)
	t.Logf("With grouping: total=%.0f errors=%.0f", status.Availability.Total, status.Availability.Errors)
}

func TestNativeObjectiveService_GraphErrorBudget_BoolGauge(t *testing.T) {
	svc, now := setupObjectiveService(t)

	start := now.Add(-5 * time.Hour)
	resp, err := svc.GraphErrorBudget(context.Background(), connect.NewRequest(&objectivesv1alpha1.GraphErrorBudgetRequest{
		Expr:  `{__name__="probe-success"}`,
		Start: timestamppb.New(start),
		End:   timestamppb.New(now),
	}))
	require.NoError(t, err)
	require.NotNil(t, resp.Msg.Timeseries, "should have timeseries data")
	require.NotEmpty(t, resp.Msg.Timeseries.Series, "should have series")

	t.Logf("BoolGauge error budget: %d series", len(resp.Msg.Timeseries.Series))
}
