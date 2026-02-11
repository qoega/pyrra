package clickhouse

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/pyrra-dev/pyrra/slo"
	"github.com/stretchr/testify/require"
)

func TestNewProvisioner(t *testing.T) {
	config := DefaultConfig()
	p := NewProvisioner(nil, config, nil)
	require.NotNil(t, p)
	require.NotNil(t, p.generator)
	require.NotNil(t, p.activeMVs)
	require.NotNil(t, p.objectiveMVs)
}

func TestProvisioner_GetActiveMVs_Empty(t *testing.T) {
	config := DefaultConfig()
	p := NewProvisioner(nil, config, nil)

	mvs := p.GetActiveMVs()
	require.Empty(t, mvs)
}

func TestProvisioner_GetObjectiveMVs_NotFound(t *testing.T) {
	config := DefaultConfig()
	p := NewProvisioner(nil, config, nil)

	mvs := p.GetObjectiveMVs("nonexistent")
	require.Nil(t, mvs)
}

func createTestObjective(name string) slo.Objective {
	return slo.Objective{
		Labels: labels.FromStrings(labels.MetricName, name),
		Window: model.Duration(28 * 24 * time.Hour),
		Target: 0.99,
		Indicator: slo.Indicator{
			Ratio: &slo.RatioIndicator{
				Errors: slo.Metric{
					Name: "http_requests_total",
					LabelMatchers: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "job", Value: "api"},
						{Type: labels.MatchRegexp, Name: "code", Value: "5.."},
					},
				},
				Total: slo.Metric{
					Name: "http_requests_total",
					LabelMatchers: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "job", Value: "api"},
					},
				},
				Grouping: []string{"handler"},
			},
		},
	}
}

// Integration test - requires ClickHouse
func TestProvisioner_ProvisionObjective_Integration(t *testing.T) {
	client := setupClickHouseWithMigrations(t)

	ctx := context.Background()
	p := NewProvisioner(client, client.Config(), nil)

	// Provision a test objective
	obj := createTestObjective("test-slo")
	err := p.ProvisionObjective(ctx, obj)
	require.NoError(t, err)

	// Verify MVs were created
	mvs := p.GetActiveMVs()
	require.NotEmpty(t, mvs)

	objMVs := p.GetObjectiveMVs("test-slo")
	require.NotEmpty(t, objMVs)

	// Deprovision
	err = p.DeprovisionObjective(ctx, "test-slo")
	require.NoError(t, err)

	// Verify MVs were removed
	objMVs = p.GetObjectiveMVs("test-slo")
	require.Nil(t, objMVs)
}

// Integration test for ReconcileAll
func TestProvisioner_ReconcileAll_Integration(t *testing.T) {
	client := setupClickHouseWithMigrations(t)

	ctx := context.Background()
	p := NewProvisioner(client, client.Config(), nil)

	// Start with two objectives
	objectives := []slo.Objective{
		createTestObjective("slo-one"),
		createTestObjective("slo-two"),
	}

	err := p.ReconcileAll(ctx, objectives)
	require.NoError(t, err)

	// Verify both were created
	require.NotNil(t, p.GetObjectiveMVs("slo-one"))
	require.NotNil(t, p.GetObjectiveMVs("slo-two"))

	// Remove one objective and reconcile
	objectives = []slo.Objective{
		createTestObjective("slo-one"),
	}

	err = p.ReconcileAll(ctx, objectives)
	require.NoError(t, err)

	// Verify slo-two was removed
	require.NotNil(t, p.GetObjectiveMVs("slo-one"))
	require.Nil(t, p.GetObjectiveMVs("slo-two"))

	// Cleanup
	err = p.DropAllMVs(ctx)
	require.NoError(t, err)
	require.Empty(t, p.GetActiveMVs())
}

// Integration test for SyncFromDatabase
func TestProvisioner_SyncFromDatabase_Integration(t *testing.T) {
	client := setupClickHouseWithMigrations(t)

	ctx := context.Background()
	p := NewProvisioner(client, client.Config(), nil)

	// Create some MVs
	obj := createTestObjective("sync-test")
	err := p.ProvisionObjective(ctx, obj)
	require.NoError(t, err)

	initialMVs := p.GetActiveMVs()
	require.NotEmpty(t, initialMVs)

	// Create a new provisioner and sync from database
	p2 := NewProvisioner(client, client.Config(), nil)
	require.Empty(t, p2.GetActiveMVs())

	err = p2.SyncFromDatabase(ctx)
	require.NoError(t, err)

	// Should have discovered the MVs
	syncedMVs := p2.GetActiveMVs()
	require.NotEmpty(t, syncedMVs)

	// Cleanup
	_ = p.DropAllMVs(ctx)
}

func TestProvisioner_ProvisionObjective_EmptyName(t *testing.T) {
	config := DefaultConfig()
	p := NewProvisioner(nil, config, nil)

	obj := slo.Objective{
		Labels: labels.Labels{}, // No __name__ label
		Window: model.Duration(28 * 24 * time.Hour),
	}

	err := p.ProvisionObjective(context.Background(), obj)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no name")
}

func TestProvisioner_DeprovisionObjective_NotFound(t *testing.T) {
	config := DefaultConfig()
	p := NewProvisioner(nil, config, nil)

	// Should not error for non-existent objective
	err := p.DeprovisionObjective(context.Background(), "nonexistent")
	require.NoError(t, err)
}

func TestProvisioner_ConcurrentAccess(t *testing.T) {
	config := DefaultConfig()
	p := NewProvisioner(nil, config, nil)

	// Simulate concurrent access by checking mutex behavior
	// This is a basic test - in production you'd want more thorough concurrency testing
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func() {
			_ = p.GetActiveMVs()
			_ = p.GetObjectiveMVs("test")
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}



