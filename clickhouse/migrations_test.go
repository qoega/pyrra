package clickhouse

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigrations_Order(t *testing.T) {
	// Verify migrations are ordered correctly
	for i := 1; i < len(Migrations); i++ {
		require.Greater(t, Migrations[i].Version, Migrations[i-1].Version,
			"migrations must be in order by version")
	}
}

func TestMigrations_HaveUpAndDown(t *testing.T) {
	for _, m := range Migrations {
		require.NotNil(t, m.Up, "migration %d must have Up function", m.Version)
		require.NotNil(t, m.Down, "migration %d must have Down function", m.Version)
		require.NotEmpty(t, m.Name, "migration %d must have a name", m.Version)
	}
}

func TestMigrations_VersionsUnique(t *testing.T) {
	versions := make(map[int]bool)
	for _, m := range Migrations {
		require.False(t, versions[m.Version], "duplicate migration version: %d", m.Version)
		versions[m.Version] = true
	}
}

// TestMigrator_Integration tests migrations against a real ClickHouse instance
func TestMigrator_Integration(t *testing.T) {
	client := setupClickHouseContainer(t)

	ctx := context.Background()
	migrator := NewMigrator(client, nil)

	// Run migrations
	err := migrator.RunMigrations(ctx)
	require.NoError(t, err)

	// Check current version
	version, err := migrator.CurrentVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, len(Migrations), version)

	// Verify tables exist
	tables := []string{"metrics_raw", "slo_recordings", "slo_window_aggregates", "slo_timeseries"}
	for _, table := range tables {
		var count uint64
		row := client.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = ? AND name = ?", client.Config().Database, table)
		err := row.Scan(&count)
		require.NoError(t, err)
		require.Equal(t, uint64(1), count, "table %s should exist", table)
	}

	// Running migrations again should be idempotent
	err = migrator.RunMigrations(ctx)
	require.NoError(t, err)

	// Rollback
	err = migrator.RollbackLast(ctx)
	require.NoError(t, err)

	// Verify tables are dropped
	for _, table := range tables {
		var count uint64
		row := client.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = ? AND name = ?", client.Config().Database, table)
		err := row.Scan(&count)
		require.NoError(t, err)
		require.Equal(t, uint64(0), count, "table %s should be dropped", table)
	}
}

// TestMigration_V1_SQLSyntax verifies the SQL is valid
func TestMigration_V1_SQLSyntax(t *testing.T) {
	client := setupClickHouseContainer(t)

	ctx := context.Background()

	// Run up migration
	err := migrateV1Up(ctx, client)
	require.NoError(t, err, "V1 Up migration should succeed")

	// Run down migration
	err = migrateV1Down(ctx, client)
	require.NoError(t, err, "V1 Down migration should succeed")
}



