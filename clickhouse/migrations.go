package clickhouse

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// Migration represents a schema migration
type Migration struct {
	Version int
	Name    string
	Up      func(ctx context.Context, c *Client) error
	Down    func(ctx context.Context, c *Client) error
}

// Migrations contains all schema migrations in order
var Migrations = []Migration{
	{
		Version: 1,
		Name:    "initial_schema",
		Up:      migrateV1Up,
		Down:    migrateV1Down,
	},
}

func migrateV1Up(ctx context.Context, c *Client) error {
	queries := []string{
		// Create metrics_raw table - ingestion point for raw metrics
		`CREATE TABLE IF NOT EXISTS metrics_raw (
			timestamp DateTime64(3),
			metric_name LowCardinality(String),
			labels Map(String, String),
			value Float64,
			metric_type Enum8('counter' = 1, 'gauge' = 2, 'histogram' = 3, 'summary' = 4, 'unknown' = 0) DEFAULT 'unknown',
			_date Date MATERIALIZED toDate(timestamp),
			_labels_hash UInt64 MATERIALIZED xxHash64(toString(labels))
		) ENGINE = MergeTree()
		PARTITION BY _date
		ORDER BY (metric_name, _labels_hash, timestamp)
		TTL toDateTime(timestamp) + INTERVAL 35 DAY
		SETTINGS index_granularity = 8192`,

		// Create slo_recordings table - synthetic recording rule metrics
		`CREATE TABLE IF NOT EXISTS slo_recordings (
			timestamp DateTime,
			metric_name LowCardinality(String),
			labels Map(String, String),
			value Float64,
			labels_hash UInt64 MATERIALIZED xxHash64(toString(labels)),
			_version DateTime DEFAULT now()
		) ENGINE = ReplacingMergeTree(_version)
		PARTITION BY toYYYYMM(timestamp)
		ORDER BY (metric_name, labels_hash, timestamp)
		TTL timestamp + INTERVAL 35 DAY`,
	}

	for i, q := range queries {
		if err := c.Exec(ctx, q); err != nil {
			return fmt.Errorf("migration v1 query %d: %w", i+1, err)
		}
	}

	return nil
}

func migrateV1Down(ctx context.Context, c *Client) error {
	tables := []string{
		"metrics_raw",
		"slo_recordings",
	}

	for _, t := range tables {
		if err := c.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", t)); err != nil {
			return fmt.Errorf("drop table %s: %w", t, err)
		}
	}

	return nil
}

// Migrator handles running schema migrations
type Migrator struct {
	client *Client
	logger log.Logger
}

// NewMigrator creates a new migrator
func NewMigrator(client *Client, logger log.Logger) *Migrator {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &Migrator{
		client: client,
		logger: logger,
	}
}

// RunMigrations executes pending migrations
func (m *Migrator) RunMigrations(ctx context.Context) error {
	// Create migrations table if not exists
	err := m.client.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS _migrations (
			version UInt32,
			name String,
			applied_at DateTime DEFAULT now()
		) ENGINE = ReplacingMergeTree(applied_at)
		ORDER BY version
	`)
	if err != nil {
		return fmt.Errorf("create migrations table: %w", err)
	}

	// Get applied migrations
	applied, err := m.getAppliedMigrations(ctx)
	if err != nil {
		return fmt.Errorf("get applied migrations: %w", err)
	}

	// Apply pending migrations
	for _, migration := range Migrations {
		if applied[migration.Version] {
			level.Debug(m.logger).Log("msg", "migration already applied", "version", migration.Version, "name", migration.Name)
			continue
		}

		level.Info(m.logger).Log("msg", "applying migration", "version", migration.Version, "name", migration.Name)

		if err := migration.Up(ctx, m.client); err != nil {
			return fmt.Errorf("migration %d (%s): %w", migration.Version, migration.Name, err)
		}

		err := m.client.Exec(ctx,
			"INSERT INTO _migrations (version, name) VALUES (?, ?)",
			migration.Version, migration.Name,
		)
		if err != nil {
			return fmt.Errorf("record migration %d: %w", migration.Version, err)
		}

		level.Info(m.logger).Log("msg", "migration applied", "version", migration.Version, "name", migration.Name)
	}

	return nil
}

// getAppliedMigrations returns a map of applied migration versions
func (m *Migrator) getAppliedMigrations(ctx context.Context) (map[int]bool, error) {
	rows, err := m.client.Query(ctx, "SELECT version FROM _migrations FINAL ORDER BY version")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := make(map[int]bool)
	for rows.Next() {
		var v uint32
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("scan migration version: %w", err)
		}
		applied[int(v)] = true
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate migrations: %w", err)
	}

	return applied, nil
}

// RollbackLast rolls back the last applied migration
func (m *Migrator) RollbackLast(ctx context.Context) error {
	applied, err := m.getAppliedMigrations(ctx)
	if err != nil {
		return err
	}

	// Find the highest applied version
	var lastVersion int
	for v := range applied {
		if v > lastVersion {
			lastVersion = v
		}
	}

	if lastVersion == 0 {
		level.Info(m.logger).Log("msg", "no migrations to rollback")
		return nil
	}

	// Find the migration
	var migration *Migration
	for i := range Migrations {
		if Migrations[i].Version == lastVersion {
			migration = &Migrations[i]
			break
		}
	}

	if migration == nil {
		return fmt.Errorf("migration version %d not found", lastVersion)
	}

	level.Info(m.logger).Log("msg", "rolling back migration", "version", migration.Version, "name", migration.Name)

	if err := migration.Down(ctx, m.client); err != nil {
		return fmt.Errorf("rollback migration %d: %w", migration.Version, err)
	}

	// Remove from _migrations
	if err := m.client.Exec(ctx, "ALTER TABLE _migrations DELETE WHERE version = ?", uint32(lastVersion)); err != nil {
		return fmt.Errorf("remove migration record: %w", err)
	}

	level.Info(m.logger).Log("msg", "migration rolled back", "version", migration.Version, "name", migration.Name)

	return nil
}

// CurrentVersion returns the current schema version
func (m *Migrator) CurrentVersion(ctx context.Context) (int, error) {
	applied, err := m.getAppliedMigrations(ctx)
	if err != nil {
		return 0, err
	}

	var maxVersion int
	for v := range applied {
		if v > maxVersion {
			maxVersion = v
		}
	}

	return maxVersion, nil
}



