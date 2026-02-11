package clickhouse

import (
	"context"
	"fmt"
	"testing"
	"time"

	chmodule "github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

// setupClickHouseContainer starts a ClickHouse container for integration tests.
// It returns a connected *Client and registers cleanup via t.Cleanup.
// The test is skipped if testing.Short() is true.
func setupClickHouseContainer(t *testing.T) *Client {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	const (
		testUser     = "pyrra"
		testPassword = "pyrra_test_pass"
		testDatabase = "pyrra_test"
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
		if err := container.Terminate(ctx); err != nil {
			t.Logf("failed to terminate ClickHouse container: %v", err)
		}
	})

	// Get the native port
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatalf("failed to get mapped native port: %v", err)
	}

	config := Config{
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

	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("failed to create ClickHouse client: %v", err)
	}

	t.Cleanup(func() {
		client.Close()
	})

	return client
}

// setupClickHouseWithMigrations starts a ClickHouse container and runs migrations.
// Returns a connected *Client ready for testing with all schema tables created.
func setupClickHouseWithMigrations(t *testing.T) *Client {
	t.Helper()

	client := setupClickHouseContainer(t)

	ctx := context.Background()
	migrator := NewMigrator(client, nil)
	if err := migrator.RunMigrations(ctx); err != nil {
		t.Fatalf("failed to run migrations: %v", err)
	}

	return client
}
