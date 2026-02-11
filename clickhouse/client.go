package clickhouse

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Client provides access to ClickHouse
type Client struct {
	conn   driver.Conn
	config Config
}

// NewClient creates a new ClickHouse client with retry logic
func NewClient(config Config) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	var conn driver.Conn
	var err error

	if config.Protocol == "http" {
		// Use HTTP protocol (for ClickHouse Cloud)
		conn, err = newHTTPClient(config)
	} else {
		// Use native protocol (default)
		conn, err = newNativeClient(config)
	}

	if err != nil {
		return nil, err
	}

	// Verify connection with retry
	client := &Client{conn: conn, config: config}
	if err := client.pingWithRetry(context.Background(), 3); err != nil {
		conn.Close()
		return nil, err
	}

	return client, nil
}

// newNativeClient creates a client using the native ClickHouse protocol
func newNativeClient(config Config) (driver.Conn, error) {
	opts := &clickhouse.Options{
		Addr: config.Addresses,
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.Username,
			Password: config.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": int(config.QueryTimeout.Seconds()),
		},
		DialTimeout:     config.DialTimeout,
		MaxOpenConns:    config.MaxOpenConns,
		MaxIdleConns:    config.MaxIdleConns,
		ConnMaxLifetime: config.ConnMaxLifetime,

		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	}

	// Configure TLS if enabled
	if config.TLSEnabled {
		tlsConfig, err := buildTLSConfig(config)
		if err != nil {
			return nil, fmt.Errorf("build TLS config: %w", err)
		}
		opts.TLS = tlsConfig
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrConnectionFailed, err)
	}

	return conn, nil
}

// newHTTPClient creates a client using HTTP protocol (for ClickHouse Cloud)
func newHTTPClient(config Config) (driver.Conn, error) {
	opts := &clickhouse.Options{
		Addr:     config.Addresses,
		Protocol: clickhouse.HTTP,
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.Username,
			Password: config.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": int(config.QueryTimeout.Seconds()),
		},
		DialTimeout:     config.DialTimeout,
		MaxOpenConns:    config.MaxOpenConns,
		MaxIdleConns:    config.MaxIdleConns,
		ConnMaxLifetime: config.ConnMaxLifetime,
	}

	// Configure TLS if enabled
	if config.TLSEnabled {
		tlsConfig, err := buildTLSConfig(config)
		if err != nil {
			return nil, fmt.Errorf("build TLS config: %w", err)
		}
		opts.TLS = tlsConfig
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrConnectionFailed, err)
	}

	return conn, nil
}

// buildTLSConfig creates a TLS configuration from the config
func buildTLSConfig(config Config) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: config.TLSSkipVerify,
	}

	if config.TLSCAFile != "" {
		caCert, err := os.ReadFile(config.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to append CA certificate")
		}
		tlsConfig.RootCAs = caCertPool
	}

	if config.TLSCertFile != "" && config.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(config.TLSCertFile, config.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

// pingWithRetry pings ClickHouse with retry logic
func (c *Client) pingWithRetry(ctx context.Context, maxRetries int) error {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		pingCtx, cancel := context.WithTimeout(ctx, c.config.DialTimeout)
		lastErr = c.conn.Ping(pingCtx)
		cancel()

		if lastErr == nil {
			return nil
		}

		if i < maxRetries-1 {
			time.Sleep(time.Second * time.Duration(i+1))
		}
	}
	return fmt.Errorf("%w: %v", ErrPingFailed, lastErr)
}

// EnsureDatabase creates the configured database if it doesn't exist.
// It opens a temporary connection to the "default" database, runs
// CREATE DATABASE IF NOT EXISTS, then closes the temporary connection.
// Call this before NewClient when the target database may not yet exist.
func EnsureDatabase(ctx context.Context, config Config) error {
	if config.Database == "" || config.Database == "default" {
		return nil
	}

	// Clone config with database = "default"
	tmpCfg := config
	tmpCfg.Database = "default"

	var conn driver.Conn
	var err error
	if tmpCfg.Protocol == "http" {
		conn, err = newHTTPClient(tmpCfg)
	} else {
		conn, err = newNativeClient(tmpCfg)
	}
	if err != nil {
		return fmt.Errorf("temp connection: %w", err)
	}
	defer conn.Close()

	if err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", config.Database)); err != nil {
		return fmt.Errorf("create database: %w", err)
	}
	return nil
}

// Ready checks if ClickHouse is available
func (c *Client) Ready(ctx context.Context) error {
	return c.conn.Ping(ctx)
}

// Close closes the connection
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Exec executes a statement
func (c *Client) Exec(ctx context.Context, query string, args ...interface{}) error {
	return c.conn.Exec(ctx, query, args...)
}

// PrepareBatch prepares a batch insert
func (c *Client) PrepareBatch(ctx context.Context, query string) (driver.Batch, error) {
	return c.conn.PrepareBatch(ctx, query)
}

// Query executes a query and returns rows
func (c *Client) Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	return c.conn.Query(ctx, query, args...)
}

// QueryRow executes a query and returns a single row
func (c *Client) QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row {
	return c.conn.QueryRow(ctx, query, args...)
}

// ServerVersion returns the ClickHouse server version
func (c *Client) ServerVersion() (*driver.ServerVersion, error) {
	return c.conn.ServerVersion()
}

// Config returns the client configuration
func (c *Client) Config() Config {
	return c.config
}

