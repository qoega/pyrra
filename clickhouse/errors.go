package clickhouse

import "errors"

var (
	// Configuration errors
	ErrNoAddresses   = errors.New("clickhouse: at least one address is required")
	ErrNoDatabase    = errors.New("clickhouse: database name is required")
	ErrTLSCARequired = errors.New("clickhouse: TLS CA file is required when TLS is enabled without skip verify")

	// Connection errors
	ErrConnectionFailed = errors.New("clickhouse: failed to connect")
	ErrPingFailed       = errors.New("clickhouse: ping failed")

	// Migration errors
	ErrMigrationFailed = errors.New("clickhouse: migration failed")

	// Query errors
	ErrQueryFailed   = errors.New("clickhouse: query failed")
	ErrNoRows        = errors.New("clickhouse: no rows returned")
	ErrScanFailed    = errors.New("clickhouse: scan failed")
	ErrInvalidMetric = errors.New("clickhouse: invalid metric name format")
)



