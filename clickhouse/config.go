package clickhouse

import "time"

// Config holds ClickHouse connection configuration
type Config struct {
	// Connection
	Addresses []string `default:"localhost:9000"`
	Database  string   `default:"pyrra"`
	Username  string   `default:"default"`
	Password  string   `default:""`

	// Protocol: "native" (default, port 9000/9440) or "http" (port 8123/8443)
	Protocol string `default:"native"`

	// TLS
	TLSEnabled    bool   `default:"false"`
	TLSCAFile     string `default:""`
	TLSCertFile   string `default:""`
	TLSKeyFile    string `default:""`
	TLSSkipVerify bool   `default:"false"`

	// Connection Pool
	MaxOpenConns    int           `default:"10"`
	MaxIdleConns    int           `default:"5"`
	ConnMaxLifetime time.Duration `default:"1h"`

	// Query Settings
	DialTimeout  time.Duration `default:"10s"`
	QueryTimeout time.Duration `default:"30s"`

	// MV Refresh Settings
	MVRefreshInterval time.Duration `default:"30s"`
}

// DefaultConfig returns a Config with default values
func DefaultConfig() Config {
	return Config{
		Addresses:         []string{"localhost:9000"},
		Database:          "pyrra",
		Username:          "default",
		Password:          "",
		Protocol:          "native",
		TLSEnabled:        false,
		TLSSkipVerify:     false,
		MaxOpenConns:      10,
		MaxIdleConns:      5,
		ConnMaxLifetime:   time.Hour,
		DialTimeout:       30 * time.Second,
		QueryTimeout:      60 * time.Second,
		MVRefreshInterval: 30 * time.Second,
	}
}

// Validate checks if the configuration is valid
func (c Config) Validate() error {
	if len(c.Addresses) == 0 {
		return ErrNoAddresses
	}
	if c.Database == "" {
		return ErrNoDatabase
	}
	// Note: TLS without a CA file is valid -- the system root CA pool will be used.
	// This is the correct behavior for ClickHouse Cloud and other services with
	// publicly-signed certificates. A custom CA file is only needed for self-signed certs.
	return nil
}

