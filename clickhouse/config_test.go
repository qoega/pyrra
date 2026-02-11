package clickhouse

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	require.Equal(t, []string{"localhost:9000"}, config.Addresses)
	require.Equal(t, "pyrra", config.Database)
	require.Equal(t, "default", config.Username)
	require.Equal(t, "", config.Password)
	require.False(t, config.TLSEnabled)
	require.Equal(t, 10, config.MaxOpenConns)
	require.Equal(t, 5, config.MaxIdleConns)
	require.Equal(t, time.Hour, config.ConnMaxLifetime)
	require.Equal(t, 30*time.Second, config.DialTimeout)
	require.Equal(t, 60*time.Second, config.QueryTimeout)
	require.Equal(t, 30*time.Second, config.MVRefreshInterval)
}

func TestConfig_Validate(t *testing.T) {
	testcases := []struct {
		name    string
		config  Config
		wantErr error
	}{
		{
			name:    "valid default config",
			config:  DefaultConfig(),
			wantErr: nil,
		},
		{
			name: "no addresses",
			config: Config{
				Addresses: []string{},
				Database:  "pyrra",
			},
			wantErr: ErrNoAddresses,
		},
		{
			name: "empty database",
			config: Config{
				Addresses: []string{"localhost:9000"},
				Database:  "",
			},
			wantErr: ErrNoDatabase,
		},
		{
			name: "TLS enabled without CA file uses system pool",
			config: Config{
				Addresses:     []string{"localhost:9000"},
				Database:      "pyrra",
				TLSEnabled:    true,
				TLSCAFile:     "",
				TLSSkipVerify: false,
			},
			wantErr: nil, // system root CA pool is used when no CA file is specified
		},
		{
			name: "TLS enabled with skip verify",
			config: Config{
				Addresses:     []string{"localhost:9000"},
				Database:      "pyrra",
				TLSEnabled:    true,
				TLSCAFile:     "",
				TLSSkipVerify: true,
			},
			wantErr: nil,
		},
		{
			name: "TLS enabled with CA file",
			config: Config{
				Addresses:     []string{"localhost:9000"},
				Database:      "pyrra",
				TLSEnabled:    true,
				TLSCAFile:     "/path/to/ca.crt",
				TLSSkipVerify: false,
			},
			wantErr: nil,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.config.Validate()
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}



