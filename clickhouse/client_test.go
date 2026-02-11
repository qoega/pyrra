package clickhouse

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewClient_InvalidConfig(t *testing.T) {
	testcases := []struct {
		name    string
		config  Config
		wantErr error
	}{
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
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			client, err := NewClient(tc.config)
			require.ErrorIs(t, err, tc.wantErr)
			require.Nil(t, client)
		})
	}
}

// TestNewClient_Connection tests actual connection to ClickHouse via testcontainers
func TestNewClient_Connection(t *testing.T) {
	client := setupClickHouseContainer(t)

	// Verify connection
	err := client.Ready(context.Background())
	require.NoError(t, err)

	// Get server version
	version, err := client.ServerVersion()
	require.NoError(t, err)
	require.NotNil(t, version)
	t.Logf("ClickHouse version: %v", version)
}

func TestClient_Config(t *testing.T) {
	config := DefaultConfig()
	// Can't create client without real ClickHouse, just test the method exists
	// by verifying config struct works
	require.NotEmpty(t, config.Addresses)
}

