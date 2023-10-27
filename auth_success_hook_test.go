package gosnowflake

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func postAuthSuccessWithServerVersion(_ context.Context, _ *snowflakeRestful, _ *http.Client, _ *url.Values, _ map[string]string, _ bodyCreatorType, _ time.Duration) (*authResponse, error) {
	return &authResponse{
		Success: true,
		Data: authResponseMain{
			Token:       "t",
			MasterToken: "m",
			SessionInfo: authResponseSessionInfo{
				DatabaseName: "dbn",
			},
			ServerVersion: "123.456.7",
		},
	}, nil
}

func TestUnitLogAuthSuccessMetadata(t *testing.T) {
	serverVersion := ""
	RegisterAuthSuccessHook(func(_ context.Context, metadata SessionMetadata) {
		serverVersion = metadata.ServerVersion
	})
	t.Cleanup(func() {
		RegisterAuthSuccessHook(nil)
	})

	sr := &snowflakeRestful{
		FuncPostAuth:  postAuthSuccessWithServerVersion,
		TokenAccessor: getSimpleTokenAccessor(),
	}
	sc := getDefaultSnowflakeConn()
	sc.ctx = context.TODO()
	sc.rest = sr
	sc.cfg.Authenticator = AuthTypeSnowflake

	if err := authenticateWithConfig(sc); err != nil {
		t.Fatalf("failed to run. err: %v", err)
	}

	if serverVersion != "123.456.7" {
		t.Fatalf("Expected server version 123.456.7, got %v", serverVersion)
	}
}
