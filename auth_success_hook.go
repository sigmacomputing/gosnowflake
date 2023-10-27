package gosnowflake

import "context"

// SessionMetadata contains metadata obtained from a successful authentication
// response.
type SessionMetadata struct {
	ServerVersion string
}

// AuthSuccessHook is a user-provided callback that will be executed after a
// successful authentication response.
type AuthSuccessHook func(context.Context, SessionMetadata)

var authSuccessHook AuthSuccessHook

// RegisterAuthSuccessHook registers a hook that can be used to extract metadata
// from an auth response, such as the server version, for example, for logging
// purposes. This function is not thread-safe, and should only be called once,
// at startup.
func RegisterAuthSuccessHook(hook AuthSuccessHook) {
	authSuccessHook = hook
}

func logAuthSuccessMetadata(ctx context.Context, metadata SessionMetadata) {
	if authSuccessHook != nil {
		authSuccessHook(ctx, metadata)
	}
}
