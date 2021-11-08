package gosnowflake

import (
	"context"
	"os"
	"strconv"
)

// whether and how much to log, for a response-body.
// 0 => don't log anything
// -1 => no limit to logging
// a valid positive number => log response body bytes up to that number
const responseBodySampleSize = "GOSF_RESPONSE_SAMPLE_SIZE"

// whether to dump details for retries.
const verboseRetryLogging = "GOSF_VERBOSE_RETRY_LOGGING"

const ctxEnvFlagsKey = "go-sf-env-var-flags"

func ReadEnvIntFlag(flagName string) int64 {
	flagVal := os.Getenv(flagName)
	if flagVal == "" {
		return 0
	}
	intVal, err := strconv.ParseInt(flagVal, 10, 64)
	if err != nil {
		return 0
	} else {
		return intVal
	}
}

func ReadEnvBoolFlag(flagName string) bool {
	flagVal := os.Getenv(flagName)
	if flagVal == "" {
		return false
	}
	boolVal, err := strconv.ParseBool(flagVal)
	if err != nil {
		return false
	} else {
		return boolVal
	}
}

type EnvFlags map[string]interface{}

func AddEnvFlags(ctx context.Context) context.Context {
	envFlags := make(EnvFlags)
	envFlags[responseBodySampleSize] = ReadEnvIntFlag(responseBodySampleSize)
	envFlags[verboseRetryLogging] = ReadEnvBoolFlag(verboseRetryLogging)
	logger.WithContext(ctx).Infof("Debug: env-flags: %v\n", envFlags)
	return context.WithValue(ctx, ctxEnvFlagsKey, envFlags)
}

func GetEnvIntFlag(ctx context.Context, flagName string) (int64, bool) {
	envFlags, ok := ctx.Value(ctxEnvFlagsKey).(EnvFlags)
	if !ok {
		return 0, false
	}
	val, found := envFlags[flagName]
	if !found {
		return 0, false
	}
	i, ok := val.(int64)
	logger.WithContext(ctx).Infof("Debug: int flag %s evaluated to %v\n", flagName, i)
	return i, ok
}

func GetEnvBoolFlag(ctx context.Context, flagName string) (bool, bool) {
	envFlags, ok := ctx.Value(ctxEnvFlagsKey).(EnvFlags)
	if !ok {
		return false, false
	}
	val, found := envFlags[flagName]
	if !found {
		return false, false
	}
	b, ok := val.(bool)
	logger.WithContext(ctx).Infof("Debug: bool flag %s evaluated to %v\n", flagName, b)
	return b, ok
}
