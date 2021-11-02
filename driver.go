// Copyright (c) 2017-2021 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"os"
)

// SnowflakeDriver is a context of Go Driver
type SnowflakeDriver struct{}

// Open creates a new connection.
func (d SnowflakeDriver) Open(dsn string) (driver.Conn, error) {
	logger.Info("Open")
	ctx := context.TODO()
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return d.OpenWithConfig(ctx, *cfg)
}

// OpenWithConfig creates a new connection with the given Config.
func (d SnowflakeDriver) OpenWithConfig(
	ctx context.Context,
	config Config) (
	driver.Conn, error) {
	logger.Info("OpenWithConfig")
	sc, err := buildSnowflakeConn(ctx, config)
	if err != nil {
		return nil, err
	}

	if err = authenticateWithConfig(sc); err != nil {
		return nil, err
	}
	sc.connectionTelemetry(&config)

	sc.startHeartBeat()
	sc.internal = &httpClient{sr: sc.rest}
	return sc, nil
}

func runningOnGithubAction() bool {
	return os.Getenv("GITHUB_ACTIONS") != ""
}

var logger = CreateDefaultLogger()

const logLevelEnvFlag = "GSNOWFLAKE_LOG_LEVEL"

func init() {
	sql.Register("snowflake", &SnowflakeDriver{})
	logLevel := "error"
	if os.Getenv(logLevelEnvFlag) != "" {
		logLevel = os.Getenv(logLevelEnvFlag)
	}
	logger.SetLogLevel(logLevel)
	if runningOnGithubAction() {
		logger.SetLogLevel("fatal")
	}
}
