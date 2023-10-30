package gosnowflake

// ServerMetadata is an interface that can be used to extract the server metadata
// from a snowflakeConn
type ServerMetadata interface {
	// Version returns the snowflakeConn's server version.
	Version() string
}

// Version returns the snowflakeConn's server version.
func (sc *snowflakeConn) Version() string {
	return sc.serverVersion
}
