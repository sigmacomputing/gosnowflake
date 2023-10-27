package gosnowflake

// ServerVersion is an interface that can be used to extract the server version
// from a snowflakeConn
type ServerVersion interface {
	// ServerVersion returns the snowflakeConn's server version.
	ServerVersion() string
}

// ServerVersion returns the snowflakeConn's server version.
func (sc *snowflakeConn) ServerVersion() string {
	return sc.serverVersion
}
