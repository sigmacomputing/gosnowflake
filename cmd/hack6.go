package main

import (
	"bytes"
    "fmt"
    "path"
    "io"
    "path/filepath"
    "os"
    "encoding/json"
    "strings"
    "log"
    "context"
    "time"
    "database/sql/driver"
    "sync"

    snowflake "github.com/sigmacomputing/gosnowflake"
)

type SnowflakeAdapter struct {

}

func init() {
    logger := snowflake.GetLogger()
    _ = logger.SetLogLevel("error")
}

const APPLICATION = "PizzaOracle"

func main() {
    c := loadTestConfig()

    // This is all the information we need to create a snowflake connections
    config := snowflake.Config{
        Account: strings.Replace(c.Server, ".snowflakecomputing.com", "", 1),
        Host: c.Host,
        Warehouse: c.Warehouse,
        Role: c.Role,
    
        Application: APPLICATION,
        InsecureMode: false,

        Params: map[string]*string{
            "timezone": StrP("UTC"),
            "abort_detached_query": StrP("true"),
        },

        ClientTimeout: 900 * time.Second,
        LoginTimeout: 30 * time.Second,
        RequestTimeout: 30 * time.Second,

        ValidateDefaultParameters: snowflake.ConfigBoolFalse,

        ConnectionID: "my-test-connection",

        MonitoringFetcher: snowflake.MonitoringFetcherConfig{
            QueryRuntimeThreshold: 2 * time.Minute,
            MaxDuration: 10 * time.Second,
            RetrySleepDuration: 250 * time.Millisecond,
        },
    }

    config.Authenticator = snowflake.AuthTypeSnowflake
    config.User = c.User
    config.Password = c.Password

    // Apparently calling the DSN() method will fill in some additional
    // information required to communicate with snowflake.
    snowflake.DSN(&config)

    d := snowflake.SnowflakeDriver{}

    conn, err := d.OpenWithConfig(context.TODO(), config)
    if err != nil {
        log.Fatal(err)
    }
    var wg sync.WaitGroup
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            var start = time.Now()
            stmt, err := conn.Prepare("SELECT 1")
            if err != nil {
                log.Fatal(err)
                return
            }
            stmt.Exec([]driver.Value{})
            if err != nil {
                log.Fatal(err)
            }
            fmt.Println("ping conn in", time.Since(start))
            fmt.Printf("%T\n", conn)
        }()
    }

    wg.Wait()
}

func StrP(s string) *string {
    return &s
}


type SnowflakeConfig struct {
    Server string
    Warehouse string
    Host string
    User string
    Role string
    Password string
}

func loadTestConfig() SnowflakeConfig {
	data, err := ReadDbSpec("snowflake")
	if err != nil {
		log.Fatal(err)
	}

	config := SnowflakeConfig{}
	if err := json.Unmarshal(data, &config); err != nil {
		log.Fatal(err)
	}

	return config
}

func ReadDbSpec(engine string) ([]byte, error) {
	basedir, found := os.LookupEnv("MPLEX_TEST_CONNECTION_DIR")

	if found {
		basedir, _ = filepath.Abs(basedir)
	} else {
        log.Fatal("MPLEX_TEST_CONNECTION_DIR must be set")
	}

	file, err := os.Open(path.Join(basedir, fmt.Sprintf("%s.json", engine)))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	buf := bytes.NewBuffer(nil)
	_, err = io.Copy(buf, file)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
