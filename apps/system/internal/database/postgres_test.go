package database

import "testing"

func TestPostgresConfDSNEscapesCredentialsAndPreservesTimeZone(t *testing.T) {
	databaseConf := PostgresConf{
		Host:     "2001:db8::1",
		Port:     5432,
		User:     "dogx user",
		Password: `p@ss:\'word`,
		Database: "dogx_dev",
		SSLMode:  "disable",
		TimeZone: "Asia/Shanghai",
	}

	want := `host='2001:db8::1' port=5432 user='dogx user' password='p@ss:\\\'word' dbname='dogx_dev' sslmode=disable TimeZone=Asia/Shanghai`
	if got := databaseConf.DSN(); got != want {
		t.Fatalf("unexpected DSN:\n got: %s\nwant: %s", got, want)
	}
}
