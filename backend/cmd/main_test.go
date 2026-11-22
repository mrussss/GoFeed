package main

import (
	"context"
	"database/sql"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/db"

	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

func TestMySQLPoolSettings(t *testing.T) {
	dsn := os.Getenv("FEED_READ_TEST_DSN")
	if dsn == "" {
		t.Skip("set FEED_READ_TEST_DSN to an isolated MySQL schema named gofeed_read_test")
	}
	parsed, err := mysql.ParseDSN(dsn)
	if err != nil || parsed.Net != "tcp" || parsed.DBName != "gofeed_read_test" {
		t.Fatal("FEED_READ_TEST_DSN must target gofeed_read_test over TCP")
	}
	host, portText, err := net.SplitHostPort(parsed.Addr)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.DatabaseConfig{Host: host, Port: port, User: parsed.User, Password: parsed.Passwd, DBName: parsed.DBName}

	for _, tc := range []struct {
		name    string
		open    func(config.DatabaseConfig) (*gorm.DB, error)
		maxOpen int
		maxIdle int
	}{
		{"API", newAPIDB, 16, 8},
		{"Worker", db.NewDB, 0, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := tc.open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			pool, err := conn.DB()
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			if got := pool.Stats().MaxOpenConnections; got != tc.maxOpen {
				t.Fatalf("maximum open connections = %d, want %d", got, tc.maxOpen)
			}

			// Hold nine separate connections, then confirm how many the pool keeps.
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			var held [9]*sql.Conn
			for i := range held {
				held[i], err = pool.Conn(ctx)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = held[i].Close() })
			}
			for _, c := range held {
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if got := pool.Stats().Idle; got != tc.maxIdle {
				t.Fatalf("idle connections = %d, want %d", got, tc.maxIdle)
			}
		})
	}
}
