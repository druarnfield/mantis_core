// Package main provides the CLI entry point for the database worker.
//
// The worker processes database metadata and query requests over stdio,
// using NDJSON format for communication.
//
// Usage:
//
//	worker [flags]
//
// Flags:
//
//	-help    Show help message
//	-version Show version information
//
// The worker reads requests from stdin and writes responses to stdout.
// Each request/response is a JSON object on a single line (NDJSON format).
//
// Example request:
//
//	{"id":"req-001","method":"metadata.list_schemas","params":{"driver":"duckdb","connection_string":":memory:"}}
//
// Example response:
//
//	{"id":"req-001","success":true,"result":{"schemas":[{"name":"main","is_default":true}]}}
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	// Register database drivers
	_ "github.com/mantis/worker/internal/driver/duckdb"
	_ "github.com/mantis/worker/internal/driver/mssql"

	"github.com/mantis/worker/internal/handler"
	"github.com/mantis/worker/internal/pool"
	"github.com/mantis/worker/internal/transport"
)

var (
	// Version is set at build time
	Version = "dev"
)

// poolConfig holds the pool configuration from command-line flags.
var poolConfig struct {
	enabled         bool
	maxIdleConns    int
	maxOpenConns    int
	connMaxLifetime time.Duration
	connMaxIdleTime time.Duration
}

func main() {
	// Parse flags
	showHelp := flag.Bool("help", false, "Show help message")
	showVersion := flag.Bool("version", false, "Show version information")

	// Pool configuration flags
	flag.BoolVar(&poolConfig.enabled, "pool", true, "Enable connection pooling")
	flag.IntVar(&poolConfig.maxIdleConns, "pool-max-idle", 5, "Maximum idle connections per pool")
	flag.IntVar(&poolConfig.maxOpenConns, "pool-max-open", 10, "Maximum open connections per pool")
	flag.DurationVar(&poolConfig.connMaxLifetime, "pool-conn-lifetime", 5*time.Minute, "Maximum connection lifetime")
	flag.DurationVar(&poolConfig.connMaxIdleTime, "pool-conn-idle", 1*time.Minute, "Maximum connection idle time")

	flag.Parse()

	if *showHelp {
		printHelp()
		os.Exit(0)
	}

	if *showVersion {
		fmt.Printf("worker version %s\n", Version)
		os.Exit(0)
	}

	// Run the worker
	if err := run(); err != nil {
		if err != io.EOF {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	}
}

func run() error {
	// Set up context with cancellation on SIGINT/SIGTERM
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	// Create transport (stdio)
	trans := transport.NewStdioTransport(os.Stdin, os.Stdout)

	// Create handler with default registry and optional pooling
	var h *handler.Handler
	var poolMgr *pool.Manager

	if poolConfig.enabled {
		cfg := pool.Config{
			MaxIdleConns:    poolConfig.maxIdleConns,
			MaxOpenConns:    poolConfig.maxOpenConns,
			ConnMaxLifetime: poolConfig.connMaxLifetime,
			ConnMaxIdleTime: poolConfig.connMaxIdleTime,
		}
		poolMgr = pool.NewManager(cfg)
		defer poolMgr.Close()
		h = handler.NewWithDefaultRegistryAndPool(poolMgr)
	} else {
		h = handler.NewWithDefaultRegistry()
	}

	// Run the serve loop
	return transport.Serve(ctx, trans, h)
}

func printHelp() {
	fmt.Println(`Database Worker - Metadata and Query Execution Service

USAGE:
    worker [FLAGS]

FLAGS:
    -help                   Show this help message
    -version                Show version information
    -pool                   Enable connection pooling (default: true)
    -pool-max-idle N        Maximum idle connections per pool (default: 5)
    -pool-max-open N        Maximum open connections per pool (default: 10)
    -pool-conn-lifetime D   Maximum connection lifetime (default: 5m)
    -pool-conn-idle D       Maximum connection idle time (default: 1m)

DESCRIPTION:
    The worker processes database requests over stdio using NDJSON format.
    It supports metadata queries and SQL execution for multiple database types.

SUPPORTED DRIVERS:
    - duckdb    DuckDB (in-process OLAP database)
    - mssql     Microsoft SQL Server

REQUEST FORMAT:
    Each request is a JSON object on a single line:
    {"id":"<request-id>","method":"<category.operation>","params":{...}}

METHODS:
    Metadata Operations:
        metadata.list_schemas       List all schemas
        metadata.list_tables        List tables in a schema
        metadata.get_table          Get detailed table info
        metadata.get_columns        Get column information
        metadata.get_primary_key    Get primary key constraint
        metadata.get_foreign_keys   Get foreign key constraints
        metadata.get_unique_constraints  Get unique constraints
        metadata.get_indexes        Get index information
        metadata.get_row_count      Get table row count
        metadata.sample_rows        Sample rows from a table
        metadata.get_database_info  Get database information

    Query Operations:
        query.execute               Execute a SQL query

EXAMPLE:
    # List schemas in DuckDB
    echo '{"id":"1","method":"metadata.list_schemas","params":{"driver":"duckdb","connection_string":":memory:"}}' | worker

    # Execute a query
    echo '{"id":"2","method":"query.execute","params":{"driver":"duckdb","connection_string":":memory:","sql":"SELECT 1"}}' | worker`)
}
