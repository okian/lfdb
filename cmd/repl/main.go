// Licensed under the MIT License. See LICENSE file in the project root for details.

// Package main provides an interactive REPL (Read-Eval-Print Loop) for the lock-free database.
//
// This command-line tool allows users to interactively test and explore the database
// functionality through a simple command interface. It's useful for development,
// testing, and learning the database API.
//
// # Features
//
//   - Interactive command-line interface
//   - Basic CRUD operations (get, put, del)
//   - Simple command syntax
//   - Graceful shutdown handling
//   - Error reporting and usage help
//
// # Usage
//
// Start the REPL:
//
//	go run cmd/repl/main.go
//
// Available commands:
//
//	get <key>           - Retrieve a value by key
//	put <key> <value>   - Store a key-value pair
//	del <key>           - Delete a key-value pair
//	quit, exit          - Exit the REPL
//
// Example session:
//
//	> put user:1 "John Doe"
//	OK
//	> get user:1
//	Value: John Doe
//	> del user:1
//	Deleted
//	> get user:1
//	Key not found
//	> quit
//	Goodbye!
//
// # Dangers and Warnings
//
//   - **Data Persistence**: The REPL uses an in-memory database. All data is lost when the program exits.
//   - **Input Validation**: Limited input validation - malformed input may cause unexpected behavior.
//   - **Memory Usage**: Large values consume memory until the program exits.
//   - **Concurrent Access**: The REPL is single-threaded and not designed for concurrent access.
//   - **Key/Value Encoding**: Keys and values are treated as raw bytes. Special characters may cause issues.
//   - **No Transactions**: The REPL doesn't support transactions or complex operations.
//
// # Best Practices
//
//   - Use descriptive keys for better organization (e.g., "user:123", "config:timeout")
//   - Keep values reasonably sized for interactive use
//   - Use consistent key naming conventions
//   - Test operations thoroughly before using in production
//   - Monitor memory usage with large datasets
//   - Use appropriate key formats for your use case
//
// # Performance Considerations
//
//   - The REPL is designed for interactive use, not high-performance scenarios
//   - Each command creates a new context, which is fine for interactive use
//   - Large datasets will consume significant memory
//   - Command parsing overhead is minimal for interactive use
//
// # Thread Safety
//
// The REPL is single-threaded and not designed for concurrent access.
// For concurrent testing, use the database API directly in your application.
//
// # Error Handling
//
// The REPL provides basic error handling:
//   - Invalid commands show usage information
//   - Missing keys return "Key not found"
//   - Malformed input shows appropriate error messages
//
// # Development Use Cases
//
// The REPL is particularly useful for:
//   - Learning the database API
//   - Quick testing of operations
//   - Debugging data issues
//   - Prototyping data structures
//   - Demonstrating database functionality
//
// # Limitations
//
//   - No persistence (in-memory only)
//   - Limited command set
//   - No batch operations
//   - No transactions
//   - No TTL support
//   - No atomic operations
//   - No metrics or monitoring
//
// # See Also
//
// For full database functionality, use the core package directly.
// For performance testing, see the bench tool.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	core "github.com/kianostad/lfdb/internal/core"
)

type REPL struct {
	database core.DB[[]byte, []byte]
}

func NewREPL(database core.DB[[]byte, []byte]) *REPL {
	return &REPL{
		database: database,
	}
}

func (r *REPL) Run() {
	fmt.Println("Lock-Free Database REPL")
	fmt.Println("Commands: get <key>, put <key> <value>, del <key>, quit")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		cmd := parts[0]
		args := parts[1:]

		ctx := context.Background()

		switch cmd {
		case "get":
			if len(args) != 1 {
				fmt.Println("Usage: get <key>")
				continue
			}
			key := []byte(args[0])
			val, exists := r.database.Get(ctx, key)
			if exists {
				fmt.Printf("Value: %s\n", string(val))
			} else {
				fmt.Println("Key not found")
			}

		case "put":
			if len(args) != 2 {
				fmt.Println("Usage: put <key> <value>")
				continue
			}
			key := []byte(args[0])
			value := []byte(args[1])
			r.database.Put(ctx, key, value)
			fmt.Println("OK")

		case "del":
			if len(args) != 1 {
				fmt.Println("Usage: del <key>")
				continue
			}
			key := []byte(args[0])
			deleted := r.database.Delete(ctx, key)
			if deleted {
				fmt.Println("Deleted")
			} else {
				fmt.Println("Key not found")
			}

		case "quit", "exit":
			fmt.Println("Goodbye!")
			return

		default:
			fmt.Printf("Unknown command: %s\n", cmd)
		}
	}
}

func main() {
	_ = flag.Bool("quiet", false, "Run in quiet mode")
	flag.Parse()

	database := core.New[[]byte, []byte]()
	defer database.Close(context.Background())

	repl := NewREPL(database)

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nReceived shutdown signal. Closing database...")
		database.Close(context.Background())
		os.Exit(0)
	}()

	repl.Run()
}
