// Licensed under the MIT License. See LICENSE file in the project root for details.

package db

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
)

// Binary format specification:
// Header: [4 bytes magic] [4 bytes version] [8 bytes count] [8 bytes total_size]
// Magic: "LFGO" (0x4C46474F)
// Version: 1
// For each entry: [4 bytes key_len] [key_bytes] [4 bytes value_len] [value_bytes]

const (
	MagicNumber = 0x4C46474F // "LFGO" in little-endian
	Version     = 1
	HeaderSize  = 24 // 4 + 4 + 8 + 8
)

// ExportHeader represents the binary file header
type ExportHeader struct {
	Magic     uint32
	Version   uint32
	Count     uint64
	TotalSize uint64
}

// ExportBinary exports database data to a highly efficient binary format
func ExportBinary[K ~[]byte, V any](db DB[K, V], filename string) error {
	file, err := os.Create(filename) // #nosec G304
	if err != nil {
		return fmt.Errorf("failed to create file %s: %v", filename, err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	// Collect data first to calculate total size
	ctx := context.Background()
	snapshot := db.Snapshot(ctx)
	defer snapshot.Close(ctx)

	var entries []struct {
		key   K
		value V
	}
	var totalSize uint64

	snapshot.Iterate(ctx, func(key K, val V) bool {
		entries = append(entries, struct {
			key   K
			value V
		}{key, val})
		// Calculate value length based on type
		var valueLen int
		switch v := any(val).(type) {
		case string:
			valueLen = len(v)
		case []byte:
			valueLen = len(v)
		default:
			valueLen = len(fmt.Sprintf("%v", v))
		}
		totalSize += uint64(len(key) + valueLen + 8) // #nosec G115
		return true
	})

	// Write header
	header := ExportHeader{
		Magic:     MagicNumber,
		Version:   Version,
		Count:     uint64(len(entries)),
		TotalSize: totalSize,
	}

	if err := binary.Write(writer, binary.LittleEndian, header); err != nil {
		return fmt.Errorf("failed to write header: %v", err)
	}

	// Write entries
	for _, entry := range entries {
		keyLen := uint32(len(entry.key)) // #nosec G115

		// Convert value to bytes based on type
		var valueBytes []byte
		switch v := any(entry.value).(type) {
		case string:
			valueBytes = []byte(v)
		case []byte:
			valueBytes = v
		default:
			valueBytes = []byte(fmt.Sprintf("%v", v))
		}
		valueLen := uint32(len(valueBytes)) // #nosec G115

		if err := binary.Write(writer, binary.LittleEndian, keyLen); err != nil {
			return fmt.Errorf("failed to write key length: %v", err)
		}
		if _, err := writer.Write(entry.key); err != nil {
			return fmt.Errorf("failed to write key: %v", err)
		}
		if err := binary.Write(writer, binary.LittleEndian, valueLen); err != nil {
			return fmt.Errorf("failed to write value length: %v", err)
		}
		if _, err := writer.Write(valueBytes); err != nil {
			return fmt.Errorf("failed to write value: %v", err)
		}
	}

	return nil
}

// ImportBinary imports database data from the binary format
func ImportBinary[K ~[]byte, V any](db DB[K, V], filename string) error {
	file, err := os.Open(filename) // #nosec G304
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v", filename, err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	// Read header
	var header ExportHeader
	if err := binary.Read(reader, binary.LittleEndian, &header); err != nil {
		return fmt.Errorf("failed to read header: %v", err)
	}

	// Validate header
	if header.Magic != MagicNumber {
		return fmt.Errorf("invalid magic number: expected %x, got %x", MagicNumber, header.Magic)
	}
	if header.Version != Version {
		return fmt.Errorf("unsupported version: expected %d, got %d", Version, header.Version)
	}

	// Read entries
	for i := uint64(0); i < header.Count; i++ {
		// Read key
		var keyLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			return fmt.Errorf("failed to read key length: %v", err)
		}

		key := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, key); err != nil {
			return fmt.Errorf("failed to read key: %v", err)
		}

		// Read value
		var valueLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &valueLen); err != nil {
			return fmt.Errorf("failed to read value length: %v", err)
		}

		value := make([]byte, valueLen)
		if _, err := io.ReadFull(reader, value); err != nil {
			return fmt.Errorf("failed to read value: %v", err)
		}

		// Store in database - convert bytes to string for string values
		ctx := context.Background()
		db.Put(ctx, K(key), any(string(value)).(V))
	}

	return nil
}

// ExportCSV exports data to CSV format (good for human-readable exports)
func ExportCSV[K ~[]byte, V any](db DB[K, V], filename string) error {
	file, err := os.Create(filename) // #nosec G304
	if err != nil {
		return fmt.Errorf("failed to create file %s: %v", filename, err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	// Write CSV header
	if _, err := writer.WriteString("key,value\n"); err != nil {
		return fmt.Errorf("failed to write CSV header: %v", err)
	}

	// Write data
	ctx := context.Background()
	snapshot := db.Snapshot(ctx)
	defer snapshot.Close(ctx)

	count := 0
	snapshot.Iterate(ctx, func(key K, val V) bool {
		// Escape CSV values and ensure they're on single lines
		keyStr := escapeCSV(string(key))
		valStr := escapeCSV(fmt.Sprintf("%v", val))

		// Replace newlines with \n in the escaped strings
		keyStr = strings.ReplaceAll(keyStr, "\n", "\\n")
		valStr = strings.ReplaceAll(valStr, "\n", "\\n")

		line := fmt.Sprintf("%s,%s\n", keyStr, valStr)
		if _, err := writer.WriteString(line); err != nil {
			return false
		}
		count++
		return true
	})

	return nil
}

// ImportCSV imports data from CSV format
func ImportCSV[K ~[]byte, V any](db DB[K, V], filename string) error {
	file, err := os.Open(filename) // #nosec G304
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v", filename, err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	// Skip header
	_, err = reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read CSV header: %v", err)
	}

	// Read data
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read CSV line: %v", err)
		}

		// Remove trailing newline
		if len(line) > 0 && line[len(line)-1] == '\n' {
			line = line[:len(line)-1]
		}

		// Parse CSV line with proper handling of quoted values
		key, value, err := parseCSVLine(line)
		if err != nil {
			continue // Skip malformed lines
		}

		// Unescape values
		key = unescapeCSV(key)
		value = unescapeCSV(value)

		// Convert escaped newlines back to actual newlines
		key = strings.ReplaceAll(key, "\\n", "\n")
		value = strings.ReplaceAll(value, "\\n", "\n")

		// For now, assume string values in CSV import
		ctx := context.Background()
		db.Put(ctx, K([]byte(key)), any(value).(V))
	}

	return nil
}

// ExportJSON exports data to JSON format (for compatibility)
func ExportJSON[K ~[]byte, V any](db DB[K, V], filename string) error {
	file, err := os.Create(filename) // #nosec G304
	if err != nil {
		return fmt.Errorf("failed to create file %s: %v", filename, err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	// Write JSON header
	if _, err := writer.WriteString("{\n"); err != nil {
		return fmt.Errorf("failed to write JSON header: %v", err)
	}

	// Write data
	ctx := context.Background()
	snapshot := db.Snapshot(ctx)
	defer snapshot.Close(ctx)

	count := 0
	first := true
	snapshot.Iterate(ctx, func(key K, val V) bool {
		if !first {
			if _, err := writer.WriteString(",\n"); err != nil {
				return false
			}
		}
		first = false

		keyStr := fmt.Sprintf("%q", string(key))
		valStr := fmt.Sprintf("%q", fmt.Sprintf("%v", val))
		line := fmt.Sprintf("  %s: %s", keyStr, valStr)

		if _, err := writer.WriteString(line); err != nil {
			return false
		}
		count++
		return true
	})

	// Write JSON footer
	if _, err := writer.WriteString("\n}\n"); err != nil {
		return fmt.Errorf("failed to write JSON footer: %v", err)
	}

	return nil
}

// ImportJSON imports data from JSON format
func ImportJSON[K ~[]byte, V any](db DB[K, V], filename string) error {
	file, err := os.Open(filename) // #nosec G304
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v", filename, err)
	}
	defer file.Close()

	// For simplicity, we'll use a basic JSON parser
	// In production, use encoding/json for proper JSON parsing
	reader := bufio.NewReader(file)

	// Skip opening brace
	if _, err := reader.ReadByte(); err != nil {
		return fmt.Errorf("failed to read opening brace: %v", err)
	}

	for {
		// Skip whitespace
		for {
			b, err := reader.ReadByte()
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return fmt.Errorf("failed to read JSON: %v", err)
			}
			if b != ' ' && b != '\n' && b != '\r' && b != '\t' {
				if err := reader.UnreadByte(); err != nil {
					return fmt.Errorf("failed to unread byte: %v", err)
				}
				break
			}
		}

		// Check for closing brace
		b, err := reader.ReadByte()
		if err != nil {
			return fmt.Errorf("failed to read JSON: %v", err)
		}
		if b == '}' {
			break
		}
		if err := reader.UnreadByte(); err != nil {
			return fmt.Errorf("failed to unread byte: %v", err)
		}

		// Read key
		key, err := readJSONString(reader)
		if err != nil {
			return fmt.Errorf("failed to read JSON key: %v", err)
		}

		// Skip colon and whitespace
		for {
			b, err := reader.ReadByte()
			if err != nil {
				return fmt.Errorf("failed to read JSON: %v", err)
			}
			if b == ':' {
				break
			}
		}

		// Skip whitespace
		for {
			b, err := reader.ReadByte()
			if err != nil {
				return fmt.Errorf("failed to read JSON: %v", err)
			}
			if b != ' ' && b != '\n' && b != '\r' && b != '\t' {
				if err := reader.UnreadByte(); err != nil {
					return fmt.Errorf("failed to unread byte: %v", err)
				}
				break
			}
		}

		// Read value
		value, err := readJSONString(reader)
		if err != nil {
			return fmt.Errorf("failed to read JSON value: %v", err)
		}

		// Store in database
		ctx := context.Background()
		db.Put(ctx, K([]byte(key)), any(value).(V))

		// Skip comma and whitespace
		for {
			b, err := reader.ReadByte()
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return fmt.Errorf("failed to read JSON: %v", err)
			}
			if b == ',' {
				break
			}
			if b == '}' {
				return nil
			}
			if b != ' ' && b != '\n' && b != '\r' && b != '\t' {
				if err := reader.UnreadByte(); err != nil {
					return fmt.Errorf("failed to unread byte: %v", err)
				}
				break
			}
		}
	}

	return nil
}

// Helper function to parse a CSV line with proper quoted value handling
func parseCSVLine(line string) (string, string, error) {
	if len(line) == 0 {
		return "", "", fmt.Errorf("empty line")
	}

	var key, value string
	var inQuotes bool
	var currentField strings.Builder
	var fieldIndex int

	for i := 0; i < len(line); i++ {
		char := line[i]

		if char == '"' {
			if inQuotes {
				// Check for escaped quote
				if i+1 < len(line) && line[i+1] == '"' {
					currentField.WriteByte('"')
					i++ // Skip the next quote
				} else {
					inQuotes = false
				}
			} else {
				inQuotes = true
			}
		} else if char == ',' && !inQuotes {
			// End of field
			if fieldIndex == 0 {
				key = currentField.String()
				currentField.Reset()
				fieldIndex = 1
			} else {
				// We shouldn't get here for a key-value CSV
				return "", "", fmt.Errorf("too many fields")
			}
		} else {
			currentField.WriteByte(char)
		}
	}

	// Get the last field (value)
	if fieldIndex == 0 {
		// Only one field, treat as key with empty value
		key = currentField.String()
		value = ""
	} else {
		value = currentField.String()
	}

	// Handle the case where we have a trailing comma (empty value)
	if len(line) > 0 && line[len(line)-1] == ',' {
		if fieldIndex == 0 {
			key = currentField.String()
			value = ""
		}
	}

	if inQuotes {
		return "", "", fmt.Errorf("unclosed quotes")
	}

	return key, value, nil
}

// Helper functions for CSV escaping
func escapeCSV(s string) string {
	if len(s) == 0 {
		return `""`
	}

	needsQuotes := false
	for _, char := range s {
		if char == '"' || char == ',' || char == '\n' || char == '\r' {
			needsQuotes = true
			break
		}
	}

	if !needsQuotes {
		return s
	}

	result := `"`
	for _, char := range s {
		if char == '"' {
			result += `""`
		} else {
			result += string(char)
		}
	}
	result += `"`
	return result
}

func unescapeCSV(s string) string {
	if len(s) == 0 {
		return s
	}

	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
		result := ""
		for i := 0; i < len(s); i++ {
			if i+1 < len(s) && s[i] == '"' && s[i+1] == '"' {
				result += `"`
				i++
			} else {
				result += string(s[i])
			}
		}
		return result
	}

	return s
}

// Helper function for reading JSON strings
func readJSONString(reader *bufio.Reader) (string, error) {
	// Expect opening quote
	b, err := reader.ReadByte()
	if err != nil {
		return "", err
	}
	if b != '"' {
		return "", fmt.Errorf("expected opening quote, got %c", b)
	}

	var result []byte
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return "", err
		}
		if b == '"' {
			break
		}
		if b == '\\' {
			// Handle escape sequences
			next, err := reader.ReadByte()
			if err != nil {
				return "", err
			}
			switch next {
			case '"', '\\', '/':
				result = append(result, next)
			case 'b':
				result = append(result, '\b')
			case 'f':
				result = append(result, '\f')
			case 'n':
				result = append(result, '\n')
			case 'r':
				result = append(result, '\r')
			case 't':
				result = append(result, '\t')
			default:
				result = append(result, next)
			}
		} else {
			result = append(result, b)
		}
	}

	return string(result), nil
}
