package pgxcache

import (
	"bytes"
	"context"
	"encoding"
	"encoding/gob"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/mitchellh/hashstructure/v2"
)

// QueryKey is a unique identifier for a query.
type QueryKey struct {
	// SQL is the SQL query.
	SQL string
	// Args are the arguments to the query.
	Args []any
}

// String returns a string representation of the query key.
func (x *QueryKey) String() string {
	fingerprint, err := hashstructure.Hash(*x, hashstructure.FormatV2, nil)
	if err != nil {
		panic(err)
	}

	return fmt.Sprintf("q%da%dh%s", len(x.SQL), len(x.Args), strconv.FormatUint(fingerprint, 10))
}

// QueryItem represents a query result.
type QueryItem struct {
	// CommandTag is the command tag returned by the query.
	CommandTag string
	// Fields is the field descriptions of the query result.
	Fields []pgconn.FieldDescription
	// Rows is the query result.
	Rows [][][]byte
}

var _ encoding.TextMarshaler = &QueryItem{}

// MarshalText implements encoding.TextMarshaler.
func (q *QueryItem) MarshalText() ([]byte, error) {
	buffer := &bytes.Buffer{}
	// encode the result
	if err := gob.NewEncoder(buffer).Encode(q); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

var _ encoding.TextUnmarshaler = &QueryItem{}

// UnmarshalText implements encoding.TextUnmarshaler.
func (q *QueryItem) UnmarshalText(data []byte) error {
	buffer := &bytes.Buffer{}
	buffer.Write(data)

	// encode the result
	return gob.NewDecoder(buffer).Decode(q)
}

// QueryCacher represents a backend cache that can be used by sqlcache package.
type QueryCacher interface {
	// Get must return a pointer to the item, a boolean representing whether
	// item is present or not, and an error (must be nil when key is not
	// present).
	Get(context.Context, *QueryKey) (*QueryItem, error)
	// Set sets the item into cache with the given TTL.
	Set(context.Context, *QueryKey, *QueryItem, time.Duration) error
	// Reset resets the cache
	Reset(context.Context) error
}

// QueryOptions represents the options that can be specified in a SQL query.
type QueryOptions struct {
	// MinRows is the minimum number of rows that the query should return.
	MinRows int
	// MaxRows is the maximum number of rows that the query should return.
	MaxRows int
	// MaxLifetime is the duration that the query result should be cached.
	MaxLifetime time.Duration
}

var patterns = []*regexp.Regexp{
	regexp.MustCompile(`(@cache-min-rows) (\d+)`),
	regexp.MustCompile(`(@cache-max-rows) (\d+)`),
	regexp.MustCompile(`(@cache-max-lifetime) (\d+[s|m|h|d])`),
}

// ParseQueryOptions parses query options from a SQL query.
func ParseQueryOptions(query string) (*QueryOptions, error) {
	var matches [][]string

	// prepare the matches
	for _, pattern := range patterns {
		// find the options
		item := pattern.FindAllStringSubmatch(query, 2)
		// if the item is empty
		if len(item) != 0 {
			// append the item to the matches
			matches = append(matches, item...)
		}
	}

	if len(matches) == 0 {
		return nil, fmt.Errorf("invalid query cache options")
	}

	options := &QueryOptions{}
	// iterate over the matches and set the options
	for _, item := range matches {
		// if the length of the item is not equal to 2, print MATCH and
		if len(item) < 3 {
			return nil, fmt.Errorf("invalid query cache options")
		}
		// set the options fields
		switch item[1] {
		case "@cache-ttl", "@cache-max-lifetime":
			value, err := time.ParseDuration(item[2])
			switch {
			case err != nil:
				return nil, fmt.Errorf("invalid @cache-ttl query option: %w", err)
			default:
				options.MaxLifetime = value
			}
		case "@cache-min-rows":
			value, err := strconv.Atoi(item[2])
			switch {
			case err != nil:
				return nil, fmt.Errorf("invalid @cache-min-rows query option: %w", err)
			default:
				options.MinRows = value
			}
		case "@cache-max-rows":
			value, err := strconv.Atoi(item[2])
			switch {
			case err != nil:
				return nil, fmt.Errorf("invalid @cache-max-rows query option: %w", err)
			default:
				options.MaxRows = value
			}
		}
	}

	return options, nil
}
