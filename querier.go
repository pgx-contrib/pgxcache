package pgxcache

import (
	"context"
	"io"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// Queryable is an interface that wraps the pgx.Executor interface.
type Queryable interface {
	// Begin starts a pseudo nested transaction.
	Begin(ctx context.Context) (pgx.Tx, error)
	// Exec executes a query that doesn't return rows.
	Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error)
	// Query executes a query that returns rows, typically a SELECT.
	Query(ctx context.Context, query string, args ...any) (pgx.Rows, error)
	// QueryRow executes a query that is expected to return at most one row. QueryRow will always return a non-nil value.
	QueryRow(ctx context.Context, query string, args ...any) pgx.Row
	// SendBatch sends a batch of queries to the server. Use Batch.Send to create a batch. The returned Batch
	SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults
	// CopyFrom copies data from reader to table.
	CopyFrom(ctx context.Context, table pgx.Identifier, columns []string, source pgx.CopyFromSource) (int64, error)
}

var _ Queryable = &Querier{}

// Querier is a wrapper around pgx.Conn that caches prepared statements.
type Querier struct {
	// Options is the cache options.
	Options *QueryOptions
	// Querier is the query executor.
	Querier Queryable
	// Cacher is the cache store.
	Cacher QueryCacher
}

// Begin implements Queryable.
func (x *Querier) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := x.Querier.Begin(ctx)
	if err != nil {
		return nil, err
	}

	return &QuerierTx{
		Tx:      tx,
		Cacher:  x.Cacher,
		Options: x.Options,
	}, nil
}

// Close implements Queryable.
func (x *Querier) Close() {
	// Pool closes the connection.
	type Pool interface {
		// Close closes the connection.
		Close()
	}

	if pool, ok := x.Querier.(Pool); ok {
		pool.Close()
	}

	// Tx closes the connection.
	type Tx interface {
		// Conn returns the connection.
		Conn() *pgx.Conn
	}

	if tx, ok := x.Querier.(Tx); ok {
		tx.Conn().Close(context.Background())
	}
}

// CopyFrom implements Queryable.
func (x *Querier) CopyFrom(ctx context.Context, table pgx.Identifier, columns []string, source pgx.CopyFromSource) (int64, error) {
	return x.Querier.CopyFrom(ctx, table, columns, source)
}

// Exec is a helper that makes it easy to execute a query that doesn't return rows.
func (x *Querier) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	// get the cached item
	item, err := x.get(ctx, query, args)
	switch {
	case err != nil:
		return pgconn.CommandTag{}, err
	case item != nil:
		return pgconn.NewCommandTag(item.CommandTag), nil
	}

	return x.Querier.Exec(ctx, query, args...)
}

// Query is a helper that makes it easy to query for multiple rows.
func (x *Querier) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	// get the cached item
	item, err := x.get(ctx, query, args)
	switch {
	case err != nil:
		return nil, err
	case item != nil:
		return &Rows{item: item, index: -1, registry: pgtype.NewMap()}, nil
	}

	// get the rows
	rows, err := x.Querier.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	// parse the query options
	options := x.options(query)
	// we should not cache the item
	if options == nil {
		return rows, nil
	}

	recorder := &RowsRecorder{
		rows: rows,
		item: &QueryResult{},
		// cache the item after scanning
		cache: func(item *QueryResult) error {
			// set the cached item
			return x.set(ctx, query, args, item, options)
		},
	}

	// done!
	return recorder, nil
}

// QueryRow is a helper that makes it easy to query for a single row.
func (x *Querier) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	// get the cached item
	item, err := x.get(ctx, query, args)
	switch {
	case err != nil:
		return &RowError{err: err}
	case item != nil:
		return &Row{item: item, registry: pgtype.NewMap()}
	}

	// parse the query options
	options := x.options(query)
	// we should not cache the item
	if options == nil {
		return x.Querier.QueryRow(ctx, query, args...)
	}

	// get the row
	rows, err := x.Querier.Query(ctx, query, args...)
	if err != nil {
		return &RowError{err: err}
	}

	// return the row recorder
	return &RowRecorder{
		rows:   rows,
		result: &QueryResult{},
		// cache the item after scanning
		cache: func(item *QueryResult) error {
			// set the cached item
			return x.set(ctx, query, args, item, options)
		},
	}
}

// SendBatch is a helper that makes it easy to send a batch of queries.
func (x *Querier) SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults {
	refresh := func() pgx.BatchResults {
		return &BatchRecorder{
			batch: x.Querier.SendBatch(ctx, batch),
			cache: func(index int, item *QueryResult) error {
				// get the query
				query := batch.QueuedQueries[index]
				// parse the query options
				options := x.options(query.SQL)
				// we should not cache the item
				if options == nil {
					return nil
				}
				// set the cached item
				return x.set(ctx, query.SQL, query.Arguments, item, options)
			},
		}
	}

	querier := &BatchQuerier{registry: pgtype.NewMap()}
	// get the cached result
	for _, query := range batch.QueuedQueries {
		// get the cached item
		item, err := x.get(ctx, query.SQL, query.Arguments)
		switch {
		case err != nil:
			// we need to refresh the cache
			return refresh()
		case item != nil:
			// append the row
			querier.batch = append(querier.batch, item)
		default:
			return refresh()
		}
	}

	return querier
}

func (x *Querier) get(ctx context.Context, query string, args []any) (*QueryResult, error) {
	// prepare the query key
	key := &QueryKey{
		SQL:  query,
		Args: args,
	}

	// get the cached item
	item, err := x.Cacher.Get(ctx, key)

	// done!
	return item, err
}

func (x *Querier) set(ctx context.Context, query string, args []any, item *QueryResult, opts *QueryOptions) error {
	if opts.MaxLifetime == 0 {
		return nil
	}

	count := len(item.Rows)

	if count < opts.MinRows {
		if opts.MinRows > 0 {
			return nil
		}
	}

	if count > opts.MaxRows {
		if opts.MaxRows > 0 {
			return nil
		}
	}

	// prepare the query key
	key := &QueryKey{
		SQL:  query,
		Args: args,
	}

	// set the cached item
	return x.Cacher.Set(ctx, key, item, opts.MaxLifetime)
}

func (x *Querier) options(query string) *QueryOptions {
	// parse the query options
	options, err := ParseQueryOptions(query)
	if err != nil {
		// we should not cache the item
		return x.Options
	}

	return options
}

var _ pgx.Tx = &QuerierTx{}

// Querier is a wrapper around pgx.Conn that caches prepared statements.
type QuerierTx struct {
	// Querier is the query executor.
	Tx pgx.Tx
	// Options is the cache options.
	Options *QueryOptions
	// Cacher is the cache store.
	Cacher QueryCacher
}

// Begin implements pgx.Tx.
func (q *QuerierTx) Begin(ctx context.Context) (pgx.Tx, error) {
	return q.Queryable().Begin(ctx)
}

// Commit implements pgx.Tx.
func (q *QuerierTx) Commit(ctx context.Context) error {
	return q.Tx.Commit(ctx)
}

// Rollback implements pgx.Tx.
func (q *QuerierTx) Rollback(ctx context.Context) error {
	return q.Tx.Rollback(ctx)
}

// Conn implements pgx.Tx.
func (q *QuerierTx) Conn() *pgx.Conn {
	return q.Tx.Conn()
}

// CopyFrom implements pgx.Tx.
func (q *QuerierTx) CopyFrom(ctx context.Context, table pgx.Identifier, columns []string, source pgx.CopyFromSource) (int64, error) {
	return q.Queryable().CopyFrom(ctx, table, columns, source)
}

// Exec implements pgx.Tx.
func (q *QuerierTx) Exec(ctx context.Context, query string, args ...any) (commandTag pgconn.CommandTag, err error) {
	return q.Queryable().Exec(ctx, query, args...)
}

// LargeObjects implements pgx.Tx.
func (q *QuerierTx) LargeObjects() pgx.LargeObjects {
	return q.Tx.LargeObjects()
}

// Prepare implements pgx.Tx.
func (q *QuerierTx) Prepare(ctx context.Context, name string, query string) (*pgconn.StatementDescription, error) {
	return q.Tx.Prepare(ctx, name, query)
}

// Query implements pgx.Tx.
func (q *QuerierTx) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	return q.Queryable().Query(ctx, query, args...)
}

// QueryRow implements pgx.Tx.
func (q *QuerierTx) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	return q.Queryable().QueryRow(ctx, query, args...)
}

// SendBatch implements pgx.Tx.
func (q *QuerierTx) SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults {
	return q.Queryable().SendBatch(ctx, batch)
}

// Queryable returns the query executor.
func (q *QuerierTx) Queryable() Queryable {
	return &Querier{
		Querier: q.Tx,
		Cacher:  q.Cacher,
		Options: q.Options,
	}
}

var _ pgx.Row = &Row{}

// Row is a wrapper around pgx.Row.
type Row struct {
	item     *QueryResult
	registry *pgtype.Map
}

// Scan implements pgx.Row.
func (r *Row) Scan(values ...any) error {
	if len(r.item.Rows) <= 0 {
		return io.EOF
	}

	// prepare the scanner
	scanner := &RowScanner{
		row:      r.item.Rows[0],
		fields:   r.item.Fields,
		registry: r.registry,
	}

	// done!
	return scanner.Scan(values...)
}

var _ pgx.Row = &RowRecorder{}

// RowRecorder is a wrapper around pgx.Row that records the error.
type RowRecorder struct {
	rows   pgx.Rows
	result *QueryResult
	cache  func(*QueryResult) error
}

// Scan implements pgx.Row.
func (r *RowRecorder) Scan(values ...any) error {
	// close the rows
	defer r.rows.Close()

	if err := r.rows.Err(); err != nil {
		return err
	}

	if !r.rows.Next() {
		err := r.rows.Err()
		// check the error
		switch err {
		case nil:
			return pgx.ErrNoRows
		default:
			return err
		}
	}

	// cache the command tag and field descriptions
	r.result.CommandTag = r.rows.CommandTag().String()
	r.result.Fields = r.rows.FieldDescriptions()
	r.result.Rows = append(r.result.Rows, r.rows.RawValues())

	if err := r.rows.Scan(values...); err != nil {
		return err
	}

	// done!
	return r.cache(r.result)
}

var _ pgx.Row = &RowError{}

// RowError is a wrapper around pgx.Row that returns an error.
type RowError struct {
	err error
}

// Scan implements pgx.Row.
func (e RowError) Scan(values ...any) error {
	return e.err
}

var _ pgx.Row = &RowScanner{}

// RowScanner is a wrapper around pgx.Row that scans the values.
type RowScanner struct {
	registry *pgtype.Map
	row      [][]byte
	fields   []pgconn.FieldDescription
}

// Scan implements pgx.Row.
func (r *RowScanner) Scan(values ...any) error {
	// scan the row
	return pgx.ScanRow(r.registry, r.fields, r.row, values...)
}

var _ pgx.Rows = &Rows{}

// Rows is a wrapper around pgx.Rows that caches the rows.
type Rows struct {
	registry *pgtype.Map
	item     *QueryResult
	index    int
}

// Close implements pgx.Rows.
func (r *Rows) Close() {
	// no-op
}

// CommandTag implements pgx.Rows.
func (r *Rows) CommandTag() pgconn.CommandTag {
	return pgconn.NewCommandTag(r.item.CommandTag)
}

// Conn implements pgx.Rows.
func (r *Rows) Conn() *pgx.Conn {
	return nil
}

// Err implements pgx.Rows.
func (r *Rows) Err() error {
	return nil
}

// FieldDescriptions implements pgx.Rows.
func (r *Rows) FieldDescriptions() []pgconn.FieldDescription {
	return r.item.Fields
}

// Next implements pgx.Rows.
func (r *Rows) Next() bool {
	r.index++
	// done!
	return r.index < len(r.item.Rows)
}

// Scan implements pgx.Rows.
func (r *Rows) Scan(values ...any) error {
	if r.index < 0 {
		return nil
	}

	if r.index >= len(r.item.Rows) {
		return io.EOF
	}

	// prepare the row scanner
	scanner := &RowScanner{
		row:      r.item.Rows[r.index],
		fields:   r.item.Fields,
		registry: r.registry,
	}

	// done!
	return scanner.Scan(values...)
}

// Values implements pgx.Rows.
func (r *Rows) Values() ([]any, error) {
	if r.index < 0 {
		return []any{}, nil
	}

	if r.index >= len(r.item.Rows) {
		return nil, io.EOF
	}

	// prepare the row scanner
	scanner := &RowScanner{
		row:      r.item.Rows[r.index],
		fields:   r.item.Fields,
		registry: r.registry,
	}

	values := make([]any, len(r.item.Fields))
	// scan the values
	if err := scanner.Scan(values...); err != nil {
		return nil, err
	}

	return values, nil
}

// RawValues implements pgx.Rows.
func (r *Rows) RawValues() [][]byte {
	if r.index < 0 {
		return nil
	}

	if r.index >= len(r.item.Rows) {
		return nil
	}

	return r.item.Rows[r.index]
}

var _ pgx.Rows = &RowsRecorder{}

// RowsRecorder is a wrapper around pgx.Rows that records the rows.
type RowsRecorder struct {
	err   error
	rows  pgx.Rows
	item  *QueryResult
	cache func(*QueryResult) error
}

// Err implements pgx.Rows.
func (r *RowsRecorder) Err() error {
	// check the rows error
	if err := r.rows.Err(); err != nil {
		return err
	}

	return r.err
}

// Conn implements pgx.Rows.
func (r *RowsRecorder) Conn() *pgx.Conn {
	return r.rows.Conn()
}

// Close implements pgx.Rows.
func (r *RowsRecorder) Close() {
	// we should cache the item if there is no error
	if err := r.rows.Err(); err == nil {
		// cache the command tag and field descriptions
		r.item.CommandTag = r.rows.CommandTag().String()
		r.item.Fields = r.rows.FieldDescriptions()
		// cache the item
		if err := r.cache(r.item); err != nil {
			r.err = err
		}
	}

	// close the rows
	r.rows.Close()
}

// CommandTag implements pgx.Rows.
func (r *RowsRecorder) CommandTag() pgconn.CommandTag {
	return r.rows.CommandTag()
}

// FieldDescriptions implements pgx.Rows.
func (r *RowsRecorder) FieldDescriptions() []pgconn.FieldDescription {
	return r.rows.FieldDescriptions()
}

// Next implements pgx.Rows.
func (r *RowsRecorder) Next() bool {
	// move to the next row
	if r.rows.Next() {
		row := r.rows.RawValues()
		// add the row
		r.item.Rows = append(r.item.Rows, row)
		// done!
		return true
	}

	return false
}

// RawValues implements pgx.Rows.
func (r *RowsRecorder) RawValues() [][]byte {
	return r.rows.RawValues()
}

// Scan implements pgx.Rows.
func (r *RowsRecorder) Scan(values ...any) error {
	return r.rows.Scan(values...)
}

// Values implements pgx.Rows.
func (r *RowsRecorder) Values() ([]any, error) {
	return r.rows.Values()
}

var _ pgx.Rows = &RowsError{}

// RowsError is a wrapper around pgx.Rows that returns an error.
type RowsError struct {
	err error
}

// Close implements pgx.Rows.
func (r *RowsError) Close() {}

// CommandTag implements pgx.Rows.
func (r *RowsError) CommandTag() pgconn.CommandTag {
	return pgconn.CommandTag{}
}

// Conn implements pgx.Rows.
func (r *RowsError) Conn() *pgx.Conn {
	return nil
}

// Err implements pgx.Rows.
func (r *RowsError) Err() error {
	return r.err
}

// FieldDescriptions implements pgx.Rows.
func (r *RowsError) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

// Next implements pgx.Rows.
func (r *RowsError) Next() bool {
	return false
}

// RawValues implements pgx.Rows.
func (r *RowsError) RawValues() [][]byte {
	return nil
}

// Scan implements pgx.Rows.
func (r *RowsError) Scan(dest ...any) error {
	return r.err
}

// Values implements pgx.Rows.
func (r *RowsError) Values() ([]any, error) {
	return nil, r.err
}

var _ pgx.BatchResults = &BatchQuerier{}

// BatchQuerier is a wrapper around pgx.BatchResults that records the batch results.
type BatchQuerier struct {
	registry *pgtype.Map
	batch    []*QueryResult
	index    int
}

// Exec implements pgx.BatchResults.
func (b *BatchQuerier) Exec() (pgconn.CommandTag, error) {
	if b.index >= len(b.batch) {
		return pgconn.CommandTag{}, io.EOF
	}

	item := b.batch[b.index]
	// next
	b.index++
	// done!
	return pgconn.NewCommandTag(item.CommandTag), nil
}

// Query implements pgx.BatchResults.
func (b *BatchQuerier) Query() (pgx.Rows, error) {
	if b.index >= len(b.batch) {
		return nil, io.EOF
	}

	item := b.batch[b.index]
	// next
	b.index++
	// done!
	return &Rows{item: item, index: -1, registry: b.registry}, nil
}

// QueryRow implements pgx.BatchResults.
func (b *BatchQuerier) QueryRow() pgx.Row {
	if b.index >= len(b.batch) {
		return &RowError{err: io.EOF}
	}

	item := b.batch[b.index]
	// next
	b.index++
	// done!
	return &Row{item: item, registry: b.registry}
}

// Close implements pgx.BatchResults.
func (b *BatchQuerier) Close() error {
	return nil
}

var _ pgx.BatchResults = &BatchRecorder{}

// BatchRecorder is a wrapper around pgx.BatchResults that records the batch results.
type BatchRecorder struct {
	batch pgx.BatchResults
	cache func(int, *QueryResult) error
	index int
}

// Exec implements pgx.BatchResults.
func (b *BatchRecorder) Exec() (pgconn.CommandTag, error) {
	tag, err := b.batch.Exec()
	if err != nil {
		return tag, err
	}

	b.index++
	// done!
	return tag, nil
}

// Query implements pgx.BatchResults.
func (b *BatchRecorder) Query() (pgx.Rows, error) {
	rows, err := b.batch.Query()
	if err != nil {
		return nil, err
	}

	index := b.index
	// prepare record
	recorder := &RowsRecorder{
		rows: rows,
		item: &QueryResult{},
		// cache the item after scanning
		cache: func(item *QueryResult) error {
			return b.cache(index, item)
		},
	}

	b.index++
	// done!
	return recorder, nil
}

// QueryRow implements pgx.BatchResults.
func (b *BatchRecorder) QueryRow() pgx.Row {
	rows, err := b.batch.Query()
	if err != nil {
		return &RowError{err: err}
	}

	index := b.index
	// prepare record
	recorder := &RowRecorder{
		rows:   rows,
		result: &QueryResult{},
		// cache the item after scanning
		cache: func(item *QueryResult) error {
			return b.cache(index, item)
		},
	}

	b.index++
	// done
	return recorder
}

// Close implements pgx.BatchResults.
func (b *BatchRecorder) Close() error {
	return b.batch.Close()
}
