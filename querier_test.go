package pgxcache

import (
	"context"
	"errors"
	"io"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// ---------------------------------------------------------------------------
// Mock types
// ---------------------------------------------------------------------------

var _ pgx.Rows = &MockRows{}

type MockRows struct {
	CloseFn             func()
	ErrFn               func() error
	CommandTagFn        func() pgconn.CommandTag
	FieldDescriptionsFn func() []pgconn.FieldDescription
	NextFn              func() bool
	ScanFn              func(dest ...any) error
	ValuesFn            func() ([]any, error)
	RawValuesFn         func() [][]byte
	ConnFn              func() *pgx.Conn
}

func (m *MockRows) Close() {
	if m.CloseFn != nil {
		m.CloseFn()
	}
}

func (m *MockRows) Err() error {
	if m.ErrFn != nil {
		return m.ErrFn()
	}
	return nil
}

func (m *MockRows) CommandTag() pgconn.CommandTag {
	if m.CommandTagFn != nil {
		return m.CommandTagFn()
	}
	return pgconn.CommandTag{}
}

func (m *MockRows) FieldDescriptions() []pgconn.FieldDescription {
	if m.FieldDescriptionsFn != nil {
		return m.FieldDescriptionsFn()
	}
	return nil
}

func (m *MockRows) Next() bool {
	if m.NextFn != nil {
		return m.NextFn()
	}
	return false
}

func (m *MockRows) Scan(dest ...any) error {
	if m.ScanFn != nil {
		return m.ScanFn(dest...)
	}
	return nil
}

func (m *MockRows) Values() ([]any, error) {
	if m.ValuesFn != nil {
		return m.ValuesFn()
	}
	return nil, nil
}

func (m *MockRows) RawValues() [][]byte {
	if m.RawValuesFn != nil {
		return m.RawValuesFn()
	}
	return nil
}

func (m *MockRows) Conn() *pgx.Conn {
	if m.ConnFn != nil {
		return m.ConnFn()
	}
	return nil
}

var _ pgx.BatchResults = &MockBatchResults{}

type MockBatchResults struct {
	ExecFn     func() (pgconn.CommandTag, error)
	QueryFn    func() (pgx.Rows, error)
	QueryRowFn func() pgx.Row
	CloseFn    func() error
}

func (m *MockBatchResults) Exec() (pgconn.CommandTag, error) {
	if m.ExecFn != nil {
		return m.ExecFn()
	}
	return pgconn.CommandTag{}, nil
}

func (m *MockBatchResults) Query() (pgx.Rows, error) {
	if m.QueryFn != nil {
		return m.QueryFn()
	}
	return nil, nil
}

func (m *MockBatchResults) QueryRow() pgx.Row {
	if m.QueryRowFn != nil {
		return m.QueryRowFn()
	}
	return nil
}

func (m *MockBatchResults) Close() error {
	if m.CloseFn != nil {
		return m.CloseFn()
	}
	return nil
}

var _ QueryCacher = &MockCacher{}

type MockCacher struct {
	GetFn   func(ctx context.Context, key *QueryKey) (*QueryItem, error)
	SetFn   func(ctx context.Context, key *QueryKey, item *QueryItem, lifetime time.Duration) error
	ResetFn func(ctx context.Context) error
}

func (m *MockCacher) Get(ctx context.Context, key *QueryKey) (*QueryItem, error) {
	if m.GetFn != nil {
		return m.GetFn(ctx, key)
	}
	return nil, nil
}

func (m *MockCacher) Set(ctx context.Context, key *QueryKey, item *QueryItem, lifetime time.Duration) error {
	if m.SetFn != nil {
		return m.SetFn(ctx, key, item, lifetime)
	}
	return nil
}

func (m *MockCacher) Reset(ctx context.Context) error {
	if m.ResetFn != nil {
		return m.ResetFn(ctx)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Unit test suites
// ---------------------------------------------------------------------------

var _ = Describe("QueryKey", func() {
	Describe("String()", func() {
		It("is consistent for the same key", func() {
			key := &QueryKey{SQL: "SELECT 1", Args: []any{42}}
			Expect(key.String()).To(Equal(key.String()))
		})

		It("differs for different SQL", func() {
			key1 := &QueryKey{SQL: "SELECT 1"}
			key2 := &QueryKey{SQL: "SELECT 2"}
			Expect(key1.String()).NotTo(Equal(key2.String()))
		})

		It("differs for different args", func() {
			key1 := &QueryKey{SQL: "SELECT $1", Args: []any{1}}
			key2 := &QueryKey{SQL: "SELECT $1", Args: []any{2}}
			Expect(key1.String()).NotTo(Equal(key2.String()))
		})
	})
})

var _ = Describe("ParseQueryOptions", func() {
	It("returns nil, nil when no annotations are present", func() {
		opts, err := ParseQueryOptions("SELECT 1")
		Expect(err).To(BeNil())
		Expect(opts).To(BeNil())
	})

	It("parses @cache-max-lifetime with seconds", func() {
		opts, err := ParseQueryOptions("SELECT 1 -- @cache-max-lifetime 30s")
		Expect(err).To(BeNil())
		Expect(opts).NotTo(BeNil())
		Expect(opts.MaxLifetime).To(Equal(30 * time.Second))
	})

	It("parses @cache-max-lifetime with minutes", func() {
		opts, err := ParseQueryOptions("SELECT 1 -- @cache-max-lifetime 5m")
		Expect(err).To(BeNil())
		Expect(opts.MaxLifetime).To(Equal(5 * time.Minute))
	})

	It("parses @cache-max-lifetime with hours", func() {
		opts, err := ParseQueryOptions("SELECT 1 -- @cache-max-lifetime 2h")
		Expect(err).To(BeNil())
		Expect(opts.MaxLifetime).To(Equal(2 * time.Hour))
	})

	It("parses @cache-min-rows", func() {
		opts, err := ParseQueryOptions("-- @cache-min-rows 5\nSELECT 1")
		Expect(err).To(BeNil())
		Expect(opts).NotTo(BeNil())
		Expect(opts.MinRows).To(Equal(5))
	})

	It("parses @cache-max-rows", func() {
		opts, err := ParseQueryOptions("-- @cache-max-rows 100\nSELECT 1")
		Expect(err).To(BeNil())
		Expect(opts).NotTo(BeNil())
		Expect(opts.MaxRows).To(Equal(100))
	})

	It("parses combined directives", func() {
		query := "-- @cache-max-lifetime 1m @cache-min-rows 2 @cache-max-rows 50\nSELECT 1"
		opts, err := ParseQueryOptions(query)
		Expect(err).To(BeNil())
		Expect(opts).NotTo(BeNil())
		Expect(opts.MaxLifetime).To(Equal(time.Minute))
		Expect(opts.MinRows).To(Equal(2))
		Expect(opts.MaxRows).To(Equal(50))
	})

	It("returns error for invalid lifetime value", func() {
		// "30d" matches the regex [smhd] but time.ParseDuration rejects "d"
		_, err := ParseQueryOptions("SELECT 1 -- @cache-max-lifetime 30d")
		Expect(err).NotTo(BeNil())
	})
})

var _ = Describe("QueryItem", func() {
	It("roundtrips via MarshalText/UnmarshalText", func() {
		original := &QueryItem{
			CommandTag: "SELECT 2",
			Fields: []pgconn.FieldDescription{
				{Name: "id", DataTypeOID: 23},
				{Name: "name", DataTypeOID: 25},
			},
			Rows: [][][]byte{
				{[]byte("1"), []byte("alice")},
				{[]byte("2"), []byte("bob")},
			},
		}

		data, err := original.MarshalText()
		Expect(err).To(BeNil())
		Expect(data).NotTo(BeEmpty())

		decoded := &QueryItem{}
		Expect(decoded.UnmarshalText(data)).To(Succeed())
		Expect(decoded.CommandTag).To(Equal(original.CommandTag))
		Expect(decoded.Fields).To(HaveLen(2))
		Expect(decoded.Rows).To(HaveLen(2))
		Expect(decoded.Rows[0][0]).To(Equal(original.Rows[0][0]))
		Expect(decoded.Rows[1][1]).To(Equal(original.Rows[1][1]))
	})
})

var _ = Describe("MemoryQueryCacher", func() {
	var (
		ctx    context.Context
		cacher *MemoryQueryCacher
		key    *QueryKey
		item   *QueryItem
	)

	BeforeEach(func() {
		ctx = context.Background()
		cacher = NewMemoryQueryCacher()
		key = &QueryKey{SQL: "SELECT 1", Args: nil}
		item = &QueryItem{
			CommandTag: "SELECT 1",
			Fields:     []pgconn.FieldDescription{{Name: "n", DataTypeOID: 25}},
			Rows:       [][][]byte{{[]byte("hello")}},
		}
	})

	It("returns nil for missing key", func() {
		result, err := cacher.Get(ctx, key)
		Expect(err).To(BeNil())
		Expect(result).To(BeNil())
	})

	It("returns item after Set and Wait", func() {
		Expect(cacher.Set(ctx, key, item, time.Minute)).To(Succeed())
		cacher.cache.Wait()
		result, err := cacher.Get(ctx, key)
		Expect(err).To(BeNil())
		Expect(result).NotTo(BeNil())
		Expect(result.CommandTag).To(Equal(item.CommandTag))
	})

	It("returns nil after Reset", func() {
		Expect(cacher.Set(ctx, key, item, time.Minute)).To(Succeed())
		cacher.cache.Wait()
		Expect(cacher.Reset(ctx)).To(Succeed())
		result, err := cacher.Get(ctx, key)
		Expect(err).To(BeNil())
		Expect(result).To(BeNil())
	})
})

var _ = Describe("Querier.set()", func() {
	var (
		ctx       context.Context
		querier   *Querier
		key       *QueryKey
		item      *QueryItem
		setCalled bool
	)

	BeforeEach(func() {
		ctx = context.Background()
		setCalled = false
		key = &QueryKey{SQL: "SELECT 1"}
		item = &QueryItem{
			Rows: [][][]byte{{[]byte("val")}},
		}
		querier = &Querier{
			Cacher: &MockCacher{
				SetFn: func(_ context.Context, _ *QueryKey, _ *QueryItem, _ time.Duration) error {
					setCalled = true
					return nil
				},
			},
		}
	})

	It("skips cache when MaxLifetime is 0", func() {
		opts := &QueryOptions{MaxLifetime: 0}
		Expect(querier.set(ctx, key, item, opts)).To(Succeed())
		Expect(setCalled).To(BeFalse())
	})

	It("skips cache when MinRows constraint is not met", func() {
		item.Rows = [][][]byte{} // 0 rows
		opts := &QueryOptions{MaxLifetime: time.Minute, MinRows: 5}
		Expect(querier.set(ctx, key, item, opts)).To(Succeed())
		Expect(setCalled).To(BeFalse())
	})

	It("skips cache when MaxRows constraint is exceeded", func() {
		item.Rows = [][][]byte{
			{[]byte("a")}, {[]byte("b")}, {[]byte("c")},
		}
		opts := &QueryOptions{MaxLifetime: time.Minute, MaxRows: 2}
		Expect(querier.set(ctx, key, item, opts)).To(Succeed())
		Expect(setCalled).To(BeFalse())
	})

	It("calls Cacher.Set on happy path", func() {
		opts := &QueryOptions{MaxLifetime: time.Minute}
		Expect(querier.set(ctx, key, item, opts)).To(Succeed())
		Expect(setCalled).To(BeTrue())
	})

	It("calls Cacher.Set when row count satisfies both constraints", func() {
		item.Rows = [][][]byte{
			{[]byte("a")}, {[]byte("b")}, {[]byte("c")},
		}
		opts := &QueryOptions{MaxLifetime: time.Minute, MinRows: 2, MaxRows: 5}
		Expect(querier.set(ctx, key, item, opts)).To(Succeed())
		Expect(setCalled).To(BeTrue())
	})
})

var _ = Describe("Row", func() {
	Describe("Scan()", func() {
		It("scans values from pre-populated item", func() {
			item := &QueryItem{
				Fields: []pgconn.FieldDescription{
					{Name: "name", DataTypeOID: 25, Format: 0},
				},
				Rows: [][][]byte{
					{[]byte("hello")},
				},
			}
			row := &Row{item: item, registry: pgtype.NewMap()}
			var name string
			Expect(row.Scan(&name)).To(Succeed())
			Expect(name).To(Equal("hello"))
		})

		It("returns io.EOF when item has no rows", func() {
			item := &QueryItem{Rows: [][][]byte{}}
			row := &Row{item: item, registry: pgtype.NewMap()}
			Expect(row.Scan()).To(Equal(io.EOF))
		})
	})
})

var _ = Describe("Rows", func() {
	var (
		item *QueryItem
		rows *Rows
	)

	BeforeEach(func() {
		item = &QueryItem{
			CommandTag: "SELECT 2",
			Fields: []pgconn.FieldDescription{
				{Name: "val", DataTypeOID: 25, Format: 0},
			},
			Rows: [][][]byte{
				{[]byte("a")},
				{[]byte("b")},
			},
		}
		rows = &Rows{item: item, index: -1, registry: pgtype.NewMap()}
	})

	It("Next advances through rows", func() {
		Expect(rows.Next()).To(BeTrue())
		Expect(rows.Next()).To(BeTrue())
		Expect(rows.Next()).To(BeFalse())
	})

	It("Scan returns values at current position", func() {
		rows.Next()
		var val string
		Expect(rows.Scan(&val)).To(Succeed())
		Expect(val).To(Equal("a"))
	})

	It("Values returns current row values", func() {
		rows.Next()
		values, err := rows.Values()
		Expect(err).To(BeNil())
		Expect(values).To(HaveLen(1))
	})

	It("RawValues returns raw bytes", func() {
		rows.Next()
		raw := rows.RawValues()
		Expect(raw).To(HaveLen(1))
		Expect(raw[0]).To(Equal([]byte("a")))
	})

	It("CommandTag returns item command tag", func() {
		Expect(rows.CommandTag().String()).To(Equal("SELECT 2"))
	})

	It("FieldDescriptions returns item fields", func() {
		Expect(rows.FieldDescriptions()).To(HaveLen(1))
		Expect(rows.FieldDescriptions()[0].Name).To(Equal("val"))
	})

	It("Close is a no-op", func() {
		Expect(func() { rows.Close() }).NotTo(Panic())
	})

	It("Err returns nil", func() {
		Expect(rows.Err()).To(BeNil())
	})
})

var _ = Describe("RowError", func() {
	Describe("Scan()", func() {
		It("returns the wrapped error", func() {
			sentinel := errors.New("test error")
			re := &RowError{err: sentinel}
			Expect(re.Scan()).To(Equal(sentinel))
		})
	})
})

var _ = Describe("RowsError", func() {
	var (
		re       *RowsError
		sentinel = errors.New("rows error")
	)

	BeforeEach(func() {
		re = &RowsError{err: sentinel}
	})

	It("Err returns the error", func() {
		Expect(re.Err()).To(Equal(sentinel))
	})

	It("Next returns false", func() {
		Expect(re.Next()).To(BeFalse())
	})

	It("Scan returns the error", func() {
		Expect(re.Scan()).To(Equal(sentinel))
	})

	It("Values returns error", func() {
		_, err := re.Values()
		Expect(err).To(Equal(sentinel))
	})

	It("RawValues returns nil", func() {
		Expect(re.RawValues()).To(BeNil())
	})

	It("FieldDescriptions returns nil", func() {
		Expect(re.FieldDescriptions()).To(BeNil())
	})

	It("CommandTag returns empty", func() {
		Expect(re.CommandTag()).To(Equal(pgconn.CommandTag{}))
	})

	It("Close does nothing", func() {
		Expect(func() { re.Close() }).NotTo(Panic())
	})
})

var _ = Describe("RowScanner", func() {
	Describe("Scan()", func() {
		It("delegates to pgx.ScanRow", func() {
			scanner := &RowScanner{
				registry: pgtype.NewMap(),
				row:      [][]byte{[]byte("hello")},
				fields: []pgconn.FieldDescription{
					{Name: "name", DataTypeOID: 25, Format: 0},
				},
			}
			var name string
			Expect(scanner.Scan(&name)).To(Succeed())
			Expect(name).To(Equal("hello"))
		})
	})
})

var _ = Describe("RowsRecorder", func() {
	It("records rows via Next and appends to item", func() {
		callCount := 0
		rawData := [][]byte{[]byte("row1")}

		mockRows := &MockRows{
			NextFn: func() bool {
				callCount++
				return callCount <= 2
			},
			RawValuesFn: func() [][]byte {
				return rawData
			},
			ErrFn: func() error { return nil },
			CommandTagFn: func() pgconn.CommandTag {
				return pgconn.NewCommandTag("SELECT 2")
			},
			FieldDescriptionsFn: func() []pgconn.FieldDescription {
				return []pgconn.FieldDescription{{Name: "v", DataTypeOID: 25}}
			},
			CloseFn: func() {},
		}

		item := &QueryItem{}
		recorder := &RowsRecorder{
			rows:  mockRows,
			item:  item,
			cache: func(i *QueryItem) error { return nil },
		}

		Expect(recorder.Next()).To(BeTrue())
		Expect(recorder.Next()).To(BeTrue())
		Expect(recorder.Next()).To(BeFalse())
		Expect(item.Rows).To(HaveLen(2))
	})

	It("caches item on Close when no error", func() {
		var cached *QueryItem
		fields := []pgconn.FieldDescription{{Name: "v", DataTypeOID: 25}}
		cmdTag := pgconn.NewCommandTag("SELECT 1")

		mockRows := &MockRows{
			ErrFn:               func() error { return nil },
			CommandTagFn:        func() pgconn.CommandTag { return cmdTag },
			FieldDescriptionsFn: func() []pgconn.FieldDescription { return fields },
			CloseFn:             func() {},
		}

		recorder := &RowsRecorder{
			rows: mockRows,
			item: &QueryItem{},
			cache: func(i *QueryItem) error {
				cached = i
				return nil
			},
		}
		recorder.Close()

		Expect(cached).NotTo(BeNil())
		Expect(cached.CommandTag).To(Equal("SELECT 1"))
		Expect(cached.Fields).To(HaveLen(1))
	})

	It("propagates Err from underlying rows", func() {
		sentinel := errors.New("underlying error")
		mockRows := &MockRows{
			ErrFn: func() error { return sentinel },
		}
		recorder := &RowsRecorder{
			rows:  mockRows,
			item:  &QueryItem{},
			cache: func(i *QueryItem) error { return nil },
		}
		Expect(recorder.Err()).To(Equal(sentinel))
	})
})

var _ = Describe("RowRecorder", func() {
	Describe("Scan()", func() {
		It("records a row and calls cache func", func() {
			var cached *QueryItem
			fields := []pgconn.FieldDescription{{Name: "n", DataTypeOID: 25}}
			callCount := 0

			mockRows := &MockRows{
				ErrFn: func() error { return nil },
				NextFn: func() bool {
					callCount++
					return callCount == 1
				},
				RawValuesFn: func() [][]byte {
					return [][]byte{[]byte("hello")}
				},
				CommandTagFn: func() pgconn.CommandTag {
					return pgconn.NewCommandTag("SELECT 1")
				},
				FieldDescriptionsFn: func() []pgconn.FieldDescription { return fields },
				ScanFn:              func(dest ...any) error { return nil },
				CloseFn:             func() {},
			}

			result := &QueryItem{}
			recorder := &RowRecorder{
				rows:   mockRows,
				result: result,
				cache: func(i *QueryItem) error {
					cached = i
					return nil
				},
			}

			var dest string
			Expect(recorder.Scan(&dest)).To(Succeed())
			Expect(cached).NotTo(BeNil())
			Expect(cached.Rows).To(HaveLen(1))
		})

		It("returns ErrNoRows when no rows", func() {
			mockRows := &MockRows{
				ErrFn:   func() error { return nil },
				NextFn:  func() bool { return false },
				CloseFn: func() {},
			}
			recorder := &RowRecorder{
				rows:   mockRows,
				result: &QueryItem{},
				cache:  func(i *QueryItem) error { return nil },
			}
			Expect(recorder.Scan()).To(Equal(pgx.ErrNoRows))
		})
	})
})

var _ = Describe("BatchQuerier", func() {
	var (
		bq    *BatchQuerier
		items []*QueryItem
	)

	BeforeEach(func() {
		items = []*QueryItem{
			{CommandTag: "SELECT 1", Rows: [][][]byte{{[]byte("a")}}},
			{CommandTag: "SELECT 1", Rows: [][][]byte{{[]byte("b")}}},
		}
		bq = &BatchQuerier{
			registry: pgtype.NewMap(),
			batch:    items,
		}
	})

	It("Query consumes items in order", func() {
		rows1, err := bq.Query()
		Expect(err).To(BeNil())
		Expect(rows1).NotTo(BeNil())

		rows2, err := bq.Query()
		Expect(err).To(BeNil())
		Expect(rows2).NotTo(BeNil())
	})

	It("Query returns io.EOF at end", func() {
		bq.Query()
		bq.Query()
		_, err := bq.Query()
		Expect(err).To(Equal(io.EOF))
	})

	It("Exec consumes items in order", func() {
		tag, err := bq.Exec()
		Expect(err).To(BeNil())
		Expect(tag.String()).To(Equal("SELECT 1"))
	})

	It("Exec returns io.EOF at end", func() {
		bq.Exec()
		bq.Exec()
		_, err := bq.Exec()
		Expect(err).To(Equal(io.EOF))
	})

	It("QueryRow consumes items in order", func() {
		row := bq.QueryRow()
		Expect(row).NotTo(BeNil())
	})

	It("QueryRow returns error row at end", func() {
		bq.QueryRow()
		bq.QueryRow()
		row := bq.QueryRow()
		Expect(row.Scan()).To(Equal(io.EOF))
	})
})

var _ = Describe("BatchRecorder", func() {
	newMockRows := func() *MockRows {
		return &MockRows{
			NextFn:  func() bool { return false },
			ErrFn:   func() error { return nil },
			CommandTagFn: func() pgconn.CommandTag {
				return pgconn.NewCommandTag("SELECT 0")
			},
			FieldDescriptionsFn: func() []pgconn.FieldDescription { return nil },
			CloseFn:             func() {},
		}
	}

	It("Query increments index and passes index to cache func", func() {
		mockBatch := &MockBatchResults{
			QueryFn: func() (pgx.Rows, error) {
				return newMockRows(), nil
			},
		}

		var cacheIndices []int
		recorder := &BatchRecorder{
			batch: mockBatch,
			cache: func(index int, item *QueryItem) error {
				cacheIndices = append(cacheIndices, index)
				return nil
			},
		}

		rows1, err := recorder.Query()
		Expect(err).To(BeNil())
		rows1.(*RowsRecorder).Close()

		rows2, err := recorder.Query()
		Expect(err).To(BeNil())
		rows2.(*RowsRecorder).Close()

		Expect(cacheIndices).To(Equal([]int{0, 1}))
		Expect(recorder.index).To(Equal(2))
	})

	It("Exec delegates and increments index", func() {
		mockBatch := &MockBatchResults{
			ExecFn: func() (pgconn.CommandTag, error) {
				return pgconn.NewCommandTag("UPDATE 1"), nil
			},
		}

		recorder := &BatchRecorder{
			batch: mockBatch,
			cache: func(int, *QueryItem) error { return nil },
		}

		tag, err := recorder.Exec()
		Expect(err).To(BeNil())
		Expect(tag.String()).To(Equal("UPDATE 1"))
		Expect(recorder.index).To(Equal(1))
	})

	It("QueryRow increments index and wraps in RowRecorder", func() {
		mockBatch := &MockBatchResults{
			QueryFn: func() (pgx.Rows, error) {
				return newMockRows(), nil
			},
		}

		recorder := &BatchRecorder{
			batch: mockBatch,
			cache: func(int, *QueryItem) error { return nil },
		}

		row := recorder.QueryRow()
		Expect(row).To(BeAssignableToTypeOf(&RowRecorder{}))
		Expect(recorder.index).To(Equal(1))
	})
})

// ---------------------------------------------------------------------------
// Integration test suite (requires PGX_DATABASE_URL)
// ---------------------------------------------------------------------------

var _ = Describe("Integration", Ordered, func() {
	var (
		ctx    context.Context
		pool   *pgxpool.Pool
		cacher *MemoryQueryCacher
		q      *Querier
	)

	BeforeAll(func() {
		dsn := os.Getenv("PGX_DATABASE_URL")
		if dsn == "" {
			Skip("PGX_DATABASE_URL not set")
		}

		ctx = context.Background()

		var err error
		pool, err = pgxpool.New(ctx, dsn)
		Expect(err).To(Succeed())

		cacher = NewMemoryQueryCacher()
		q = &Querier{
			Querier: pool,
			Cacher:  cacher,
			Options: &QueryOptions{
				MaxLifetime: time.Minute,
			},
		}
	})

	AfterAll(func() {
		if pool != nil {
			pool.Close()
		}
	})

	BeforeEach(func() {
		Expect(cacher.Reset(ctx)).To(Succeed())
	})

	Describe("Querier.Query", func() {
		const query = "-- @cache-max-lifetime 30s\nSELECT 1::text AS val"

		It("returns RowsRecorder on cache miss", func() {
			rows, err := q.Query(ctx, query)
			Expect(err).To(Succeed())
			Expect(rows).To(BeAssignableToTypeOf(&RowsRecorder{}))
			rows.Close()
		})

		It("returns cached Rows on second call", func() {
			rows, err := q.Query(ctx, query)
			Expect(err).To(Succeed())
			rows.Close()
			cacher.cache.Wait()

			rows2, err := q.Query(ctx, query)
			Expect(err).To(Succeed())
			Expect(rows2).To(BeAssignableToTypeOf(&Rows{}))
			rows2.Close()
		})

		It("Reset clears cache so next call hits DB again", func() {
			rows, err := q.Query(ctx, query)
			Expect(err).To(Succeed())
			rows.Close()
			cacher.cache.Wait()

			Expect(cacher.Reset(ctx)).To(Succeed())

			rows2, err := q.Query(ctx, query)
			Expect(err).To(Succeed())
			Expect(rows2).To(BeAssignableToTypeOf(&RowsRecorder{}))
			rows2.Close()
		})
	})

	Describe("Querier.QueryRow", func() {
		const query = "-- @cache-max-lifetime 30s\nSELECT 'cached'::text AS val"

		It("cache miss and hit cycle returns correct value", func() {
			var val string
			Expect(q.QueryRow(ctx, query).Scan(&val)).To(Succeed())
			Expect(val).To(Equal("cached"))
			cacher.cache.Wait()

			var val2 string
			Expect(q.QueryRow(ctx, query).Scan(&val2)).To(Succeed())
			Expect(val2).To(Equal("cached"))
		})
	})

	Describe("Querier.Exec", func() {
		It("executes and returns a non-empty command tag", func() {
			tag, err := q.Exec(ctx, "SELECT 1")
			Expect(err).To(Succeed())
			Expect(tag.String()).NotTo(BeEmpty())
		})
	})

	Describe("Querier.SendBatch", func() {
		It("all batch queries served from cache on second send", func() {
			batch := &pgx.Batch{}
			batch.Queue("-- @cache-max-lifetime 30s\nSELECT 1::text AS v")
			batch.Queue("-- @cache-max-lifetime 30s\nSELECT 2::text AS v")

			results := q.SendBatch(ctx, batch)
			rows1, err := results.Query()
			Expect(err).To(Succeed())
			rows1.Close()
			rows2, err := results.Query()
			Expect(err).To(Succeed())
			rows2.Close()
			Expect(results.Close()).To(Succeed())
			cacher.cache.Wait()

			results2 := q.SendBatch(ctx, batch)
			Expect(results2).To(BeAssignableToTypeOf(&BatchQuerier{}))
			Expect(results2.Close()).To(Succeed())
		})
	})

	Describe("Querier.Begin", func() {
		It("returns a *QuerierTx", func() {
			tx, err := q.Begin(ctx)
			Expect(err).To(Succeed())
			defer tx.Rollback(ctx)
			Expect(tx).To(BeAssignableToTypeOf(&QuerierTx{}))
		})

		It("transaction queries are cached via default options", func() {
			tx, err := q.Begin(ctx)
			Expect(err).To(Succeed())
			defer tx.Rollback(ctx)

			qtx := tx.(*QuerierTx)
			inner := qtx.Queryable()

			const query = "-- @cache-max-lifetime 30s\nSELECT 'tx'::text AS v"
			rows, err := inner.Query(ctx, query)
			Expect(err).To(Succeed())
			rows.Close()
		})
	})

	Describe("Query annotations", func() {
		It("@cache-min-rows skips caching when result is too small", func() {
			const query = "-- @cache-max-lifetime 30s @cache-min-rows 5\nSELECT 1 WHERE false"

			rows, err := q.Query(ctx, query)
			Expect(err).To(Succeed())
			rows.Close()
			cacher.cache.Wait()

			// Result has 0 rows < min 5, so it was not cached; second call hits DB
			rows2, err := q.Query(ctx, query)
			Expect(err).To(Succeed())
			Expect(rows2).To(BeAssignableToTypeOf(&RowsRecorder{}))
			rows2.Close()
		})

		It("@cache-max-rows skips caching when result is too large", func() {
			const query = "-- @cache-max-lifetime 30s @cache-max-rows 2\nSELECT generate_series(1,5)::text AS v"

			rows, err := q.Query(ctx, query)
			Expect(err).To(Succeed())
			for rows.Next() {
			}
			rows.Close()
			cacher.cache.Wait()

			// Result has 5 rows > max 2, so it was not cached; second call hits DB
			rows2, err := q.Query(ctx, query)
			Expect(err).To(Succeed())
			Expect(rows2).To(BeAssignableToTypeOf(&RowsRecorder{}))
			rows2.Close()
		})
	})
})
