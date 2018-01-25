package instrumentedsql

import (
	"context"
	"database/sql/driver"

	"github.com/kr/pretty"
	"github.com/pkg/errors"
)

const (
	// LabelComponent ...
	LabelComponent = "component"
	// LabelQuery ...
	LabelQuery = "query"
	// LabelArgs ...
	LabelArgs = "args"
	// SQLTxBegin name for transaction begin
	SQLTxBegin = "sql-tx-begin"
	// SQLPrepare name for prepare statements
	SQLPrepare = "sql-prepare"
	// SQLConnExec name for exec statements
	SQLConnExec = "sql-conn-exec"
	// SQLPing name for pings
	SQLPing = "sql-ping"
	// SQLConnQuery name for query statements
	SQLConnQuery = "sql-conn-query"
	// SQLTxCommit name for transaction commits
	SQLTxCommit = "sql-tx-commit"
	// SQLTxRollback name for transaction rollbacks
	SQLTxRollback = "sql-tx-rollback"
	// SQLStmtClose name for statement closes
	SQLStmtClose = "sql-stmt-close"
	// SQLStmtExec name for statement exec
	SQLStmtExec = "sql-stmt-exec"
	// SQLStmtQuery name for statement query
	SQLStmtQuery = "sql-stmt-query"
	// SQLLastInsertID name for last inserted id
	SQLLastInsertID = "sql-res-lastInsertId"
	// SQLRowsAffected name for rows affected
	SQLRowsAffected = "sql-res-rowsAffected"
	// SQLRowsNext name for requesting the next set of rows
	SQLRowsNext = "sql-rows-next"
)

var (
	nilSpan = nullSpan{}
)

type opts struct {
	Logger
	Tracer
	OmitArgs           bool
	TraceWithoutParent bool
}

type wrappedDriver struct {
	opts
	parent driver.Driver
}

type wrappedConn struct {
	opts
	parent     driver.Conn
	parentSpan Span
	txSpan     Span
}

func newWrappedConn(opt opts, parent driver.Conn, parentSpan, txSpan Span) wrappedConn {
	return wrappedConn{opt, parent, parentSpan, txSpan}
}

func (c wrappedConn) closeSpans() {
	c.parentSpan.Finish()
	c.parentSpan = nil
	c.txSpan.Finish()
	c.txSpan = nil
}

type wrappedTx struct {
	opts
	ctx        context.Context
	parent     driver.Tx
	parentConn wrappedConn
	txSpan     Span
}

func newWrappedTx(ctx context.Context, opt opts, parent driver.Tx, parentConn wrappedConn, txSpan Span) wrappedTx {
	return wrappedTx{opt, ctx, parent, parentConn, txSpan}
}

func (t wrappedTx) closeSpans() {
	t.txSpan.Finish()
	t.txSpan = nil
	t.parentConn.txSpan.Finish()
	t.parentConn.txSpan = nil
}

type wrappedStmt struct {
	opts
	ctx        context.Context
	query      string
	parent     driver.Stmt
	parentSpan Span
}

func newWrappedStmt(ctx context.Context, opt opts, query string, parent driver.Stmt, parentSpan Span) wrappedStmt {
	return wrappedStmt{opt, ctx, query, parent, parentSpan}
}

func (s wrappedStmt) closeSpans() {
	s.parentSpan.Finish()
	s.parentSpan = nil
}

type wrappedResult struct {
	opts
	ctx    context.Context
	parent driver.Result
}

type wrappedRows struct {
	opts
	ctx    context.Context
	parent driver.Rows
}

// WrapDriver will wrap the passed SQL driver and return a new sql driver that uses it and also logs and traces calls using the passed logger and tracer
// The returned driver will still have to be registered with the sql package before it can be used.
//
// Important note: Seeing as the context passed into the various instrumentation calls this package calls,
// Any call without a context passed will not be instrumented. Please be sure to use the ___Context() and BeginTx() function calls added in Go 1.8
// instead of the older calls which do not accept a context.
func WrapDriver(driver driver.Driver, opts ...Opt) driver.Driver {
	d := wrappedDriver{parent: driver}

	for _, opt := range opts {
		opt(&d.opts)
	}

	if d.Logger == nil {
		d.Logger = nullLogger{}
	}
	if d.Tracer == nil {
		d.Tracer = nullTracer{}
	}

	return d
}

func logQuery(ctx context.Context, opts opts, op, query string, err error, args interface{}) {
	keyvals := []interface{}{
		LabelQuery, query,
		"err", err,
	}

	if !opts.OmitArgs && args != nil {
		keyvals = append(keyvals, LabelArgs, pretty.Sprint(args))
	}

	opts.Log(ctx, op, keyvals...)
}

func (d wrappedDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.parent.Open(name)
	if err != nil {
		return nil, err
	}

	return newWrappedConn(d.opts, conn, nilSpan, nilSpan), nil
}

func (c wrappedConn) Prepare(query string) (driver.Stmt, error) {
	span := c.parentSpan
	c.parentSpan = nil

	parent, err := c.parent.Prepare(query)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return newWrappedStmt(nil, c.opts, query, parent, span), nil
}

func (c wrappedConn) Close() error {
	c.closeSpans()
	return c.parent.Close()
}

func (c wrappedConn) Begin() (driver.Tx, error) {
	tx, err := c.parent.Begin()
	if err != nil {
		return nil, err
	}

	return newWrappedTx(nil, c.opts, tx, c, nilSpan), nil
}

func (c wrappedConn) BeginTx(ctx context.Context, opts driver.TxOptions) (tx driver.Tx, err error) {
	parentSpan := c.GetSpan(ctx).NewChild(SQLTxBegin)
	parentSpan.SetLabel(LabelComponent, "database/sql")

	span := parentSpan.NewChild(SQLTxBegin)
	span.SetLabel(LabelComponent, "database/sql")
	defer func() {
		span.SetError(err)
		span.Finish()

		if err != nil {
			c.closeSpans()
		}

		c.Log(ctx, SQLTxBegin, "err", err)
	}()

	if connBeginTx, ok := c.parent.(driver.ConnBeginTx); ok {
		tx, err = connBeginTx.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}

		return newWrappedTx(ctx, c.opts, tx, c, parentSpan), nil
	}

	tx, err = c.parent.Begin()
	if err != nil {
		return nil, err
	}

	return newWrappedTx(ctx, c.opts, tx, c, parentSpan), nil
}

func (c wrappedConn) PrepareContext(ctx context.Context, query string) (stmt driver.Stmt, err error) {
	parentSpan := c.GetSpan(ctx).NewChild(SQLPrepare)
	parentSpan.SetLabel(LabelComponent, "database/sql")

	span := parentSpan.NewChild(SQLPrepare)
	span.SetLabel(LabelComponent, "database/sql")
	defer func() {
		span.SetError(err)
		span.Finish()
		if err != nil {
			c.closeSpans()
		}

		logQuery(ctx, c.opts, SQLPrepare, query, err, nil)
	}()

	if connPrepareCtx, ok := c.parent.(driver.ConnPrepareContext); ok {
		stmt, err := connPrepareCtx.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}

		return newWrappedStmt(ctx, c.opts, query, stmt, parentSpan), nil
	}

	return c.Prepare(query)
}

func (c wrappedConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if execer, ok := c.parent.(driver.Execer); ok {
		res, err := execer.Exec(query, args)
		if err != nil {
			return nil, err
		}

		return wrappedResult{opts: c.opts, parent: res}, nil
	}

	return nil, driver.ErrSkip
}

func (c wrappedConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (r driver.Result, err error) {
	parentSpan := c.GetSpan(ctx).NewChild(query)
	parentSpan.SetLabel(LabelComponent, "database/sql")
	parentSpan.SetLabel(LabelQuery, query)
	if !c.OmitArgs {
		parentSpan.SetLabel(LabelArgs, pretty.Sprint(args))
	}
	c.parentSpan = parentSpan

	span := parentSpan.NewChild(SQLConnExec)
	span.SetLabel(LabelComponent, "database/sql")
	span.SetLabel(LabelQuery, query)
	if !c.OmitArgs {
		span.SetLabel(LabelArgs, pretty.Sprint(args))
	}
	defer func() {
		span.SetError(err)
		span.Finish()
		if err != nil {
			c.closeSpans()
		}

		logQuery(ctx, c.opts, SQLConnExec, query, err, args)
	}()

	if execContext, ok := c.parent.(driver.ExecerContext); ok {
		res, err := execContext.ExecContext(ctx, query, args)
		if err != nil {
			return nil, err
		}

		return wrappedResult{opts: c.opts, ctx: ctx, parent: res}, nil
	}

	// Fallback implementation
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return c.Exec(query, dargs)
}

func (c wrappedConn) Ping(ctx context.Context) (err error) {
	if pinger, ok := c.parent.(driver.Pinger); ok {
		span := c.GetSpan(ctx).NewChild(SQLPing)
		span.SetLabel(LabelComponent, "database/sql")
		defer func() {
			span.SetError(err)
			span.Finish()
			c.Log(ctx, SQLPing, "err", err)
		}()

		return pinger.Ping(ctx)
	}

	c.Log(ctx, "sql-dummy-ping")

	return nil
}

func (c wrappedConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	if queryer, ok := c.parent.(driver.Queryer); ok {
		rows, err := queryer.Query(query, args)
		if err != nil {
			return nil, err
		}

		return wrappedRows{opts: c.opts, parent: rows}, nil
	}

	return nil, driver.ErrSkip
}

func (c wrappedConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (rows driver.Rows, err error) {
	span := c.GetSpan(ctx).NewChild(SQLConnQuery)
	span.SetLabel(LabelComponent, "database/sql")
	span.SetLabel(LabelQuery, query)
	if !c.OmitArgs {
		span.SetLabel(LabelArgs, pretty.Sprint(args))
	}
	defer func() {
		span.SetError(err)
		span.Finish()
		logQuery(ctx, c.opts, SQLConnQuery, query, err, args)
	}()

	if queryerContext, ok := c.parent.(driver.QueryerContext); ok {
		rows, err := queryerContext.QueryContext(ctx, query, args)
		if err != nil {
			return nil, err
		}

		return wrappedRows{opts: c.opts, ctx: ctx, parent: rows}, nil
	}

	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return c.Query(query, dargs)
}

func (t wrappedTx) Commit() (err error) {
	parentSpan := t.GetSpan(t.ctx)
	if t.txSpan != nil {
		parentSpan = t.txSpan
	}

	span := parentSpan.NewChild(SQLTxCommit)
	span.SetLabel(LabelComponent, "database/sql")
	defer func() {
		span.SetError(err)
		span.Finish()
		t.closeSpans()
		t.Log(t.ctx, SQLTxCommit, "err", err)
	}()

	return t.parent.Commit()
}

func (t wrappedTx) Rollback() (err error) {
	parentSpan := t.GetSpan(t.ctx)
	if t.txSpan != nil {
		parentSpan = t.txSpan
	}

	span := parentSpan.NewChild(SQLTxRollback)
	span.SetLabel(LabelComponent, "database/sql")
	defer func() {
		span.SetError(err)
		span.Finish()
		t.closeSpans()
		t.Log(t.ctx, SQLTxRollback, "err", err)
	}()

	return t.parent.Rollback()
}

func (s wrappedStmt) Close() (err error) {
	parentSpan := s.GetSpan(s.ctx)
	if s.parentSpan != nil {
		parentSpan = s.parentSpan
	}

	span := parentSpan.NewChild(SQLStmtClose)
	span.SetLabel(LabelComponent, "database/sql")
	defer func() {
		span.SetError(err)
		span.Finish()
		s.closeSpans()
		s.Log(s.ctx, SQLStmtClose, "err", err)
	}()

	return s.parent.Close()
}

func (s wrappedStmt) NumInput() int {
	return s.parent.NumInput()
}

func (s wrappedStmt) Exec(args []driver.Value) (res driver.Result, err error) {
	span := s.GetSpan(s.ctx).NewChild(SQLStmtExec)
	span.SetLabel(LabelComponent, "database/sql")
	span.SetLabel(LabelQuery, s.query)
	span.SetLabel(LabelArgs, pretty.Sprint(args))
	defer func() {
		span.SetError(err)
		span.Finish()

		if err != nil {
			s.closeSpans()
		}

		logQuery(s.ctx, s.opts, SQLStmtExec, s.query, err, args)
	}()

	res, err = s.parent.Exec(args)
	if err != nil {
		return nil, err
	}

	return wrappedResult{opts: s.opts, ctx: s.ctx, parent: res}, nil
}

func (s wrappedStmt) Query(args []driver.Value) (rows driver.Rows, err error) {
	span := s.GetSpan(s.ctx).NewChild(SQLStmtQuery)
	span.SetLabel(LabelComponent, "database/sql")
	span.SetLabel(LabelQuery, s.query)
	span.SetLabel(LabelArgs, pretty.Sprint(args))
	defer func() {
		span.SetError(err)
		span.Finish()

		if err != nil {
			s.closeSpans()
		}
		logQuery(s.ctx, s.opts, SQLStmtQuery, s.query, err, args)
	}()

	rows, err = s.parent.Query(args)
	if err != nil {
		return nil, err
	}

	return wrappedRows{opts: s.opts, ctx: s.ctx, parent: rows}, nil
}

func (s wrappedStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (res driver.Result, err error) {
	span := s.GetSpan(ctx).NewChild(SQLStmtExec)
	span.SetLabel(LabelComponent, "database/sql")
	span.SetLabel(LabelQuery, s.query)
	span.SetLabel(LabelArgs, pretty.Sprint(args))
	defer func() {
		span.SetError(err)
		span.Finish()

		if err != nil {
			s.closeSpans()
		}

		logQuery(ctx, s.opts, SQLStmtExec, s.query, err, args)
	}()

	if stmtExecContext, ok := s.parent.(driver.StmtExecContext); ok {
		res, err := stmtExecContext.ExecContext(ctx, args)
		if err != nil {
			return nil, err
		}

		return wrappedResult{opts: s.opts, ctx: ctx, parent: res}, nil
	}

	// Fallback implementation
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return s.Exec(dargs)
}

func (s wrappedStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (rows driver.Rows, err error) {
	span := s.GetSpan(ctx).NewChild(SQLStmtQuery)
	span.SetLabel(LabelComponent, "database/sql")
	span.SetLabel(LabelQuery, s.query)
	span.SetLabel(LabelArgs, pretty.Sprint(args))
	defer func() {
		span.SetError(err)
		span.Finish()

		if err != nil {
			s.closeSpans()
		}

		logQuery(ctx, s.opts, SQLStmtQuery, s.query, err, args)
	}()

	if stmtQueryContext, ok := s.parent.(driver.StmtQueryContext); ok {
		rows, err := stmtQueryContext.QueryContext(ctx, args)
		if err != nil {
			return nil, err
		}

		return wrappedRows{opts: s.opts, ctx: ctx, parent: rows}, nil
	}

	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return s.Query(dargs)
}

func (r wrappedResult) LastInsertId() (id int64, err error) {
	span := r.GetSpan(r.ctx).NewChild(SQLLastInsertID)
	span.SetLabel(LabelComponent, "database/sql")
	defer func() {
		span.SetError(err)
		span.Finish()
		r.Log(r.ctx, SQLLastInsertID, "err", err)
	}()

	return r.parent.LastInsertId()
}

func (r wrappedResult) RowsAffected() (num int64, err error) {
	span := r.GetSpan(r.ctx).NewChild(SQLRowsAffected)
	span.SetLabel(LabelComponent, "database/sql")
	defer func() {
		span.SetError(err)
		span.Finish()
		r.Log(r.ctx, SQLRowsAffected, "err", err)
	}()

	return r.parent.RowsAffected()
}

func (r wrappedRows) Columns() []string {
	return r.parent.Columns()
}

func (r wrappedRows) Close() error {
	return r.parent.Close()
}

func (r wrappedRows) Next(dest []driver.Value) (err error) {
	span := r.GetSpan(r.ctx).NewChild(SQLRowsNext)
	span.SetLabel(LabelComponent, "database/sql")
	defer func() {
		span.SetError(err)
		span.Finish()
		r.Log(r.ctx, SQLRowsNext, "err", err)
	}()

	return r.parent.Next(dest)
}

// namedValueToValue is a helper function copied from the database/sql package
func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	dargs := make([]driver.Value, len(named))
	for n, param := range named {
		if len(param.Name) > 0 {
			return nil, errors.New("sql: driver does not support the use of Named Parameters")
		}
		dargs[n] = param.Value
	}
	return dargs, nil
}
