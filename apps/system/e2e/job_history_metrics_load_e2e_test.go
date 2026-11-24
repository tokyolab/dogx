//go:build integration && e2e && load

package e2e

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"time"
)

// jobHistoryDatabaseSample contains catalog and cumulative-statistics observations,
// not exact live row counts. WAL covers the entire isolated PostgreSQL cluster;
// database counters include this observer's queries. No query below scans Task
// content, resets statistics, forces VACUUM, or changes the database configuration.
type jobHistoryDatabaseSample struct {
	At             time.Time
	ObservedAt     time.Time
	CollectionMS   float64
	Schema         string
	Database       string
	WAL            jobHistoryWALStats
	Transactions   jobHistoryTransactionStats
	Tables         []jobHistoryTableStats
	Indexes        []jobHistoryIndexStats
	Connections    map[string]jobLoadConnections
	Waits          []jobHistoryWaitStats
	VacuumProgress []jobHistoryVacuumProgress
}

type jobHistoryWALStats struct {
	Records, FullPageImages, Bytes, BuffersFull int64
	StatsReset                                  *time.Time
}

type jobHistoryTransactionStats struct {
	Commits, Rollbacks, Deadlocks, BlocksRead, BlocksHit int64
	TempFiles, TempBytes                                 int64
	BlockReadMS, BlockWriteMS                            float64
	StatsReset                                           *time.Time
}

type jobHistoryTableStats struct {
	Name string
	// TableBytes already includes TOAST storage. ToastBytes is that subset,
	// including its indexes, and must not be added to TotalBytes a second time.
	TableBytes, IndexBytes, TotalBytes, ToastBytes int64
	LiveRowsEstimate, DeadRowsEstimate             int64
	Inserted, Updated, Deleted                     int64
	VacuumCount, AutovacuumCount                   int64
	LastVacuum, LastAutovacuum                     *time.Time
	// PostgreSQL versions without this counter leave it nil, rather than
	// reporting zero cost. This value is cumulative milliseconds, not elapsed
	// time spent blocking an online request.
	TotalAutovacuumMS *float64
}

type jobHistoryIndexStats struct {
	Table, Name                       string
	Bytes, Scans, TuplesRead, Fetched int64
}

type jobHistoryWaitStats struct {
	Process, State, Type, Event string
	Connections                 int64
}

type jobHistoryVacuumProgress struct {
	Table, Phase                              string
	PID                                       int
	HeapBlocks, ScannedBlocks, VacuumedBlocks int64
}

// jobHistoryDatabaseEnvironment records only public runtime and tuning facts.
// It deliberately omits connection strings, credentials and arbitrary settings.
type jobHistoryDatabaseEnvironment struct {
	At                                              time.Time
	Schema, Database, ServerVersion                 string
	ObserverGoVersion, ObserverGOOS, ObserverGOARCH string
	LogicalCPUs                                     int
	Settings                                        map[string]string
}

// readJobHistoryDatabaseEnvironment is intended for the preparation window.
// The supplied context bounds all statements; this function does not own db.
func readJobHistoryDatabaseEnvironment(ctx context.Context, db *sql.DB, schema string) (jobHistoryDatabaseEnvironment, error) {
	result := jobHistoryDatabaseEnvironment{Schema: schema, ObserverGoVersion: runtime.Version(),
		ObserverGOOS: runtime.GOOS, ObserverGOARCH: runtime.GOARCH, LogicalCPUs: runtime.NumCPU(), Settings: make(map[string]string)}
	if db == nil || schema == "" {
		return result, errors.New("history database environment requires a database and schema")
	}
	var currentSchema string
	if err := db.QueryRowContext(ctx, `SELECT clock_timestamp(), current_database(), current_schema(), current_setting('server_version')`).
		Scan(&result.At, &result.Database, &currentSchema, &result.ServerVersion); err != nil {
		return result, fmt.Errorf("read history database environment: %w", err)
	}
	if currentSchema != schema {
		return result, errors.New("history database observer is outside its fixture schema")
	}
	settings := []string{"server_version_num", "block_size", "wal_segment_size", "shared_buffers", "work_mem", "max_connections",
		"autovacuum", "autovacuum_vacuum_scale_factor", "autovacuum_vacuum_threshold", "autovacuum_naptime",
		"log_autovacuum_min_duration", "track_io_timing", "track_wal_io_timing", "TimeZone"}
	rows, err := db.QueryContext(ctx, `SELECT name, setting, coalesce(unit, '') FROM pg_settings WHERE name = ANY($1) ORDER BY name`, settings)
	if err != nil {
		return result, fmt.Errorf("read history database tuning: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name, value, unit string
		if err := rows.Scan(&name, &value, &unit); err != nil {
			return result, fmt.Errorf("scan history database tuning: %w", err)
		}
		if unit != "" {
			value += " " + unit
		}
		result.Settings[name] = value
	}
	return result, rows.Err()
}

// readJobHistoryDatabaseSample performs a fixed number of catalog/statistics
// queries. Call at window boundaries and at low frequency (30 seconds), not for
// every Task. Table statistics are schema-scoped. PostgreSQL does not expose a
// backend's current search_path in pg_stat_activity, so connection observations
// instead require the fixture's exact application names in the current database.
// Multiple statements are not an atomic snapshot, and statistics may lag commits.
func readJobHistoryDatabaseSample(ctx context.Context, db *sql.DB, schema string, applicationNames map[string]string) (jobHistoryDatabaseSample, error) {
	started := time.Now()
	result := jobHistoryDatabaseSample{ObservedAt: started.UTC(), Schema: schema, Connections: make(map[string]jobLoadConnections),
		Tables: make([]jobHistoryTableStats, 0), Indexes: make([]jobHistoryIndexStats, 0),
		Waits: make([]jobHistoryWaitStats, 0), VacuumProgress: make([]jobHistoryVacuumProgress, 0)}
	if db == nil || schema == "" {
		return result, errors.New("history database sample requires a database and schema")
	}
	var walReset, databaseReset sql.NullTime
	if err := db.QueryRowContext(ctx, `SELECT clock_timestamp(), current_database(),
		w.wal_records, w.wal_fpi, w.wal_bytes::bigint, w.wal_buffers_full, w.stats_reset,
		d.xact_commit, d.xact_rollback, d.deadlocks, d.blks_read, d.blks_hit,
		d.temp_files, d.temp_bytes, d.blk_read_time, d.blk_write_time, d.stats_reset
		FROM pg_stat_wal w CROSS JOIN pg_stat_database d WHERE d.datname = current_database()`).
		Scan(&result.At, &result.Database, &result.WAL.Records, &result.WAL.FullPageImages, &result.WAL.Bytes,
			&result.WAL.BuffersFull, &walReset, &result.Transactions.Commits, &result.Transactions.Rollbacks,
			&result.Transactions.Deadlocks, &result.Transactions.BlocksRead, &result.Transactions.BlocksHit,
			&result.Transactions.TempFiles, &result.Transactions.TempBytes, &result.Transactions.BlockReadMS,
			&result.Transactions.BlockWriteMS, &databaseReset); err != nil {
		return result, fmt.Errorf("read history database cumulative statistics: %w", err)
	}
	result.WAL.StatsReset, result.Transactions.StatsReset = jobHistoryOptionalTime(walReset), jobHistoryOptionalTime(databaseReset)
	if err := readJobHistoryTableStats(ctx, db, &result); err != nil {
		return result, err
	}
	if err := readJobHistoryIndexStats(ctx, db, &result); err != nil {
		return result, err
	}
	if err := readJobHistoryActivity(ctx, db, applicationNames, &result); err != nil {
		return result, err
	}
	if err := readJobHistoryVacuumProgress(ctx, db, &result); err != nil {
		return result, err
	}
	result.CollectionMS = time.Since(started).Seconds() * 1000
	return result, nil
}

func readJobHistoryTableStats(ctx context.Context, db *sql.DB, result *jobHistoryDatabaseSample) error {
	rows, err := db.QueryContext(ctx, `SELECT s.relname,
		pg_table_size(s.relid), pg_indexes_size(s.relid), pg_total_relation_size(s.relid),
		CASE WHEN c.reltoastrelid = 0 THEN 0 ELSE pg_total_relation_size(c.reltoastrelid) END,
		s.n_live_tup, s.n_dead_tup, s.n_tup_ins, s.n_tup_upd, s.n_tup_del,
		s.vacuum_count, s.autovacuum_count, s.last_vacuum, s.last_autovacuum,
		(to_jsonb(s)->>'total_autovacuum_time')::double precision
		FROM pg_stat_user_tables s JOIN pg_class c ON c.oid = s.relid
		WHERE s.schemaname = $1 AND s.relname IN ('job_definition', 'job_instance', 'job_task', 'system_job_e2e_records')
		ORDER BY s.relname`, result.Schema)
	if err != nil {
		return fmt.Errorf("read history table statistics: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var value jobHistoryTableStats
		var vacuum, autovacuum sql.NullTime
		var autovacuumMS sql.NullFloat64
		if err := rows.Scan(&value.Name, &value.TableBytes, &value.IndexBytes, &value.TotalBytes, &value.ToastBytes,
			&value.LiveRowsEstimate, &value.DeadRowsEstimate, &value.Inserted, &value.Updated, &value.Deleted,
			&value.VacuumCount, &value.AutovacuumCount, &vacuum, &autovacuum, &autovacuumMS); err != nil {
			return fmt.Errorf("scan history table statistics: %w", err)
		}
		value.LastVacuum, value.LastAutovacuum = jobHistoryOptionalTime(vacuum), jobHistoryOptionalTime(autovacuum)
		if autovacuumMS.Valid {
			value.TotalAutovacuumMS = &autovacuumMS.Float64
		}
		result.Tables = append(result.Tables, value)
	}
	return rows.Err()
}

func readJobHistoryIndexStats(ctx context.Context, db *sql.DB, result *jobHistoryDatabaseSample) error {
	rows, err := db.QueryContext(ctx, `SELECT relname, indexrelname, pg_relation_size(indexrelid), idx_scan, idx_tup_read, idx_tup_fetch
		FROM pg_stat_user_indexes WHERE schemaname = $1 AND relname IN ('job_definition', 'job_instance', 'job_task', 'system_job_e2e_records')
		ORDER BY relname, indexrelname`, result.Schema)
	if err != nil {
		return fmt.Errorf("read history index statistics: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var value jobHistoryIndexStats
		if err := rows.Scan(&value.Table, &value.Name, &value.Bytes, &value.Scans, &value.TuplesRead, &value.Fetched); err != nil {
			return fmt.Errorf("scan history index statistics: %w", err)
		}
		result.Indexes = append(result.Indexes, value)
	}
	return rows.Err()
}

func readJobHistoryActivity(ctx context.Context, db *sql.DB, names map[string]string, result *jobHistoryDatabaseSample) error {
	rows, err := db.QueryContext(ctx, `SELECT application_name, coalesce(state, ''),
		coalesce(wait_event_type, ''), coalesce(wait_event, ''), count(*),
		coalesce(max(extract(epoch FROM clock_timestamp() - query_start) * 1000) FILTER (WHERE state = 'active'), 0)
		FROM pg_stat_activity WHERE datname = current_database() AND application_name = ANY($1)
		GROUP BY application_name, state, wait_event_type, wait_event
		ORDER BY application_name, state, wait_event_type, wait_event`, mapJobLoadNames(names))
	if err != nil {
		return fmt.Errorf("read history database activity: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name string
		var value jobHistoryWaitStats
		var oldestMS float64
		if err := rows.Scan(&name, &value.State, &value.Type, &value.Event, &value.Connections, &oldestMS); err != nil {
			return fmt.Errorf("scan history database activity: %w", err)
		}
		value.Process = names[name]
		counts := result.Connections[value.Process]
		counts.Total += int(value.Connections)
		if value.State == "active" {
			counts.Active += int(value.Connections)
		}
		if value.Type == "Lock" {
			counts.LockWaiting += int(value.Connections)
		}
		counts.OldestActiveMilliseconds = max(counts.OldestActiveMilliseconds, oldestMS)
		result.Connections[value.Process] = counts
		if value.Type != "" {
			result.Waits = append(result.Waits, value)
		}
	}
	return rows.Err()
}

func readJobHistoryVacuumProgress(ctx context.Context, db *sql.DB, result *jobHistoryDatabaseSample) error {
	rows, err := db.QueryContext(ctx, `SELECT c.relname, p.pid, p.phase, p.heap_blks_total, p.heap_blks_scanned, p.heap_blks_vacuumed
		FROM pg_stat_progress_vacuum p JOIN pg_class c ON c.oid = p.relid JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE p.datname = current_database() AND n.nspname = $1 ORDER BY c.relname, p.pid`, result.Schema)
	if err != nil {
		return fmt.Errorf("read history vacuum progress: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var value jobHistoryVacuumProgress
		if err := rows.Scan(&value.Table, &value.PID, &value.Phase, &value.HeapBlocks, &value.ScannedBlocks, &value.VacuumedBlocks); err != nil {
			return fmt.Errorf("scan history vacuum progress: %w", err)
		}
		result.VacuumProgress = append(result.VacuumProgress, value)
	}
	return rows.Err()
}

func jobHistoryOptionalTime(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	return &value.Time
}

// jobHistoryDatabaseDelta is meaningful only for two samples from the same
// isolated cluster and database with unchanged cumulative-statistics epochs.
// Table size and row-estimate changes may be negative after deletion or VACUUM.
type jobHistoryDatabaseDelta struct {
	ElapsedSeconds float64
	WAL            jobHistoryWALStats
	Transactions   jobHistoryTransactionStats
	Tables         []jobHistoryTableStats
}

func jobHistoryDatabaseDifference(before, after jobHistoryDatabaseSample) (jobHistoryDatabaseDelta, error) {
	result := jobHistoryDatabaseDelta{ElapsedSeconds: after.At.Sub(before.At).Seconds(), Tables: make([]jobHistoryTableStats, 0)}
	if before.Schema != after.Schema || before.Database != after.Database || result.ElapsedSeconds <= 0 ||
		!jobHistorySameStatsEpoch(before.WAL.StatsReset, after.WAL.StatsReset) ||
		!jobHistorySameStatsEpoch(before.Transactions.StatsReset, after.Transactions.StatsReset) {
		return result, errors.New("history statistics samples do not share a valid measurement window")
	}
	result.WAL = jobHistoryWALStats{Records: after.WAL.Records - before.WAL.Records,
		FullPageImages: after.WAL.FullPageImages - before.WAL.FullPageImages, Bytes: after.WAL.Bytes - before.WAL.Bytes,
		BuffersFull: after.WAL.BuffersFull - before.WAL.BuffersFull}
	b, a := before.Transactions, after.Transactions
	result.Transactions = jobHistoryTransactionStats{Commits: a.Commits - b.Commits, Rollbacks: a.Rollbacks - b.Rollbacks,
		Deadlocks: a.Deadlocks - b.Deadlocks, BlocksRead: a.BlocksRead - b.BlocksRead, BlocksHit: a.BlocksHit - b.BlocksHit,
		TempFiles: a.TempFiles - b.TempFiles, TempBytes: a.TempBytes - b.TempBytes,
		BlockReadMS: a.BlockReadMS - b.BlockReadMS, BlockWriteMS: a.BlockWriteMS - b.BlockWriteMS}
	for _, value := range []int64{result.WAL.Records, result.WAL.FullPageImages, result.WAL.Bytes, result.WAL.BuffersFull,
		result.Transactions.Commits, result.Transactions.Rollbacks, result.Transactions.Deadlocks, result.Transactions.BlocksRead,
		result.Transactions.BlocksHit, result.Transactions.TempFiles, result.Transactions.TempBytes} {
		if value < 0 {
			return result, errors.New("history cumulative statistics decreased during measurement")
		}
	}
	previous := make(map[string]jobHistoryTableStats, len(before.Tables))
	for _, value := range before.Tables {
		previous[value.Name] = value
	}
	for _, value := range after.Tables {
		old, ok := previous[value.Name]
		if !ok {
			return result, errors.New("history measurement added an untracked table")
		}
		delta := jobHistoryTableStats{Name: value.Name, TableBytes: value.TableBytes - old.TableBytes,
			IndexBytes: value.IndexBytes - old.IndexBytes, TotalBytes: value.TotalBytes - old.TotalBytes, ToastBytes: value.ToastBytes - old.ToastBytes,
			LiveRowsEstimate: value.LiveRowsEstimate - old.LiveRowsEstimate, DeadRowsEstimate: value.DeadRowsEstimate - old.DeadRowsEstimate,
			Inserted: value.Inserted - old.Inserted, Updated: value.Updated - old.Updated, Deleted: value.Deleted - old.Deleted,
			VacuumCount: value.VacuumCount - old.VacuumCount, AutovacuumCount: value.AutovacuumCount - old.AutovacuumCount}
		for _, count := range []int64{delta.Inserted, delta.Updated, delta.Deleted, delta.VacuumCount, delta.AutovacuumCount} {
			if count < 0 {
				return result, errors.New("history table statistics decreased during measurement")
			}
		}
		if value.TotalAutovacuumMS != nil && old.TotalAutovacuumMS != nil {
			elapsed := *value.TotalAutovacuumMS - *old.TotalAutovacuumMS
			if elapsed < 0 {
				return result, errors.New("history autovacuum timing decreased during measurement")
			}
			delta.TotalAutovacuumMS = &elapsed
		}
		result.Tables = append(result.Tables, delta)
		delete(previous, value.Name)
	}
	if len(previous) != 0 {
		return result, errors.New("history measurement lost a tracked table")
	}
	return result, nil
}

func jobHistorySameStatsEpoch(before, after *time.Time) bool {
	if before == nil || after == nil {
		return before == nil && after == nil
	}
	return before.Equal(*after)
}
