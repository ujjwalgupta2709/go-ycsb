// Copyright 2024 Rubrik, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Package rubrik_cockroachdb implements a go-ycsb DB driver that issues realistic
// queries against the five main production CockroachDB tables in Rubrik CDM:
//   - sd.files        (SDFS file/dir metadata; Atlas MDS point-lookups by uuid)
//   - sd.event        (job/workload events)
//   - sd.event_series (aggregated event series per org)
//   - sd.job_instance (job lifecycle tracking)
//   - sd.report_stats (per-object monthly storage stats)
//
// Run-phase table weights (% of ops): files=55, event=20, event_series=13,
// job_instance=8, report_stats=4.
// Load-phase table weights (% of inserts): files=80, event=12, event_series=4,
// job_instance=2.5, report_stats=1.5. Enable with crdb.load_mode=true.
//
// Global operation mix mirrors live production: 94.2% SELECT, 2.37% UPDATE,
// 2.36% INSERT, 1.02% DELETE.
package rubrik_cockroachdb

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/twmb/murmur3"
)

// Config property keys
const (
	crdbHost              = "crdb.host"
	crdbHosts             = "crdb.hosts"              // comma-separated list, overrides crdb.host if set
	crdbPort              = "crdb.port"
	crdbUser              = "crdb.user"
	crdbPassword          = "crdb.password"
	crdbDBName            = "crdb.db"
	crdbSSLMode           = "crdb.sslmode"
	crdbLoadMode          = "crdb.load_mode"          // bool: true = load phase, false = run phase
	crdbLoadTargetTables  = "crdb.load_target_tables" // comma-separated e.g. "sd.event,sd.job_instance"; empty = all tables
)

// table name constants
const (
	tableFiles       = "sd.files"
	tableEvent       = "sd.event"
	tableEventSeries = "sd.event_series"
	tableJobInstance = "sd.job_instance"
	tableReportStats = "sd.report_stats"
)

// tableWeight defines cumulative upper-bound thresholds for table selection.
type tableWeight struct {
	name      string
	threshold float64 // cumulative upper bound in [0,1)
}

// runWeights: files=55%, event=20%, event_series=13%, job_instance=8%, report_stats=4%
var runWeights = []tableWeight{
	{tableFiles, 0.55},
	{tableEvent, 0.75},
	{tableEventSeries, 0.88},
	{tableJobInstance, 0.96},
	{tableReportStats, 1.00},
}

// loadWeights: files=80%, event=12%, event_series=4%, job_instance=2.5%, report_stats=1.5%
var loadWeights = []tableWeight{
	{tableFiles, 0.80},
	{tableEvent, 0.92},
	{tableEventSeries, 0.96},
	{tableJobInstance, 0.985},
	{tableReportStats, 1.00},
}

func pickTable(weights []tableWeight) string {
	r := rand.Float64()
	for _, w := range weights {
		if r < w.threshold {
			return w.name
		}
	}
	return weights[len(weights)-1].name
}

// effectiveLoadWeights returns loadWeights filtered and re-normalised to
// only include tables in db.loadTargetTables (all tables when nil).
func (db *cockroachDB) effectiveLoadWeights() []tableWeight {
	if db.loadTargetTables == nil {
		return loadWeights
	}
	// Compute per-table deltas from the cumulative loadWeights slice.
	deltas := make([]float64, len(loadWeights))
	prev := 0.0
	for i, w := range loadWeights {
		deltas[i] = w.threshold - prev
		prev = w.threshold
	}
	// Sum of selected deltas for normalisation.
	var total float64
	for i, w := range loadWeights {
		if db.loadTargetTables[w.name] {
			total += deltas[i]
		}
	}
	if total == 0 {
		return loadWeights // safety fallback
	}
	var result []tableWeight
	cumulative := 0.0
	for i, w := range loadWeights {
		if db.loadTargetTables[w.name] {
			cumulative += deltas[i] / total
			result = append(result, tableWeight{w.name, cumulative})
		}
	}
	return result
}

// hashToken derives the token__ column value from a string key,
// matching the murmur3 partitioner used by cqlproxy.
func hashToken(s string) int64 {
	return int64(murmur3.Sum64([]byte(s)))
}

// derivedTime returns a deterministic creation_time string for report_stats
// rows so that reads and inserts for the same key agree on the full PK.
func derivedTime(key string) string {
	// map key hash to a timestamp within the last year
	epoch := int64(murmur3.Sum32([]byte(key))) % int64(365*24*3600)
	return time.Unix(epoch, 0).UTC().Format(time.RFC3339)
}

// Fixed organization UUIDs simulating a small set of tenants for event_series.
var orgIDs = []string{
	"00000000-0000-0000-0000-000000000001",
	"00000000-0000-0000-0000-000000000002",
	"00000000-0000-0000-0000-000000000003",
	"00000000-0000-0000-0000-000000000004",
	"00000000-0000-0000-0000-000000000005",
}

type cockroachCreator struct{}

type cockroachDB struct {
	p                *properties.Properties
	pools            []*sql.DB // one pool per node for load balancing
	counter          atomic.Uint64
	verbose          bool
	loadMode         bool
	loadTargetTables map[string]bool // nil = all tables; non-nil = only listed tables get inserts
}

func (db *cockroachDB) nextPool() *sql.DB {
	idx := db.counter.Add(1) % uint64(len(db.pools))
	return db.pools[idx]
}

type contextKey string

const stateKey = contextKey("cockroachDB")

type cockroachState struct {
	stmtCache map[string]*sql.Stmt
	conn      *sql.Conn
	rng       *rand.Rand
}

func (c cockroachCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	d := new(cockroachDB)
	d.p = p

	port := p.GetInt(crdbPort, 26257)
	user := p.GetString(crdbUser, "root")
	password := p.GetString(crdbPassword, "")
	dbName := p.GetString(crdbDBName, "sd")
	sslMode := p.GetString(crdbSSLMode, "disable")
	d.loadMode = p.GetBool(crdbLoadMode, false)
	d.verbose = p.GetBool(prop.Verbose, prop.VerboseDefault)
	if tables := p.GetString(crdbLoadTargetTables, ""); tables != "" {
		d.loadTargetTables = make(map[string]bool)
		for _, t := range strings.Split(tables, ",") {
			if trimmed := strings.TrimSpace(t); trimmed != "" {
				d.loadTargetTables[trimmed] = true
			}
		}
	}

	// Build host list: crdb.hosts (comma-separated) takes priority over crdb.host
	var hosts []string
	if h := p.GetString(crdbHosts, ""); h != "" {
		for _, host := range strings.Split(h, ",") {
			if trimmed := strings.TrimSpace(host); trimmed != "" {
				hosts = append(hosts, trimmed)
			}
		}
	}
	if len(hosts) == 0 {
		hosts = []string{p.GetString(crdbHost, "127.0.0.1")}
	}

	const (
		caCert     = "/var/lib/rubrik/certs/cockroachdb/ca.crt"
		clientCert = "/var/lib/rubrik/certs/cockroachdb/client.root.crt"
		clientKey  = "/var/lib/rubrik/certs/cockroachdb/client.root.key"
	)

	threadCount := int(p.GetInt64(prop.ThreadCount, prop.ThreadCountDefault))
	connsPerNode := (threadCount / len(hosts)) + 5

	for _, host := range hosts {
		var dsn string
		if sslMode == "require" || sslMode == "verify-full" || sslMode == "verify-ca" {
			dsn = fmt.Sprintf(
				"postgres://%s@%s:%d/%s?sslmode=%s&sslrootcert=%s&sslcert=%s&sslkey=%s",
				user, host, port, dbName, sslMode, caCert, clientCert, clientKey,
			)
		} else if password == "" {
			dsn = fmt.Sprintf("postgres://%s@%s:%d/%s?sslmode=%s", user, host, port, dbName, sslMode)
		} else {
			dsn = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s", user, password, host, port, dbName, sslMode)
		}

		pool, err := sql.Open("postgres", dsn)
		if err != nil {
			return nil, fmt.Errorf("open crdb failed for %s: %w", host, err)
		}
		pool.SetMaxIdleConns(connsPerNode)
		pool.SetMaxOpenConns(connsPerNode)
		d.pools = append(d.pools, pool)
	}

	return d, nil
}

func (db *cockroachDB) Close() error {
	var lastErr error
	for _, pool := range db.pools {
		if err := pool.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (db *cockroachDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	conn, err := db.nextPool().Conn(ctx)
	if err != nil {
		panic(fmt.Sprintf("failed to create db conn: %v", err))
	}
	state := &cockroachState{
		stmtCache: make(map[string]*sql.Stmt),
		conn:      conn,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	return context.WithValue(ctx, stateKey, state)
}

func (db *cockroachDB) CleanupThread(ctx context.Context) {
	state := ctx.Value(stateKey).(*cockroachState)
	for _, stmt := range state.stmtCache {
		stmt.Close()
	}
	state.conn.Close()
}

func (db *cockroachDB) getAndCacheStmt(ctx context.Context, query string) (*sql.Stmt, error) {
	state := ctx.Value(stateKey).(*cockroachState)
	if stmt, ok := state.stmtCache[query]; ok {
		return stmt, nil
	}
	stmt, err := state.conn.PrepareContext(ctx, query)
	if err == sql.ErrConnDone {
		if state.conn, err = db.nextPool().Conn(ctx); err == nil {
			stmt, err = state.conn.PrepareContext(ctx, query)
		}
	}
	if err != nil {
		return nil, err
	}
	state.stmtCache[query] = stmt
	return stmt, nil
}

func (db *cockroachDB) clearCacheIfFailed(ctx context.Context, query string, err error) {
	if err == nil {
		return
	}
	state := ctx.Value(stateKey).(*cockroachState)
	if stmt, ok := state.stmtCache[query]; ok {
		stmt.Close()
	}
	delete(state.stmtCache, query)
}

func (db *cockroachDB) execQuery(ctx context.Context, query string, args ...interface{}) error {
	if db.verbose {
		fmt.Printf("%s %v\n", query, args)
	}
	stmt, err := db.getAndCacheStmt(ctx, query)
	if err != nil {
		return err
	}
	_, err = stmt.ExecContext(ctx, args...)
	db.clearCacheIfFailed(ctx, query, err)
	return err
}

func (db *cockroachDB) querySingleRow(ctx context.Context, query string, args ...interface{}) (map[string][]byte, error) {
	if db.verbose {
		fmt.Printf("%s %v\n", query, args)
	}
	stmt, err := db.getAndCacheStmt(ctx, query)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		db.clearCacheIfFailed(ctx, query, err)
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		return nil, rows.Err()
	}

	dest := make([]interface{}, len(cols))
	for i := range dest {
		dest[i] = new([]byte)
	}
	if err = rows.Scan(dest...); err != nil {
		return nil, err
	}
	m := make(map[string][]byte, len(cols))
	for i, col := range cols {
		m[col] = *dest[i].(*[]byte)
	}
	return m, rows.Err()
}

// --------------------------------------------------------------------------
// Read — 97.1% of operations
// --------------------------------------------------------------------------

func (db *cockroachDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	state := ctx.Value(stateKey).(*cockroachState)
	switch pickTable(runWeights) {
	case tableFiles:
		return db.readFiles(ctx, key)
	case tableEvent:
		return db.readEvent(ctx, key)
	case tableEventSeries:
		return db.readEventSeries(ctx, orgIDs[state.rng.Intn(len(orgIDs))], key)
	case tableJobInstance:
		return db.readJobInstance(ctx, key)
	default:
		return db.readReportStats(ctx, key)
	}
}

func (db *cockroachDB) readFiles(ctx context.Context, key string) (map[string][]byte, error) {
	query := `SELECT uuid, stripe_id, parent_uuid_hint FROM sd.files WHERE token__uuid = $1 AND uuid = $2 AND stripe_id = -1 LIMIT 1`
	return db.querySingleRow(ctx, query, hashToken(key), key)
}

func (db *cockroachDB) readEvent(ctx context.Context, key string) (map[string][]byte, error) {
	query := `SELECT event_id, event_type, event_severity, object_id FROM sd.event WHERE event_id = $1 LIMIT 1`
	return db.querySingleRow(ctx, query, key)
}

func (db *cockroachDB) readEventSeries(ctx context.Context, orgID, key string) (map[string][]byte, error) {
	query := `SELECT organization_id, event_series_id, latest_event_status, latest_time FROM sd.event_series WHERE organization_id = $1 AND event_series_id = $2 LIMIT 1`
	return db.querySingleRow(ctx, query, orgID, key)
}

func (db *cockroachDB) readJobInstance(ctx context.Context, key string) (map[string][]byte, error) {
	query := `SELECT token__job_id, job_id, instance_id, job_type_2, status, start_time FROM sd.job_instance WHERE token__job_id = $1 AND job_id = $2 LIMIT 1`
	return db.querySingleRow(ctx, query, hashToken(key), key)
}

func (db *cockroachDB) readReportStats(ctx context.Context, key string) (map[string][]byte, error) {
	query := `SELECT token__month_object_id, month_object_id, creation_time, local FROM sd.report_stats WHERE token__month_object_id = $1 AND month_object_id = $2 LIMIT 1`
	return db.querySingleRow(ctx, query, hashToken(key), key)
}

// --------------------------------------------------------------------------
// Scan — disabled in workload (scanproportion=0)
// --------------------------------------------------------------------------

func (db *cockroachDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, nil
}

// --------------------------------------------------------------------------
// Update — 0.94% of operations
// --------------------------------------------------------------------------

func (db *cockroachDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	state := ctx.Value(stateKey).(*cockroachState)
	switch pickTable(runWeights) {
	case tableFiles:
		query := `UPDATE sd.files SET open_heartbeat_time = $1 WHERE token__uuid = $2 AND uuid = $3 AND stripe_id = -1`
		return db.execQuery(ctx, query, time.Now().UnixNano(), hashToken(key), key)
	case tableEvent:
		severities := []string{"DEBUG", "INFO", "WARNING", "ERROR"}
		sev := severities[state.rng.Intn(len(severities))]
		query := `UPDATE sd.event SET event_severity = $1 WHERE event_id = $2`
		return db.execQuery(ctx, query, sev, key)
	case tableEventSeries:
		statuses := []string{"Active", "Resolved"}
		st := statuses[state.rng.Intn(len(statuses))]
		query := `UPDATE sd.event_series SET event_series_status = $1 WHERE organization_id = $2 AND event_series_id = $3`
		return db.execQuery(ctx, query, st, orgIDs[state.rng.Intn(len(orgIDs))], key)
	case tableJobInstance:
		statuses := []string{"QUEUED", "RUNNING", "SUCCEEDED", "FAILED"}
		st := statuses[state.rng.Intn(len(statuses))]
		query := `UPDATE sd.job_instance SET status = $1 WHERE token__job_id = $2 AND job_id = $3`
		return db.execQuery(ctx, query, st, hashToken(key), key)
	default:
		query := `UPDATE sd.report_stats SET local = $1 WHERE token__month_object_id = $2 AND month_object_id = $3 AND creation_time = $4`
		return db.execQuery(ctx, query, fmt.Sprintf("%d", state.rng.Int63()), hashToken(key), key, derivedTime(key))
	}
}

// --------------------------------------------------------------------------
// Insert — 1.77% of operations (run phase) / 100% of operations (load phase)
// --------------------------------------------------------------------------

func (db *cockroachDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	state := ctx.Value(stateKey).(*cockroachState)
	weights := runWeights
	if db.loadMode {
		weights = db.effectiveLoadWeights()
	}
	switch pickTable(weights) {
	case tableFiles:
		return db.insertFiles(ctx, key, state.rng)
	case tableEvent:
		return db.insertEvent(ctx, key, state.rng)
	case tableEventSeries:
		return db.insertEventSeries(ctx, key, state.rng)
	case tableJobInstance:
		return db.insertJobInstance(ctx, key, state.rng)
	default:
		return db.insertReportStats(ctx, key, state.rng)
	}
}

func (db *cockroachDB) insertFiles(ctx context.Context, key string, rng *rand.Rand) error {
	parentUUID := fmt.Sprintf("parent-%x", rng.Int63())
	query := `INSERT INTO sd.files (token__uuid, uuid, stripe_id, parent_uuid_hint, birth_time) VALUES ($1, $2, -1, $3, $4) ON CONFLICT DO NOTHING`
	return db.execQuery(ctx, query, hashToken(key), key, parentUUID, time.Now().UnixNano())
}

func (db *cockroachDB) insertEvent(ctx context.Context, key string, rng *rand.Rand) error {
	eventTypes := []string{"Backup", "Restore", "Replication", "CloudArchival", "LogBackup"}
	severities := []string{"DEBUG", "INFO", "WARNING", "ERROR"}
	statuses := []string{"Success", "Failure", "Warning", "Info"}
	objectTypes := []string{"VirtualMachine", "Fileset", "ManagedVolume", "LinuxFileset"}
	et := eventTypes[rng.Intn(len(eventTypes))]
	query := `INSERT INTO sd.event (
		event_id, event_info, event_name, event_series_id,
		event_severity, event_status, event_type,
		managed_ids, object_id, object_name, object_type, time
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT DO NOTHING`
	return db.execQuery(ctx, query,
		key,
		fmt.Sprintf("info-%x", rng.Int63()),
		et,
		fmt.Sprintf("series-%x", rng.Int63()),
		severities[rng.Intn(len(severities))],
		statuses[rng.Intn(len(statuses))],
		et,
		"[]",
		fmt.Sprintf("obj-%x", rng.Int63()),
		"TestObject",
		objectTypes[rng.Intn(len(objectTypes))],
		time.Now().Unix(),
	)
}

func (db *cockroachDB) insertEventSeries(ctx context.Context, key string, rng *rand.Rand) error {
	orgID := orgIDs[rng.Intn(len(orgIDs))]
	statuses := []string{"Active", "Resolved", "Info"}
	severities := []string{"Critical", "Warning", "Info"}
	eventTypes := []string{"Backup", "Restore", "Replication"}
	objectTypes := []string{"VirtualMachine", "Fileset", "ManagedVolume"}
	query := `INSERT INTO sd.event_series (
		organization_id, event_series_id, event_series_status,
		latest_event_id, latest_event_name, latest_event_severity,
		latest_event_status, latest_event_type,
		latest_object_id, latest_object_name, latest_object_type,
		latest_time, warning_count
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT DO NOTHING`
	return db.execQuery(ctx, query,
		orgID, key, statuses[rng.Intn(len(statuses))],
		fmt.Sprintf("evt-%x", rng.Int63()), "BackupJob", severities[rng.Intn(len(severities))],
		"Succeeded", eventTypes[rng.Intn(len(eventTypes))],
		fmt.Sprintf("obj-%x", rng.Int63()), "TestObject", objectTypes[rng.Intn(len(objectTypes))],
		time.Now().Unix(), int64(rng.Intn(10)),
	)
}

func (db *cockroachDB) insertJobInstance(ctx context.Context, key string, rng *rand.Rand) error {
	jobTypes := []string{"BACKUP", "RESTORE", "REPLICATION", "CLOUD_ARCHIVAL"}
	statuses := []string{"QUEUED", "RUNNING", "SUCCEEDED", "FAILED"}
	jt := jobTypes[rng.Intn(len(jobTypes))]
	st := statuses[rng.Intn(len(statuses))]
	instanceID := rng.Int63n(5)
	startTime := time.Now().Add(-time.Duration(rng.Int63n(int64(24 * time.Hour)))).Unix()
	query := `INSERT INTO sd.job_instance (token__job_id, job_id, instance_id, job_type_2, status, start_time) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`
	return db.execQuery(ctx, query, hashToken(key), key, instanceID, jt, st, startTime)
}

func (db *cockroachDB) insertReportStats(ctx context.Context, key string, rng *rand.Rand) error {
	query := `INSERT INTO sd.report_stats (token__month_object_id, month_object_id, creation_time, local) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`
	return db.execQuery(ctx, query, hashToken(key), key, derivedTime(key), fmt.Sprintf("%d", rng.Int63n(1<<40)))
}

// --------------------------------------------------------------------------
// Delete — 0.20% of operations
// --------------------------------------------------------------------------

func (db *cockroachDB) Delete(ctx context.Context, table string, key string) error {
	state := ctx.Value(stateKey).(*cockroachState)
	switch pickTable(runWeights) {
	case tableFiles:
		query := `DELETE FROM sd.files WHERE token__uuid = $1 AND uuid = $2 AND stripe_id = -1`
		return db.execQuery(ctx, query, hashToken(key), key)
	case tableEvent:
		query := `DELETE FROM sd.event WHERE event_id = $1`
		return db.execQuery(ctx, query, key)
	case tableEventSeries:
		query := `DELETE FROM sd.event_series WHERE organization_id = $1 AND event_series_id = $2`
		return db.execQuery(ctx, query, orgIDs[state.rng.Intn(len(orgIDs))], key)
	case tableJobInstance:
		query := `DELETE FROM sd.job_instance WHERE token__job_id = $1 AND job_id = $2`
		return db.execQuery(ctx, query, hashToken(key), key)
	default:
		query := `DELETE FROM sd.event WHERE event_id = $1`
		return db.execQuery(ctx, query, key)
	}
}

func init() {
	ycsb.RegisterDBCreator("rubrik_cockroachdb", cockroachCreator{})
}
