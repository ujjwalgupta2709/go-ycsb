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

// Package rubrik_cassandra implements a go-ycsb DB driver that issues CQL
// queries against cqlproxy (CAMS), which translates them to SQL for
// CockroachDB. This exercises the full CAMS stack instead of bypassing
// cqlproxy and hitting CockroachDB directly.
//
// Uses scaledata/gocql (Rubrik's gocql fork) with SkipPrepStmt=true so that
// QUERY frames are sent directly (no PREPARE + EXECUTE), matching the pattern
// required by cqlproxy.
//
// Tables and operation mix:
//
//	Run weights:  files=82%, job_instance=12%, report_stats=6%
//	Load weights: files=95%, job_instance=3%, report_stats=2%
//	Op mix:       75% SELECT (point lookup), 20% SCAN (range), 2% INSERT, 2% UPDATE, 1% DELETE
//
// Scan routing (across 2 table patterns):
//
//	63% job_instance — token range scan (JFL job discovery pattern)
//	37% files        — clustering key range scan (MDS all-stripes-for-a-file pattern)
package rubrik_cassandra

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/axiomhq/hyperloglog"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/gocql/gocql"
)

// ─── gocql helpers (from Swapnil's fork) ──────────────────────────────────────

// ProxyAddressTranslator redirects all discovered Cassandra node addresses
// to the cqlproxy address(es). Without this, gocql would try to connect
// directly to the Cassandra nodes (e.g. 127.0.0.1) instead of the proxy.
type ProxyAddressTranslator struct {
	proxyAddr net.IP
	proxyPort int
}

func (t *ProxyAddressTranslator) Translate(addr net.IP, port int) (net.IP, int) {
	return t.proxyAddr, t.proxyPort
}

// tolerantConvictionPolicy never marks a host as down. Since cqlproxy fronts
// multiple CockroachDB pods behind a single address, convicting the proxy
// host would be fatal.
type tolerantConvictionPolicy struct{}

func (p *tolerantConvictionPolicy) AddFailure(err error, host *gocql.HostInfo) bool { return false }
func (p *tolerantConvictionPolicy) Reset(host *gocql.HostInfo)                      {}

// gocqlLogger implements gocql.StdLogger for verbose CQL logging.
type gocqlLogger struct{}

func (l gocqlLogger) Print(v ...interface{})                 { log.Print(v...) }
func (l gocqlLogger) Printf(format string, v ...interface{}) { log.Printf(format, v...) }
func (l gocqlLogger) Println(v ...interface{})               { log.Println(v...) }

// ─── Query statistics (from Swapnil's fork) ───────────────────────────────────

type queryStats struct {
	total      atomic.Int64
	success    atomic.Int64
	failed     atomic.Int64
	noHosts    atomic.Int64
	timeouts   atomic.Int64
	totalLatNs atomic.Int64
	maxLatNs   atomic.Int64
	stopCh     chan struct{}
}

func newQueryStats() *queryStats {
	return &queryStats{stopCh: make(chan struct{})}
}

func (s *queryStats) record(dur time.Duration, err error) {
	ns := dur.Nanoseconds()
	s.total.Add(1)
	s.totalLatNs.Add(ns)
	// Update max atomically (CAS loop).
	for {
		cur := s.maxLatNs.Load()
		if ns <= cur || s.maxLatNs.CompareAndSwap(cur, ns) {
			break
		}
	}
	if err == nil {
		s.success.Add(1)
		return
	}
	s.failed.Add(1)
	errMsg := err.Error()
	if strings.Contains(errMsg, "no hosts available") {
		s.noHosts.Add(1)
	}
	if strings.Contains(errMsg, "timeout") || strings.Contains(errMsg, "i/o timeout") {
		s.timeouts.Add(1)
	}
}

func (s *queryStats) startReporter(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				total := s.total.Load()
				if total == 0 {
					continue
				}
				avgMs := float64(s.totalLatNs.Load()) / float64(total) / 1e6
				maxMs := float64(s.maxLatNs.Load()) / 1e6
				fmt.Fprintf(os.Stderr,
					"[STATS] total=%d ok=%d fail=%d no_hosts=%d timeouts=%d avg_ms=%.1f max_ms=%.1f\n",
					total, s.success.Load(), s.failed.Load(),
					s.noHosts.Load(), s.timeouts.Load(), avgMs, maxMs,
				)
			case <-s.stopCh:
				return
			}
		}
	}()
}

func (s *queryStats) stop() {
	close(s.stopCh)
}

// ─── Driver config keys ───────────────────────────────────────────────────────

const (
	cassandraHosts            = "cassandra.hosts"
	cassandraPort             = "cassandra.port"
	cassandraLoadMode         = "cassandra.load_mode"
	cassandraLoadTargetTables = "cassandra.load_target_tables"
	cassandraDebug            = "cassandra.debug"
	cassandraTimeout          = "cassandra.timeout"
	cassandraConnectTimeout   = "cassandra.connect_timeout"
	cassandraNumConns         = "cassandra.num_conns"
)

// ─── Table names ──────────────────────────────────────────────────────────────

// sd.event and sd.event_series are native SQL tables in CockroachDB —
// cqlproxy rejects CQL queries against them with "cannot run queries on
// native SQL table". Only the three CQL-managed tables are accessible.
const (
	tableFiles       = "files"
	tableJobInstance = "job_instance"
	tableReportStats = "report_stats"
)

// ─── Table routing weights ────────────────────────────────────────────────────

type tableWeight struct {
	name      string
	threshold float64 // cumulative upper bound in [0,1)
}

// runWeights: files=82%, job_instance=12%, report_stats=6%
var runWeights = []tableWeight{
	{tableFiles, 0.82},
	{tableJobInstance, 0.94},
	{tableReportStats, 1.00},
}

// loadWeights: files=95%, job_instance=3%, report_stats=2%
var loadWeights = []tableWeight{
	{tableFiles, 0.95},
	{tableJobInstance, 0.98},
	{tableReportStats, 1.00},
}

// scanWeights: job_instance=63%, files=37%
var scanWeights = []tableWeight{
	{tableJobInstance, 0.63},
	{tableFiles, 1.00},
}

func pickTable(rng *rand.Rand, weights []tableWeight) string {
	r := rng.Float64()
	for _, w := range weights {
		if r < w.threshold {
			return w.name
		}
	}
	return weights[len(weights)-1].name
}

// cqlStr quotes a string value for safe inline embedding in CQL.
// Single quotes are escaped by doubling them (standard CQL escaping).
func cqlStr(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// noPrepare prefixes a CQL statement with a block comment so that gocql's
// shouldPrepare() logic (which checks the first word) sees "/*" instead of
// a DML keyword and sends a raw QUERY frame rather than PREPARE + EXECUTE.
// cqlproxy does not handle EXECUTE opcodes, so all queries must be sent as
// QUERY frames. The comment is transparent to the CQL parser.
func noPrepare(q string) string {
	return "/* */ " + q
}

// ─── Driver structs ───────────────────────────────────────────────────────────

type cassandraCreator struct{}

func init() { ycsb.RegisterDBCreator("rubrik_cassandra", cassandraCreator{}) }

type cassandraDB struct {
	session          *gocql.Session
	verbose          bool
	loadMode         bool
	loadTargetTables map[string]bool // nil = all tables
	stats            *queryStats
	totalOps         atomic.Int64
	globalHLL        *hyperloglog.Sketch
	hllMu            sync.Mutex
}

type contextKey string

const stateKey = contextKey("cassandraDB")

type cassandraState struct {
	rng      *rand.Rand
	hll      *hyperloglog.Sketch
	threadID int
	ops      int64
}

// ─── Creator ──────────────────────────────────────────────────────────────────

func (c cassandraCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	hostsStr := p.GetString(cassandraHosts, "127.0.0.1")
	var hosts []string
	for _, h := range strings.Split(hostsStr, ",") {
		if trimmed := strings.TrimSpace(h); trimmed != "" {
			hosts = append(hosts, trimmed)
		}
	}
	if len(hosts) == 0 {
		hosts = []string{"127.0.0.1"}
	}

	port := p.GetInt(cassandraPort, 27577)
	debug := p.GetBool(cassandraDebug, false)
	timeout := p.GetInt(cassandraTimeout, 30)
	connectTimeout := p.GetInt(cassandraConnectTimeout, 30)
	numConns := p.GetInt(cassandraNumConns, 64)

	// Set up gocql cluster targeting cqlproxy.
	cluster := gocql.NewCluster(hosts...)
	cluster.Port = port
	cluster.Consistency = gocql.One
	cluster.ProtoVersion = 4
	cluster.NumConns = numConns
	cluster.Timeout = time.Duration(timeout) * time.Second
	cluster.ConnectTimeout = time.Duration(connectTimeout) * time.Second
	cluster.DisableInitialHostLookup = true
	cluster.IgnorePeerAddr = true
	cluster.PageSize = 10000
	cluster.ConvictionPolicy = &tolerantConvictionPolicy{}
	cluster.ReconnectionPolicy = &gocql.ExponentialReconnectionPolicy{
		MaxRetries:      10,
		InitialInterval: 10 * time.Millisecond,
	}
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
		gocql.RoundRobinHostPolicy(),
	)

	// Redirect any discovered addresses back to the proxy.
	cluster.AddressTranslator = &ProxyAddressTranslator{
		proxyAddr: net.ParseIP(hosts[0]),
		proxyPort: port,
	}

	if debug {
		gocql.Logger = gocqlLogger{}
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("gocql session creation failed: %w", err)
	}

	d := &cassandraDB{
		session:   session,
		verbose:   p.GetBool(prop.Verbose, prop.VerboseDefault),
		loadMode:  p.GetBool(cassandraLoadMode, false),
		stats:     newQueryStats(),
		globalHLL: hyperloglog.New16(),
	}
	if tables := p.GetString(cassandraLoadTargetTables, ""); tables != "" {
		d.loadTargetTables = make(map[string]bool)
		for _, t := range strings.Split(tables, ",") {
			if trimmed := strings.TrimSpace(t); trimmed != "" {
				d.loadTargetTables[trimmed] = true
			}
		}
	}

	// Start periodic stats reporter (every 10s).
	d.stats.startReporter(10 * time.Second)

	return d, nil
}

func (db *cassandraDB) Close() error {
	db.stats.stop()

	ts := time.Now().Format("20060102_150405")
	totalOps := db.totalOps.Load()
	uniqueKeys := db.globalHLL.Estimate()

	// Final stats summary.
	total := db.stats.total.Load()
	var avgMs, maxMs float64
	if total > 0 {
		avgMs = float64(db.stats.totalLatNs.Load()) / float64(total) / 1e6
		maxMs = float64(db.stats.maxLatNs.Load()) / 1e6
	}
	fmt.Fprintf(os.Stderr,
		"\n[STATS] === FINAL ===\n"+
			"[STATS] total=%d ok=%d fail=%d no_hosts=%d timeouts=%d avg_ms=%.1f max_ms=%.1f\n",
		total, db.stats.success.Load(), db.stats.failed.Load(),
		db.stats.noHosts.Load(), db.stats.timeouts.Load(), avgMs, maxMs,
	)

	summary := fmt.Sprintf("[HLL] === NODE TOTAL ===\n"+
		"[HLL] total_ops=%d  estimated_unique_keys=%d\n",
		totalOps, uniqueKeys)
	fmt.Print("\n" + summary)

	// Serialize HLL sketch to file for cross-node merging.
	data, err := db.globalHLL.MarshalBinary()
	if err != nil {
		fmt.Printf("[HLL] WARNING: failed to marshal sketch: %v\n", err)
	} else {
		sketchFile := fmt.Sprintf("hll_sketch_%s.bin", ts)
		if err := os.WriteFile(sketchFile, data, 0644); err != nil {
			fmt.Printf("[HLL] WARNING: failed to write sketch file: %v\n", err)
		} else {
			fmt.Printf("[HLL] sketch saved to %s (%d bytes)\n", sketchFile, len(data))
		}
	}

	// Write human-readable summary.
	logFile := fmt.Sprintf("hll_summary_%s.txt", ts)
	logContent := fmt.Sprintf("timestamp: %s\ntotal_ops: %d\nestimated_unique_keys: %d\n",
		ts, totalOps, uniqueKeys)
	if err := os.WriteFile(logFile, []byte(logContent), 0644); err != nil {
		fmt.Printf("[HLL] WARNING: failed to write log file: %v\n", err)
	} else {
		fmt.Printf("[HLL] summary saved to %s\n", logFile)
	}

	db.session.Close()
	return nil
}

func (db *cassandraDB) InitThread(ctx context.Context, threadID int, _ int) context.Context {
	return context.WithValue(ctx, stateKey, &cassandraState{
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
		hll:      hyperloglog.New16(),
		threadID: threadID,
	})
}

func (db *cassandraDB) CleanupThread(ctx context.Context) {
	if s, ok := ctx.Value(stateKey).(*cassandraState); ok {
		fmt.Printf("[HLL] thread=%d  ops=%d  estimated_unique_keys=%d\n",
			s.threadID, s.ops, s.hll.Estimate())
		db.totalOps.Add(s.ops)
		db.hllMu.Lock()
		db.globalHLL.Merge(s.hll)
		db.hllMu.Unlock()
	}
}

// ─── Core query executor ─────────────────────────────────────────────────────

func (db *cassandraDB) runCQL(s *cassandraState, q string) error {
	if db.verbose {
		fmt.Printf("CQL %s\n", q)
	}
	start := time.Now()
	err := db.session.Query(noPrepare(q)).Exec()
	db.stats.record(time.Since(start), err)
	return err
}

// runCQLRead executes a SELECT and discards rows (we measure latency, not data).
func (db *cassandraDB) runCQLRead(s *cassandraState, q string) error {
	if db.verbose {
		fmt.Printf("CQL %s\n", q)
	}
	start := time.Now()
	iter := db.session.Query(noPrepare(q)).Iter()
	err := iter.Close()
	db.stats.record(time.Since(start), err)
	return err
}

// ─── effectiveLoadWeights ────────────────────────────────────────────────────

func (db *cassandraDB) effectiveLoadWeights() []tableWeight {
	if db.loadTargetTables == nil {
		return loadWeights
	}
	prev := 0.0
	deltas := make([]float64, len(loadWeights))
	for i, w := range loadWeights {
		deltas[i] = w.threshold - prev
		prev = w.threshold
	}
	var total float64
	for i, w := range loadWeights {
		if db.loadTargetTables[w.name] {
			total += deltas[i]
		}
	}
	if total == 0 {
		return loadWeights
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

// ─── Read (75%) ───────────────────────────────────────────────────────────────

func (db *cassandraDB) Read(ctx context.Context, _ string, key string, _ []string) (map[string][]byte, error) {
	s := ctx.Value(stateKey).(*cassandraState)
	s.hll.Insert([]byte(key))
	s.ops++
	var q string
	switch pickTable(s.rng, runWeights) {
	case tableFiles:
		q = fmt.Sprintf(`SELECT * FROM sd.files WHERE uuid = %s AND stripe_id = -1 LIMIT 1`, cqlStr(key))
	case tableJobInstance:
		q = fmt.Sprintf(`SELECT * FROM sd.job_instance WHERE job_id = %s LIMIT 1`, cqlStr(key))
	default:
		q = fmt.Sprintf(`SELECT * FROM sd.report_stats WHERE month_object_id = %s LIMIT 1`, cqlStr(key))
	}
	if err := db.runCQLRead(s, q); err != nil {
		return nil, err
	}
	return map[string][]byte{"key": []byte(key)}, nil
}

// ─── Scan (20%) ───────────────────────────────────────────────────────────────

func (db *cassandraDB) Scan(ctx context.Context, _ string, startKey string, count int, _ []string) ([]map[string][]byte, error) {
	s := ctx.Value(stateKey).(*cassandraState)
	if count <= 0 {
		count = 10
	}
	var q string
	switch pickTable(s.rng, scanWeights) {
	case tableJobInstance:
		// Token range scan — mirrors JFL job discovery pattern.
		q = fmt.Sprintf(
			`SELECT * FROM sd.job_instance WHERE token(job_id) >= token(%s) LIMIT %d`,
			cqlStr(startKey), count,
		)
	default:
		// Clustering key range scan — all stripes for one uuid.
		q = fmt.Sprintf(
			`SELECT * FROM sd.files WHERE uuid = %s AND stripe_id >= 0 LIMIT %d`,
			cqlStr(startKey), count,
		)
	}
	if err := db.runCQLRead(s, q); err != nil {
		return nil, err
	}
	return nil, nil // rows discarded — we care about QPS/latency, not data
}

// ─── Update (2%) ──────────────────────────────────────────────────────────────

func (db *cassandraDB) Update(ctx context.Context, _ string, key string, _ map[string][]byte) error {
	s := ctx.Value(stateKey).(*cassandraState)
	var q string
	switch pickTable(s.rng, runWeights) {
	case tableFiles:
		q = fmt.Sprintf(
			`UPDATE sd.files SET parent_uuid_hint = %s WHERE uuid = %s AND stripe_id = -1`,
			cqlStr(fmt.Sprintf("parent-%x", s.rng.Int63())), cqlStr(key),
		)
	case tableJobInstance:
		statuses := []string{"QUEUED", "RUNNING", "SUCCEEDED", "FAILED"}
		q = fmt.Sprintf(
			`UPDATE sd.job_instance SET status = %s WHERE job_id = %s AND instance_id = %d`,
			cqlStr(statuses[s.rng.Intn(4)]), cqlStr(key), s.rng.Int63n(5),
		)
	default:
		// report_stats PK includes creation_time; route updates to files instead
		q = fmt.Sprintf(
			`UPDATE sd.files SET parent_uuid_hint = %s WHERE uuid = %s AND stripe_id = -1`,
			cqlStr(fmt.Sprintf("parent-%x", s.rng.Int63())), cqlStr(key),
		)
	}
	return db.runCQL(s, q)
}

// ─── Insert (2% run / 100% load) ─────────────────────────────────────────────

func (db *cassandraDB) Insert(ctx context.Context, _ string, key string, _ map[string][]byte) error {
	s := ctx.Value(stateKey).(*cassandraState)
	weights := runWeights
	if db.loadMode {
		weights = db.effectiveLoadWeights()
	}
	switch pickTable(s.rng, weights) {
	case tableFiles:
		return db.runCQL(s, fmt.Sprintf(
			`INSERT INTO sd.files (uuid, stripe_id, parent_uuid_hint, birth_time) VALUES (%s, -1, %s, %d)`,
			cqlStr(key),
			cqlStr(fmt.Sprintf("parent-%x", s.rng.Int63())),
			time.Now().UnixNano(),
		))
	case tableJobInstance:
		jobTypes := []string{"BACKUP", "RESTORE", "REPLICATION", "CLOUD_ARCHIVAL"}
		statuses := []string{"QUEUED", "RUNNING", "SUCCEEDED", "FAILED"}
		ts := time.Now().Format("2006-01-02 15:04:05.999-0700")
		return db.runCQL(s, fmt.Sprintf(
			`INSERT INTO sd.job_instance (job_id, instance_id, job_type_2, status, start_time) VALUES (%s, %d, %s, %s, %s)`,
			cqlStr(key),
			s.rng.Int63n(5),
			cqlStr(jobTypes[s.rng.Intn(4)]),
			cqlStr(statuses[s.rng.Intn(4)]),
			cqlStr(ts),
		))
	default:
		creationTime := time.Unix(int64(s.rng.Uint32())%int64(365*24*3600), 0).UTC().Format(time.RFC3339)
		return db.runCQL(s, fmt.Sprintf(
			`INSERT INTO sd.report_stats (month_object_id, creation_time, local) VALUES (%s, %s, '%d')`,
			cqlStr(key),
			cqlStr(creationTime),
			s.rng.Int63n(1<<40),
		))
	}
}

// ─── Delete (1%) ──────────────────────────────────────────────────────────────

func (db *cassandraDB) Delete(ctx context.Context, _ string, key string) error {
	s := ctx.Value(stateKey).(*cassandraState)
	var q string
	switch pickTable(s.rng, runWeights) {
	case tableFiles:
		q = fmt.Sprintf(`DELETE FROM sd.files WHERE uuid = %s AND stripe_id = -1`, cqlStr(key))
	case tableJobInstance:
		q = fmt.Sprintf(
			`DELETE FROM sd.job_instance WHERE job_id = %s AND instance_id = %d`,
			cqlStr(key), s.rng.Int63n(5),
		)
	default:
		// report_stats PK includes creation_time; route to files
		q = fmt.Sprintf(`DELETE FROM sd.files WHERE uuid = %s AND stripe_id = -1`, cqlStr(key))
	}
	return db.runCQL(s, q)
}
