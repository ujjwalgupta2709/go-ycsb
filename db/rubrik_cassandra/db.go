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
// Uses a minimal raw CQL v4 QUERY-frame client (no gocql) because upstream
// gocql always sends PREPARE + EXECUTE, but cqlproxy returns a dummy empty
// RESULT to PREPARE, which gocql cannot parse. The raw client sends QUERY
// frames directly, matching the pattern used by Rubrik's own scaledata/gocql
// fork (SkipPrepStmt=true).
//
// Tables and operation mix:
//
//	Run weights:  files=89%, report_stats=11%
//	Load weights: files=95%, report_stats=5%
//	Op mix:       75% SELECT (point lookup), 20% SCAN (range), 2% INSERT, 2% UPDATE, 1% DELETE
//
// Scan routing:
//
//	100% files — clustering key range scan (MDS all-stripes-for-a-file pattern)
package rubrik_cassandra

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
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
)

// CQL v4 protocol constants

const (
	cqlProtoVersion = byte(0x04) // CQL protocol v4 (request direction)

	// Opcodes (request)
	cqlOpStartup      = byte(0x01)
	cqlOpQuery        = byte(0x07)
	cqlOpAuthResponse = byte(0x0F)

	// Opcodes (response)
	cqlOpReady        = byte(0x02)
	cqlOpAuthenticate = byte(0x03)
	cqlOpResult       = byte(0x08)
	cqlOpError        = byte(0x00)
	cqlOpAuthSuccess  = byte(0x10)

	// Consistency: LOCAL_ONE
	cqlConsistencyLocalOne = uint16(10)

	// CQL v4 frame header is always 9 bytes:
	//   version(1) + flags(1) + stream(2) + opcode(1) + length(4)
	cqlFrameHeaderSize = 9
)

// minConn is a single TCP connection that speaks CQL v4.
// It only supports QUERY frames (no PREPARE / EXECUTE).
// Not safe for concurrent use — each YCSB worker thread gets its own minConn.
type minConn struct {
	conn net.Conn
}

func dialMinConn(host string, port int, username, password string) (*minConn, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	c, err := net.DialTimeout("tcp", addr, 15*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial cqlproxy %s: %w", addr, err)
	}
	mc := &minConn{conn: c}
	if err := mc.startup(username, password); err != nil {
		c.Close()
		return nil, fmt.Errorf("cqlproxy startup at %s: %w", addr, err)
	}
	return mc, nil
}

func (mc *minConn) close() { mc.conn.Close() }

// buildFrame constructs a CQL v4 request frame.
func buildFrame(opcode byte, stream int16, body []byte) []byte {
	buf := make([]byte, cqlFrameHeaderSize+len(body))
	buf[0] = cqlProtoVersion
	buf[1] = 0x00 // flags
	buf[2] = byte(stream >> 8)
	buf[3] = byte(stream)
	buf[4] = opcode
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(body)))
	copy(buf[9:], body)
	return buf
}

// appendString appends [short len][bytes] (CQL "string" type) to buf.
func appendString(buf []byte, s string) []byte {
	l := len(s)
	buf = append(buf, byte(l>>8), byte(l))
	return append(buf, s...)
}

// appendLongString appends [int len][bytes] (CQL "long string" type) to buf.
func appendLongString(buf []byte, s string) []byte {
	l := len(s)
	buf = append(buf, byte(l>>24), byte(l>>16), byte(l>>8), byte(l))
	return append(buf, s...)
}

// startup performs the STARTUP → READY/AUTHENTICATE handshake.
// If the server requires authentication, sends AUTH_RESPONSE with credentials.
func (mc *minConn) startup(username, password string) error {
	// Encode string map: {"CQL_VERSION": "3.4.0"}
	var body []byte
	body = append(body, 0x00, 0x01) // [short] 1 entry
	body = appendString(body, "CQL_VERSION")
	body = appendString(body, "3.4.0")

	if _, err := mc.conn.Write(buildFrame(cqlOpStartup, 1, body)); err != nil {
		return err
	}
	opcode, _, err := mc.readFrame()
	if err != nil {
		return err
	}
	if opcode == cqlOpReady {
		return nil
	}
	if opcode == cqlOpAuthenticate {
		return mc.authenticate(username, password)
	}
	return fmt.Errorf("expected READY (0x%02x) or AUTHENTICATE (0x%02x), got 0x%02x",
		cqlOpReady, cqlOpAuthenticate, opcode)
}

// authenticate sends AUTH_RESPONSE with SASL PLAIN credentials: \0username\0password
func (mc *minConn) authenticate(username, password string) error {
	// SASL PLAIN: \0username\0password
	token := make([]byte, 0, 2+len(username)+len(password))
	token = append(token, 0x00)
	token = append(token, []byte(username)...)
	token = append(token, 0x00)
	token = append(token, []byte(password)...)

	// AUTH_RESPONSE body: [int len][bytes token]
	var body []byte
	l := len(token)
	body = append(body, byte(l>>24), byte(l>>16), byte(l>>8), byte(l))
	body = append(body, token...)

	if _, err := mc.conn.Write(buildFrame(cqlOpAuthResponse, 1, body)); err != nil {
		return err
	}
	opcode, respBody, err := mc.readFrame()
	if err != nil {
		return err
	}
	if opcode == cqlOpAuthSuccess || opcode == cqlOpReady {
		return nil
	}
	if opcode == cqlOpError {
		return parseCQLError(respBody)
	}
	return fmt.Errorf("auth: expected AUTH_SUCCESS (0x%02x), got 0x%02x", cqlOpAuthSuccess, opcode)
}

// readFrame reads one CQL v4 response frame and returns (opcode, body, error).
func (mc *minConn) readFrame() (byte, []byte, error) {
	var hdr [cqlFrameHeaderSize]byte
	if _, err := io.ReadFull(mc.conn, hdr[:]); err != nil {
		return 0, nil, fmt.Errorf("read frame header: %w", err)
	}
	opcode := hdr[4]
	bodyLen := int(binary.BigEndian.Uint32(hdr[5:9]))
	if bodyLen < 0 || bodyLen > 32*1024*1024 {
		return 0, nil, fmt.Errorf("implausible frame body length %d", bodyLen)
	}
	body := make([]byte, bodyLen)
	if bodyLen > 0 {
		if _, err := io.ReadFull(mc.conn, body); err != nil {
			return 0, nil, fmt.Errorf("read frame body: %w", err)
		}
	}
	return opcode, body, nil
}

// execQuery sends a CQL QUERY frame for the given statement and reads the
// response. On an ERROR opcode it returns the server's error message.
func (mc *minConn) execQuery(stream int16, q string) error {
	// Body: [long string] query + [short] consistency + [byte] flags=0
	var body []byte
	body = appendLongString(body, q)
	body = append(body, byte(cqlConsistencyLocalOne>>8), byte(cqlConsistencyLocalOne))
	body = append(body, 0x00) // flags = 0 (no bound values, no paging, etc.)

	if _, err := mc.conn.Write(buildFrame(cqlOpQuery, stream, body)); err != nil {
		return err
	}
	opcode, respBody, err := mc.readFrame()
	if err != nil {
		return err
	}
	if opcode == cqlOpError {
		return parseCQLError(respBody)
	}
	if opcode != cqlOpResult {
		return fmt.Errorf("unexpected CQL opcode 0x%02x", opcode)
	}
	return nil
}

// parseCQLError decodes a CQL v4 ERROR body: [int] code + [string] message.
func parseCQLError(body []byte) error {
	if len(body) < 4 {
		return fmt.Errorf("CQL ERROR (truncated body)")
	}
	code := binary.BigEndian.Uint32(body[0:4])
	msg := ""
	if len(body) >= 6 {
		msgLen := int(binary.BigEndian.Uint16(body[4:6]))
		if len(body) >= 6+msgLen {
			msg = string(body[6 : 6+msgLen])
		}
	}
	return fmt.Errorf("CQL error 0x%04x: %s", code, msg)
}

type queryStats struct {
	total      atomic.Int64
	success    atomic.Int64
	failed     atomic.Int64
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
					"[STATS] total=%d ok=%d fail=%d avg_ms=%.1f max_ms=%.1f\n",
					total, s.success.Load(), s.failed.Load(), avgMs, maxMs,
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

const (
	cassandraHosts            = "cassandra.hosts"
	cassandraPort             = "cassandra.port"
	cassandraLoadMode         = "cassandra.load_mode"
	cassandraLoadTargetTables = "cassandra.load_target_tables"
	cassandraUsername         = "cassandra.username"
	cassandraPassword         = "cassandra.password"
)

// sd.event and sd.event_series are native SQL tables in CockroachDB —
// cqlproxy rejects CQL queries against them with "cannot run queries on
// native SQL table". Only the three CQL-managed tables are accessible.
const (
	tableFiles       = "files"
	tableReportStats = "report_stats"
)

type tableWeight struct {
	name      string
	threshold float64 // cumulative upper bound in [0,1)
}

// runWeights: files=89%, report_stats=11%
var runWeights = []tableWeight{
	{tableFiles, 0.89},
	{tableReportStats, 1.00},
}

// loadWeights: files=95%, report_stats=5%
var loadWeights = []tableWeight{
	{tableFiles, 0.95},
	{tableReportStats, 1.00},
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

func randHex16(rng *rand.Rand) string {
	var buf [8]byte
	for i := range buf {
		buf[i] = byte(rng.Intn(256))
	}
	return hex.EncodeToString(buf[:])
}

func randUUID4(rng *rand.Rand) string {
	var u [16]byte
	for i := range u {
		u[i] = byte(rng.Intn(256))
	}
	u[6] = (u[6] & 0x0f) | 0x40
	u[8] = (u[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}

func genDirectorySpec(rng *rand.Rand) string {
	sdfsUUID := randUUID4(rng)
	numStripes := 1 + rng.Intn(3)
	version := 1 + rng.Intn(2)
	chunkSize := 67108864
	return fmt.Sprintf(
		`{"1":{"i32":%d},"2":{"lst":["str",0]},"3":{"rec":{"1":{"i32":1},"2":{"i32":%d},"3":{"i32":%d},"4":{"i32":%d},"5":{"i32":0},"6":{"i32":0},"7":{"i32":0},"8":{"i32":%d},"9":{"lst":["str",1,"%s"]}}},"4":{"tf":%d},"5":{"str":""},"6":{"tf":0},"7":{"i64":0},"8":{"map":["str","rec",0,{}]},"9":{"str":""},"10":{"tf":1},"12":{"tf":0},"13":{"tf":0},"14":{"rec":{"1":{"tf":0},"2":{"i64":0}}},"15":{"tf":1}}`,
		1+rng.Intn(3), numStripes, version, chunkSize, rng.Intn(5), sdfsUUID, rng.Intn(2),
	)
}

func genParentMap(rng *rand.Rand, parentHint string) string {
	names := []string{
		fmt.Sprintf("data_%d.avro", rng.Intn(100)),
		randHex16(rng),
		fmt.Sprintf("%s_%s", randUUID4(rng), randUUID4(rng)),
	}
	name := names[rng.Intn(len(names))]
	entryType := 1 + rng.Intn(2)
	return fmt.Sprintf(
		`{"1":{"map":["str","map",1,{"%s":["str","i32",1,{"%s":%d}]}]}}`,
		parentHint, name, entryType,
	)
}

// cqlHexBlob formats a byte slice as a CQL hex blob literal (0x...).
// Returns "null" for nil slices.
func cqlHexBlob(b []byte) string {
	if b == nil {
		return "null"
	}
	return "0x" + hex.EncodeToString(b)
}

func genChildMap(rng *rand.Rand) []byte {
	r := rng.Float64()
	if r < 0.60 {
		return nil
	}
	if r < 0.70 {
		return []byte{0x00, 0x00, 0x00, 0x00}
	}
	numEntries := 1 + rng.Intn(5)
	size := 4 + numEntries*((16+4+16+4)*2)
	buf := make([]byte, size)
	buf[0], buf[1], buf[2], buf[3] = 0x00, 0x00, 0x00, byte(numEntries)
	for i := 4; i < len(buf); i++ {
		buf[i] = byte(rng.Intn(256))
	}
	return buf
}

type cassandraCreator struct{}

func init() { ycsb.RegisterDBCreator("rubrik_cassandra", cassandraCreator{}) }

type cassandraDB struct {
	hosts            []string
	port             int
	username         string
	password         string
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
	conn     *minConn
	stream   int16
	hll      *hyperloglog.Sketch
	threadID int
	ops      int64
}

func (s *cassandraState) nextStream() int16 {
	s.stream++
	if s.stream <= 0 {
		s.stream = 1
	}
	return s.stream
}

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
	username := p.GetString(cassandraUsername, "")
	password := p.GetString(cassandraPassword, "")

	d := &cassandraDB{
		hosts:     hosts,
		port:      port,
		username:  username,
		password:  password,
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
			"[STATS] total=%d ok=%d fail=%d avg_ms=%.1f max_ms=%.1f\n",
		total, db.stats.success.Load(), db.stats.failed.Load(), avgMs, maxMs,
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

	return nil
}

func (db *cassandraDB) InitThread(ctx context.Context, threadID int, _ int) context.Context {
	host := db.hosts[rand.Intn(len(db.hosts))]
	conn, err := dialMinConn(host, db.port, db.username, db.password)
	if err != nil {
		fmt.Printf("ERROR: thread %d failed to connect to cqlproxy: %v\n", threadID, err)
		return context.WithValue(ctx, stateKey, &cassandraState{
			rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
			hll:      hyperloglog.New16(),
			threadID: threadID,
		})
	}
	return context.WithValue(ctx, stateKey, &cassandraState{
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
		conn:     conn,
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
		if s.conn != nil {
			s.conn.close()
		}
	}
}

func (db *cassandraDB) runCQL(s *cassandraState, q string) error {
	if s.conn == nil {
		return fmt.Errorf("no cqlproxy connection for this thread")
	}
	if db.verbose {
		fmt.Printf("CQL %s\n", q)
	}
	start := time.Now()
	err := s.conn.execQuery(s.nextStream(), q)
	db.stats.record(time.Since(start), err)
	if err != nil && db.verbose {
		fmt.Printf("ERR %s → %v\n", q, err)
	}
	return err
}

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

func (db *cassandraDB) Read(ctx context.Context, _ string, key string, _ []string) (map[string][]byte, error) {
	s := ctx.Value(stateKey).(*cassandraState)
	s.hll.Insert([]byte(key))
	s.ops++
	var q string
	switch pickTable(s.rng, runWeights) {
	case tableFiles:
		q = fmt.Sprintf(`SELECT * FROM sd.files WHERE uuid = %s AND stripe_id = -1 LIMIT 1`, cqlStr(key))
	default:
		q = fmt.Sprintf(`SELECT * FROM sd.report_stats WHERE month_object_id = %s LIMIT 1`, cqlStr(key))
	}
	if err := db.runCQL(s, q); err != nil {
		return nil, err
	}
	return map[string][]byte{"key": []byte(key)}, nil
}

func (db *cassandraDB) Scan(ctx context.Context, _ string, startKey string, count int, _ []string) ([]map[string][]byte, error) {
	s := ctx.Value(stateKey).(*cassandraState)
	if count <= 0 {
		count = 10
	}
	// Clustering key range scan — all stripes for one uuid.
	q := fmt.Sprintf(
		`SELECT * FROM sd.files WHERE uuid = %s AND stripe_id >= 0 LIMIT %d`,
		cqlStr(startKey), count,
	)
	if err := db.runCQL(s, q); err != nil {
		return nil, err
	}
	return nil, nil // rows discarded — we care about QPS/latency, not data
}

func (db *cassandraDB) Update(ctx context.Context, _ string, key string, _ map[string][]byte) error {
	s := ctx.Value(stateKey).(*cassandraState)
	// All updates route to files (report_stats PK includes creation_time, making updates impractical).
	q := fmt.Sprintf(
		`UPDATE sd.files SET parent_uuid_hint = %s WHERE uuid = %s AND stripe_id = -1`,
		cqlStr(fmt.Sprintf("parent-%x", s.rng.Int63())), cqlStr(key),
	)
	return db.runCQL(s, q)
}

func (db *cassandraDB) Insert(ctx context.Context, _ string, key string, _ map[string][]byte) error {
	s := ctx.Value(stateKey).(*cassandraState)
	weights := runWeights
	if db.loadMode {
		weights = db.effectiveLoadWeights()
	}
	switch pickTable(s.rng, weights) {
	case tableFiles:
		return db.insertFiles(s, key)
	default:
		return db.insertReportStats(s, key)
	}
}

func (db *cassandraDB) insertFiles(s *cassandraState, key string) error {
	rng := s.rng
	parentHint := randHex16(rng)
	birthTime := time.Now().Unix() - rng.Int63n(30*24*3600)
	directorySpec := genDirectorySpec(rng)
	parentMap := genParentMap(rng, parentHint)
	childMap := genChildMap(rng)

	openHeartbeat := "null"
	if rng.Float64() < 0.20 {
		openHeartbeat = fmt.Sprintf("%d", birthTime+rng.Int63n(86400))
	}

	q := fmt.Sprintf(
		`INSERT INTO sd.files (uuid, stripe_id, birth_time, child_map, directory_spec, lock, open_heartbeat_time, parent_map, parent_uuid_hint, stripe_metadata, symlink_target) VALUES (%s, -1, %d, %s, %s, null, %s, %s, %s, null, null)`,
		cqlStr(key), birthTime, cqlHexBlob(childMap), cqlStr(directorySpec),
		openHeartbeat, cqlStr(parentMap), cqlStr(parentHint),
	)
	return db.runCQL(s, q)
}

func (db *cassandraDB) insertReportStats(s *cassandraState, key string) error {
	rng := s.rng
	baseTime := time.Now().Add(-time.Duration(rng.Int63n(int64(180 * 24 * time.Hour))))
	creationTime := baseTime.UTC().Format("2006-01-02T15:04:05+0000")

	logicalBytes := rng.Int63n(100) * 1073741824
	ingestedBytes := rng.Int63n(50) * 1073741824
	exclusivePhys := rng.Int63n(30) * 1073741824
	sharedPhys := rng.Int63n(20) * 1073741824
	lastSnapLogical := rng.Int63n(100) * 1073741824
	provisionedOpt := int64(1+rng.Intn(20)) * 1099511627776

	local := fmt.Sprintf(
		`{"logicalBytes":%d,"ingestedBytes":%d,"exclusivePhysicalBytes":%d,"sharedPhysicalBytes":%d,"indexStorageBytes":0,"lastSnapshotLogicalBytes":%d,"cdpLogStorageBytes":0,"cdpLocalThroughput":0.0,"totalIngestedBytes":%d,"preCrossPreCompBytes":0,"fairnessPhysicalBytes":0,"provisionedBytesOpt":%d}`,
		logicalBytes, ingestedBytes, exclusivePhys, sharedPhys,
		lastSnapLogical, ingestedBytes, provisionedOpt,
	)

	archivalBytes := rng.Int63n(10) * 1073741824
	var archival string
	if rng.Float64() < 0.80 {
		archival = fmt.Sprintf(`{"archivalBytes":%d,"ingestedBytes":%d,"logicalBytes":%d}`,
			archivalBytes, archivalBytes/2, archivalBytes)
	} else {
		archival = `{"perLocationStats":{}}`
	}

	q := fmt.Sprintf(
		`INSERT INTO sd.report_stats (month_object_id, creation_time, local, archival) VALUES (%s, %s, %s, %s)`,
		cqlStr(key), cqlStr(creationTime), cqlStr(local), cqlStr(archival),
	)
	return db.runCQL(s, q)
}

func (db *cassandraDB) Delete(ctx context.Context, _ string, key string) error {
	s := ctx.Value(stateKey).(*cassandraState)
	q := fmt.Sprintf(`DELETE FROM sd.files WHERE uuid = %s AND stripe_id = -1`, cqlStr(key))
	return db.runCQL(s, q)
}
