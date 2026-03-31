// Package rubrik_cassandra_gocql implements a go-ycsb DB driver that issues CQL
// queries against cqlproxy (CAMS) using Rubrik's scaledata/gocql fork with
// SkipPrepStmt=true. This exercises the full CAMS stack (cqlproxy → CockroachDB)
// with proper connection pooling, retries, and reconnection.
//
// Why scaledata/gocql instead of upstream gocql:
//   - cqlproxy returns a dummy empty RESULT to PREPARE (no column metadata)
//   - upstream gocql always sends PREPARE+EXECUTE, which breaks on that empty response
//   - scaledata/gocql adds SkipPrepStmt=true, which sends QUERY frames directly
//
// Tables and operation mix (same as rubrik_cassandra):
//
//	Run weights:  files=89%, report_stats=11%
//	Load weights: files=95%, report_stats=5%
package rubrik_cassandra_gocql

import (
	"context"
	"encoding/hex"
	"fmt"
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
	"github.com/scaledata/gocql"
)

// Configuration property keys.
const (
	cassandraHosts            = "cassandra.hosts"
	cassandraPort             = "cassandra.port"
	cassandraConnections      = "cassandra.connections"
	cassandraUsername          = "cassandra.username"
	cassandraPassword         = "cassandra.password"
	cassandraLoadMode         = "cassandra.load_mode"
	cassandraLoadTargetTables = "cassandra.load_target_tables"
	cassandraTimeout          = "cassandra.timeout"
	cassandraConnectTimeout   = "cassandra.connect_timeout"
)

// Defaults.
const (
	defaultPort           = 27577 // cqlproxy port
	defaultConnections    = 64
	defaultTimeout        = 30 // seconds
	defaultConnectTimeout = 30 // seconds
)

// Production tables accessible through cqlproxy.
// sd.event and sd.event_series are native SQL tables — cqlproxy rejects
// CQL queries against them.
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
// WARNING: This is a benchmark-only pattern. Production code must use
// parameterized queries (bound variables) to prevent CQL injection.
// We use inline values here because cqlproxy requires raw QUERY frames
// (SkipPrepStmt=true) which don't support bound parameters.
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


// ProxyAddressTranslator maps all discovered Cassandra node addresses back to
// the cqlproxy address. Without this, gocql tries to connect directly to nodes
// discovered via system.peers.
type ProxyAddressTranslator struct {
	proxyIP   net.IP
	proxyPort int
}

func NewProxyAddressTranslator(proxyHost string, proxyPort int) (*ProxyAddressTranslator, error) {
	proxyIP := net.ParseIP(proxyHost)
	if proxyIP == nil {
		ips, err := net.LookupIP(proxyHost)
		if err != nil || len(ips) == 0 {
			return nil, fmt.Errorf("failed to resolve proxy host %q: %v", proxyHost, err)
		}
		proxyIP = ips[0]
	}
	return &ProxyAddressTranslator{proxyIP: proxyIP, proxyPort: proxyPort}, nil
}

func (t *ProxyAddressTranslator) Translate(addr net.IP, port int) (net.IP, int) {
	return t.proxyIP, t.proxyPort
}

// tolerantConvictionPolicy never marks a host as down. cqlproxy is a single
// endpoint fronting multiple pods — marking it down would kill all traffic.
type tolerantConvictionPolicy struct{}

func (t *tolerantConvictionPolicy) AddFailure(err error, host *gocql.HostInfo) bool {
	return false
}

func (t *tolerantConvictionPolicy) Reset(host *gocql.HostInfo) {}

// queryStats tracks latency and error counts using lock-free atomics.
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

type cassandraCreator struct{}

func init() { ycsb.RegisterDBCreator("rubrik_cassandra_gocql", cassandraCreator{}) }

type cassandraDB struct {
	session          *gocql.Session
	verbose          bool
	loadMode         bool
	loadTargetTables map[string]bool
	stats            *queryStats
	totalOps         atomic.Int64
	globalHLL        *hyperloglog.Sketch
	hllMu            sync.Mutex
}

type contextKey string

const stateKey = contextKey("cassandraGocqlState")

type cassandraState struct {
	rng      *rand.Rand
	hll      *hyperloglog.Sketch
	threadID int
	ops      int64
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

	port := p.GetInt(cassandraPort, defaultPort)
	numConns := p.GetInt(cassandraConnections, defaultConnections)
	timeoutSecs := p.GetInt(cassandraTimeout, defaultTimeout)
	connectTimeoutSecs := p.GetInt(cassandraConnectTimeout, defaultConnectTimeout)
	username := p.GetString(cassandraUsername, "")
	password := p.GetString(cassandraPassword, "")

	cluster := gocql.NewCluster(hosts...)
	cluster.Port = port
	cluster.NumConns = numConns
	cluster.Timeout = time.Duration(timeoutSecs) * time.Second
	cluster.ConnectTimeout = time.Duration(connectTimeoutSecs) * time.Second
	cluster.Consistency = gocql.One
	cluster.ProtoVersion = 4

	// SkipPrepStmt is the key setting: cqlproxy returns a dummy empty RESULT
	// to PREPARE and does not handle EXECUTE at all. This flag makes gocql
	// send QUERY frames directly, bypassing the PREPARE+EXECUTE flow.
	cluster.SkipPrepStmt = true

	// Prevent gocql from querying system.peers and trying to connect
	// directly to backend CockroachDB nodes.
	cluster.DisableInitialHostLookup = true
	cluster.IgnorePeerAddr = true
	addrTranslator, err := NewProxyAddressTranslator(hosts[0], port)
	if err != nil {
		return nil, fmt.Errorf("proxy address translator: %w", err)
	}
	cluster.AddressTranslator = addrTranslator

	// Never mark the proxy host as down — it fronts multiple pods.
	cluster.ConvictionPolicy = &tolerantConvictionPolicy{}

	// Retry and reconnection settings for resilience under load.
	cluster.NumExecuteRetries = 100
	cluster.ReconnectInterval = 100 * time.Millisecond
	cluster.MaxRetryTime = 1 * time.Minute
	cluster.ReconnectionPolicy = &gocql.ExponentialReconnectionPolicy{
		MaxRetries:      10,
		InitialInterval: 10 * time.Millisecond,
	}

	// Authentication — enable if credentials are provided.
	if username != "" && password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
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

	d.stats.startReporter(10 * time.Second)
	return d, nil
}

func (db *cassandraDB) Close() error {
	db.stats.stop()

	ts := time.Now().Format("20060102_150405")
	totalOps := db.totalOps.Load()
	uniqueKeys := db.globalHLL.Estimate()

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

	logFile := fmt.Sprintf("hll_summary_%s.txt", ts)
	logContent := fmt.Sprintf("timestamp: %s\ntotal_ops: %d\nestimated_unique_keys: %d\n",
		ts, totalOps, uniqueKeys)
	if err := os.WriteFile(logFile, []byte(logContent), 0644); err != nil {
		fmt.Printf("[HLL] WARNING: failed to write log file: %v\n", err)
	} else {
		fmt.Printf("[HLL] summary saved to %s\n", logFile)
	}

	if db.session != nil {
		db.session.Close()
	}
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

func (db *cassandraDB) execCQL(ctx context.Context, q string) error {
	if db.verbose {
		fmt.Printf("CQL %s\n", q)
	}
	start := time.Now()
	err := db.session.Query(q).WithContext(ctx).Exec()
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
	if err := db.execCQL(ctx, q); err != nil {
		return nil, err
	}
	return map[string][]byte{"key": []byte(key)}, nil
}

func (db *cassandraDB) Scan(ctx context.Context, _ string, startKey string, count int, _ []string) ([]map[string][]byte, error) {
	s := ctx.Value(stateKey).(*cassandraState)
	s.hll.Insert([]byte(startKey))
	s.ops++
	if count <= 0 {
		count = 10
	}
	// Scan for stripe_id >= -1 so we hit the rows we actually inserted
	// (all inserts use stripe_id = -1). This exercises a real CQL range scan.
	q := fmt.Sprintf(
		`SELECT * FROM sd.files WHERE uuid = %s AND stripe_id >= -1 LIMIT %d`,
		cqlStr(startKey), count,
	)
	if err := db.execCQL(ctx, q); err != nil {
		return nil, err
	}
	return nil, nil
}

func (db *cassandraDB) Update(ctx context.Context, _ string, key string, _ map[string][]byte) error {
	s := ctx.Value(stateKey).(*cassandraState)
	s.hll.Insert([]byte(key))
	s.ops++
	// Use INSERT (upsert) instead of UPDATE to work around a cqlproxy bug
	// where UPDATE statements cause "pq: SSL is not enabled on the server".
	// NOTE: This CQL INSERT is an upsert that overwrites all unspecified columns
	// to their defaults/nulls. This changes the data profile for subsequent reads
	// but is acceptable for QPS/latency benchmarking.
	q := fmt.Sprintf(
		`INSERT INTO sd.files (uuid, stripe_id, parent_uuid_hint) VALUES (%s, -1, %s)`,
		cqlStr(key), cqlStr(fmt.Sprintf("parent-%x", s.rng.Int63())),
	)
	return db.execCQL(ctx, q)
}

func (db *cassandraDB) Insert(ctx context.Context, _ string, key string, _ map[string][]byte) error {
	s := ctx.Value(stateKey).(*cassandraState)
	weights := runWeights
	if db.loadMode {
		weights = db.effectiveLoadWeights()
	}
	switch pickTable(s.rng, weights) {
	case tableFiles:
		return db.insertFiles(ctx, s, key)
	default:
		return db.insertReportStats(ctx, s, key)
	}
}

func (db *cassandraDB) insertFiles(ctx context.Context, s *cassandraState, key string) error {
	rng := s.rng
	parentHint := randHex16(rng)
	birthTime := time.Now().Unix() - rng.Int63n(30*24*3600)
	directorySpec := genDirectorySpec(rng)
	parentMap := genParentMap(rng, parentHint)

	openHeartbeat := "null"
	if rng.Float64() < 0.20 {
		openHeartbeat = fmt.Sprintf("%d", birthTime+rng.Int63n(86400))
	}

	// child_map is always null here: cqlproxy's CQL lexer cannot parse hex
	// blob literals (0x...) in inline QUERY frames. The raw CQL driver had
	// the same issue but didn't retry, masking the failures. For benchmarking
	// purposes (QPS/latency measurement) this has no meaningful impact.
	q := fmt.Sprintf(
		`INSERT INTO sd.files (uuid, stripe_id, birth_time, child_map, directory_spec, lock, open_heartbeat_time, parent_map, parent_uuid_hint, stripe_metadata, symlink_target) VALUES (%s, -1, %d, null, %s, null, %s, %s, %s, null, null)`,
		cqlStr(key), birthTime, cqlStr(directorySpec),
		openHeartbeat, cqlStr(parentMap), cqlStr(parentHint),
	)
	return db.execCQL(ctx, q)
}

func (db *cassandraDB) insertReportStats(ctx context.Context, s *cassandraState, key string) error {
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
	return db.execCQL(ctx, q)
}

func (db *cassandraDB) Delete(ctx context.Context, _ string, key string) error {
	s := ctx.Value(stateKey).(*cassandraState)
	s.hll.Insert([]byte(key))
	s.ops++
	q := fmt.Sprintf(`DELETE FROM sd.files WHERE uuid = %s AND stripe_id = -1`, cqlStr(key))
	return db.execCQL(ctx, q)
}
