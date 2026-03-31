// Package rubrik_cockroachdb implements a go-ycsb DB driver that issues realistic
// queries against the four main production CockroachDB tables in Rubrik CDM:
//   - sd.files        (SDFS file/dir metadata; Atlas MDS point-lookups by uuid)
//   - sd.event        (job/workload events)
//   - sd.event_series (aggregated event series per org)
//   - sd.report_stats (per-object monthly storage stats)
//
// Run-phase table weights (% of ops): files=60, event=22, event_series=14,
// report_stats=4.
// Load-phase table weights (% of inserts): files=82, event=12, event_series=4,
// report_stats=2. Enable with crdb.load_mode=true.
package rubrik_cockroachdb

import (
	"context"
	"database/sql"
	"encoding/hex"
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
	crdbHost             = "crdb.host"
	crdbHosts            = "crdb.hosts"
	crdbPort             = "crdb.port"
	crdbUser             = "crdb.user"
	crdbPassword         = "crdb.password"
	crdbDBName           = "crdb.db"
	crdbSSLMode          = "crdb.sslmode"
	crdbLoadMode         = "crdb.load_mode"
	crdbLoadTargetTables = "crdb.load_target_tables"
)

// table name constants
const (
	tableFiles       = "sd.files"
	tableEvent       = "sd.event"
	tableEventSeries = "sd.event_series"
	tableReportStats = "sd.report_stats"
)

// tableWeight defines cumulative upper-bound thresholds for table selection.
type tableWeight struct {
	name      string
	threshold float64 // cumulative upper bound in [0,1)
}

// runWeights: files=60%, event=22%, event_series=14%, report_stats=4%
var runWeights = []tableWeight{
	{tableFiles, 0.60},
	{tableEvent, 0.82},
	{tableEventSeries, 0.96},
	{tableReportStats, 1.00},
}

// loadWeights: files=82%, event=12%, event_series=4%, report_stats=2%
var loadWeights = []tableWeight{
	{tableFiles, 0.82},
	{tableEvent, 0.94},
	{tableEventSeries, 0.98},
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

// randHex16 returns a 16-character lowercase hex string (8 random bytes).
func randHex16(rng *rand.Rand) string {
	var buf [8]byte
	for i := range buf {
		buf[i] = byte(rng.Intn(256))
	}
	return hex.EncodeToString(buf[:])
}

// randUUID4 returns a random UUID v4 string.
func randUUID4(rng *rand.Rand) string {
	var u [16]byte
	for i := range u {
		u[i] = byte(rng.Intn(256))
	}
	u[6] = (u[6] & 0x0f) | 0x40
	u[8] = (u[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}

// genDirectorySpec builds a Thrift-style JSON blob matching production
// sd.files.directory_spec rows (~400-600 bytes). The structure mirrors
// the real DirectorySpec thrift struct used by MDS.
func genDirectorySpec(rng *rand.Rand) string {
	sdfsUUID := randUUID4(rng)
	// Field 3 sub-fields vary slightly: num_stripes 1-3, version 1-2
	numStripes := 1 + rng.Intn(3)
	version := 1 + rng.Intn(2)
	chunkSize := 67108864 // 64 MiB, standard

	spec := fmt.Sprintf(
		`{"1":{"i32":%d},"2":{"lst":["str",0]},"3":{"rec":{"1":{"i32":1},"2":{"i32":%d},"3":{"i32":%d},"4":{"i32":%d},"5":{"i32":0},"6":{"i32":0},"7":{"i32":0},"8":{"i32":%d},"9":{"lst":["str",1,"%s"]}}},"4":{"tf":%d},"5":{"str":""},"6":{"tf":0},"7":{"i64":0},"8":{"map":["str","rec",0,{}]},"9":{"str":""},"10":{"tf":1},"12":{"tf":0},"13":{"tf":0},"14":{"rec":{"1":{"tf":0},"2":{"i64":0}}},"15":{"tf":1}}`,
		1+rng.Intn(3), // dir type: 1=file, 2=dir, 3=symlink
		numStripes,
		version,
		chunkSize,
		rng.Intn(5),
		sdfsUUID,
		rng.Intn(2),
	)
	return spec
}

// genParentMap builds a Thrift-style JSON blob matching production
// sd.files.parent_map rows (~100-200 bytes).
func genParentMap(rng *rand.Rand, parentHint string) string {
	// parent entry name: either a hex hash, a filename, or a UUID-style path
	names := []string{
		fmt.Sprintf("data_%d.avro", rng.Intn(100)),
		randHex16(rng),
		fmt.Sprintf("%s_%s", randUUID4(rng), randUUID4(rng)),
	}
	name := names[rng.Intn(len(names))]
	entryType := 1 + rng.Intn(2) // 1=file, 2=dir

	return fmt.Sprintf(
		`{"1":{"map":["str","map",1,{"%s":["str","i32",1,{"%s":%d}]}]}}`,
		parentHint,
		name,
		entryType,
	)
}

// genChildMap builds a binary blob matching production sd.files.child_map.
// ~60% of rows have NULL child_map, ~10% have empty (4 zero bytes),
// ~30% have real entries (variable length 50-500 bytes).
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
	buf[0] = 0x00
	buf[1] = 0x00
	buf[2] = 0x00
	buf[3] = byte(numEntries)
	// Fill rest with random data to simulate serialized thrift entries
	for i := 4; i < len(buf); i++ {
		buf[i] = byte(rng.Intn(256))
	}
	return buf
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
	pools            []*sql.DB
	counter          atomic.Uint64
	verbose          bool
	loadMode         bool
	loadTargetTables map[string]bool
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

func (db *cockroachDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	state := ctx.Value(stateKey).(*cockroachState)
	switch pickTable(state.rng, runWeights) {
	case tableFiles:
		return db.readFiles(ctx, key)
	case tableEvent:
		return db.readEvent(ctx, key)
	case tableEventSeries:
		return db.readEventSeries(ctx, orgIDs[state.rng.Intn(len(orgIDs))], key)
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

func (db *cockroachDB) readReportStats(ctx context.Context, key string) (map[string][]byte, error) {
	query := `SELECT token__month_object_id, month_object_id, creation_time, local FROM sd.report_stats WHERE token__month_object_id = $1 AND month_object_id = $2 LIMIT 1`
	return db.querySingleRow(ctx, query, hashToken(key), key)
}

func (db *cockroachDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, nil
}

func (db *cockroachDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	state := ctx.Value(stateKey).(*cockroachState)
	switch pickTable(state.rng, runWeights) {
	case tableFiles:
		query := `UPDATE sd.files SET open_heartbeat_time = $1 WHERE token__uuid = $2 AND uuid = $3 AND stripe_id = -1`
		return db.execQuery(ctx, query, time.Now().UnixNano(), hashToken(key), key)
	case tableEvent:
		severities := []string{"Informational", "Warning", "Critical"}
		sev := severities[state.rng.Intn(len(severities))]
		query := `UPDATE sd.event SET event_severity = $1 WHERE event_id = $2`
		return db.execQuery(ctx, query, sev, key)
	case tableEventSeries:
		statuses := []string{"Active", "Resolved"}
		st := statuses[state.rng.Intn(len(statuses))]
		query := `UPDATE sd.event_series SET event_series_status = $1 WHERE organization_id = $2 AND event_series_id = $3`
		return db.execQuery(ctx, query, st, orgIDs[state.rng.Intn(len(orgIDs))], key)
	default:
		query := `UPDATE sd.report_stats SET local = $1 WHERE token__month_object_id = $2 AND month_object_id = $3 AND creation_time = $4`
		return db.execQuery(ctx, query, fmt.Sprintf("%d", state.rng.Int63()), hashToken(key), key, derivedTime(key))
	}
}

func (db *cockroachDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	state := ctx.Value(stateKey).(*cockroachState)
	weights := runWeights
	if db.loadMode {
		weights = db.effectiveLoadWeights()
	}
	switch pickTable(state.rng, weights) {
	case tableFiles:
		return db.insertFiles(ctx, key, state.rng)
	case tableEvent:
		return db.insertEvent(ctx, key, state.rng)
	case tableEventSeries:
		return db.insertEventSeries(ctx, key, state.rng)
	default:
		return db.insertReportStats(ctx, key, state.rng)
	}
}

func (db *cockroachDB) insertFiles(ctx context.Context, key string, rng *rand.Rand) error {
	parentHint := randHex16(rng)
	birthTime := time.Now().Unix() - rng.Int63n(30*24*3600) // within last 30 days
	directorySpec := genDirectorySpec(rng)
	parentMap := genParentMap(rng, parentHint)
	childMap := genChildMap(rng)

	// open_heartbeat_time: ~20% of rows have it set
	var openHeartbeat interface{}
	if rng.Float64() < 0.20 {
		openHeartbeat = birthTime + rng.Int63n(86400)
	}

	query := `INSERT INTO sd.files (
		token__uuid, uuid, stripe_id, birth_time,
		child_map, directory_spec, lock,
		open_heartbeat_time, parent_map,
		parent_uuid_hint, stripe_metadata, symlink_target
	) VALUES ($1,$2,-1,$3,$4,$5,NULL,$6,$7,$8,NULL,NULL) ON CONFLICT DO NOTHING`

	return db.execQuery(ctx, query,
		hashToken(key), key, birthTime,
		childMap, directorySpec,
		openHeartbeat, parentMap,
		parentHint,
	)
}

func (db *cockroachDB) insertEvent(ctx context.Context, key string, rng *rand.Rand) error {
	type eventTemplate struct {
		eventType  string
		eventName  string
		objectType string
		msgTmpl    string
		msgID      string
	}
	templates := []eventTemplate{
		{"LogBackup", "Oracle.LogDeletionStart", "OracleDb",
			"The log backup is complete and available for recovery. Running post-backup actions for Oracle Database '%s' on the hosts: %s.",
			"Oracle.LogDeletionStart"},
		{"LogBackup", "Oracle.LogDeletionSuccess", "OracleDb",
			"Successfully deleted the archived logs that have been backed-up for Oracle Database '%s' with unique database name '%s' from host %s.",
			"Oracle.LogDeletionSuccess"},
		{"LogBackup", "Oracle.EndSnapshotPreparation", "OracleDb",
			"Finished preparation for log backup of Oracle Database '%s'. Created %d channels.",
			"Oracle.EndSnapshotPreparation"},
		{"Backup", "Fileset.FilesetDataFetchFinished", "LinuxFileset",
			"Successfully found %d files in '%s' from '%s'. A total of %d MB were fetched in %d minutes, %d seconds.",
			"Fileset.FilesetDataFetchFinished"},
		{"Backup", "VirtualMachine.ExportSnapshotComplete", "VirtualMachine",
			"Successfully completed snapshot export for '%s'. Transferred %d MB in %d seconds.",
			"VirtualMachine.ExportSnapshotComplete"},
		{"Backup", "Mssql.BackupCompleted", "Mssql",
			"Completed backup of SQL Server database '%s' on host '%s'. Duration: %d minutes.",
			"Mssql.BackupCompleted"},
		{"Restore", "VirtualMachine.RestoreCompleted", "VirtualMachine",
			"Successfully restored virtual machine '%s' to host '%s'.",
			"VirtualMachine.RestoreCompleted"},
		{"Replication", "Replication.SnapshotTransferred", "VirtualMachine",
			"Snapshot for '%s' successfully transferred to target cluster. Size: %d MB.",
			"Replication.SnapshotTransferred"},
	}
	t := templates[rng.Intn(len(templates))]

	objectID := randUUID4(rng)
	objectNames := []string{
		fmt.Sprintf("pn%clo%c%dc", 'a'+byte(rng.Intn(26)), 'a'+byte(rng.Intn(26)), rng.Intn(9)),
		fmt.Sprintf("db-%s-%02d", []string{"prod", "stg", "qa"}[rng.Intn(3)], rng.Intn(20)),
		fmt.Sprintf("LOCAL MOUNT %s BACKUP", []string{"PROD", "DEV", "QA"}[rng.Intn(3)]),
		fmt.Sprintf("vm-%s-%03d", []string{"app", "web", "svc"}[rng.Intn(3)], rng.Intn(200)),
	}
	objectName := objectNames[rng.Intn(len(objectNames))]
	hostName := fmt.Sprintf("host-%s-%02d.corp.example.com", []string{"prod", "qa", "stg"}[rng.Intn(3)], rng.Intn(50))

	// Build realistic event_info JSON (~200-600 bytes like production)
	var message string
	switch t.msgID {
	case "Oracle.LogDeletionStart":
		message = fmt.Sprintf(t.msgTmpl, objectName, hostName)
	case "Oracle.LogDeletionSuccess":
		message = fmt.Sprintf(t.msgTmpl, objectName, objectName, hostName)
	case "Oracle.EndSnapshotPreparation":
		message = fmt.Sprintf(t.msgTmpl, objectName, 2+rng.Intn(10))
	case "Fileset.FilesetDataFetchFinished":
		message = fmt.Sprintf(t.msgTmpl, 1000+rng.Intn(25000000), objectName, hostName, 10+rng.Intn(5000), 1+rng.Intn(30), rng.Intn(60))
	default:
		message = fmt.Sprintf(t.msgTmpl, objectName, hostName, 1+rng.Intn(500))
	}
	eventInfo := fmt.Sprintf(`{"message":"%s","id":"%s","params":{}}`, message, t.msgID)

	severities := []string{"Informational", "Warning", "Critical"}
	sevWeights := []float64{0.85, 0.10, 0.05}
	r := rng.Float64()
	sev := severities[0]
	cum := 0.0
	for i, w := range sevWeights {
		cum += w
		if r < cum {
			sev = severities[i]
			break
		}
	}

	statuses := []string{"Success", "TaskSuccess", "Running", "Failure", "Warning"}
	stWeights := []float64{0.40, 0.30, 0.15, 0.10, 0.05}
	r = rng.Float64()
	st := statuses[0]
	cum = 0.0
	for i, w := range stWeights {
		cum += w
		if r < cum {
			st = statuses[i]
			break
		}
	}

	// job_instance_id: CREATE_<TYPE>_SNAPSHOT_<uuid>:::<N>
	snapshotPrefixes := []string{
		"CREATE_ORACLE_LOG_SNAPSHOT",
		"CREATE_FILESET_SNAPSHOT",
		"CREATE_VM_SNAPSHOT",
		"CREATE_MSSQL_SNAPSHOT",
	}
	jobInstanceID := fmt.Sprintf("%s_%s:::%d", snapshotPrefixes[rng.Intn(len(snapshotPrefixes))], objectID, 1+rng.Intn(2000))

	// managed_ids: ["<ObjectType>:::<uuid>"]
	managedIDs := fmt.Sprintf(`["%s:::%s"]`, t.objectType, objectID)

	// time in milliseconds (13-digit)
	timeMs := time.Now().UnixMilli() - rng.Int63n(30*24*3600*1000)

	query := `INSERT INTO sd.event (
		event_id, event_info, event_name, event_series_id,
		event_severity, event_status, event_type,
		job_instance_id, managed_ids, object_id, object_name,
		object_type, time, notification_status
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) ON CONFLICT DO NOTHING`
	return db.execQuery(ctx, query,
		key,
		eventInfo,
		t.eventName,
		randUUID4(rng),
		sev,
		st,
		t.eventType,
		jobInstanceID,
		managedIDs,
		objectID,
		objectName,
		t.objectType,
		timeMs,
		"None",
	)
}

func (db *cockroachDB) insertEventSeries(ctx context.Context, key string, rng *rand.Rand) error {
	orgID := orgIDs[rng.Intn(len(orgIDs))]

	// Production event name + type combos
	type esTemplate struct {
		eventName  string
		eventType  string
		objectType string
		jobPrefix  string // for jobInstanceIds key in event_series_stats
	}
	templates := []esTemplate{
		{"Hawkeye.IndexOperationOnLocationCompleted", "Index", "LinuxFileset", "INDEX_SNAPPABLE_SNAPSHOTS"},
		{"Snapshot.LogBackupSucceeded", "LogBackup", "OracleDb", "CREATE_ORACLE_LOG_SNAPSHOT"},
		{"Oracle.BackupSucceededAndLogBackupQueued", "Backup", "OracleDb", "CREATE_ORACLE_SNAPSHOT"},
		{"Fileset.FilesetDataFetchFinished", "Backup", "LinuxFileset", "CREATE_FILESET_SNAPSHOT"},
		{"VirtualMachine.SnapshotCompleted", "Backup", "VirtualMachine", "CREATE_VM_SNAPSHOT"},
		{"Mssql.BackupCompleted", "Backup", "Mssql", "CREATE_MSSQL_SNAPSHOT"},
	}
	t := templates[rng.Intn(len(templates))]

	objectID := randUUID4(rng)
	objectNames := []string{
		fmt.Sprintf("ANF %s %s %s", []string{"ALPHA", "ECHO", "DELTA"}[rng.Intn(3)],
			[]string{"PROD", "QA", "QA2"}[rng.Intn(3)],
			[]string{"OLTP App", "OLTP DB", "DW"}[rng.Intn(3)]),
		fmt.Sprintf("pn%c%c%c%02dc", 'a'+byte(rng.Intn(26)), 'a'+byte(rng.Intn(26)), 'a'+byte(rng.Intn(26)), rng.Intn(9)),
		fmt.Sprintf("vm-%s-%03d", []string{"app", "web", "svc"}[rng.Intn(3)], rng.Intn(200)),
	}
	objectName := objectNames[rng.Intn(len(objectNames))]

	// event_series_stats: large JSON blob (~300-600 bytes) matching production
	clusterIDs := []string{"VRAZ29C921CA6", "VRAZ2BFC98AAA", "VRAZ13D3789DB", "VRAZ988374FBC"}
	instanceNum := 1 + rng.Intn(50000)
	nowMs := time.Now().UnixMilli()
	startedMs := nowMs - rng.Int63n(7200*1000)
	endMs := startedMs + rng.Int63n(3600*1000)
	lastUpdateMs := endMs + rng.Int63n(1000)
	slaID := randUUID4(rng)
	logicalSize := int64(1+rng.Intn(10)) * 1073741824
	objLogicalSize := int64(10+rng.Intn(5000)) * 1073741824

	// Some series have dataTransferred + ingestionDuration (backup/logbackup), some don't (index)
	var extraFields string
	if t.eventType != "Index" {
		dataTransferred := logicalSize / (1 + rng.Int63n(3))
		ingestionMs := 5000 + rng.Int63n(800000)
		extraFields = fmt.Sprintf(`,"logicalSizeOpt":%d,"dataTransferredOpt":%d,"ingestionDurationInMillis":%d`,
			logicalSize, dataTransferred, ingestionMs)
	}

	stats := fmt.Sprintf(
		`{"eventSeriesId":"%s","jobInstanceIds":[{"%s_%s":%d}],"startedRunningTimes":{"%d":{"millis":%d}},"lastUpdateTime":{"millis":%d},"endTimeOpt":{"millis":%d},"slaIdOpt":"%s"%s,"nodeIds":["cluster:::%s"],"retries":0,"objectLogicalSizeOpt":%d,"isFirstFullSnapshot":false,"hasWarningEvent":false,"taskType":"","objectManagedId":""}`,
		key,
		t.jobPrefix, objectID, instanceNum,
		instanceNum, startedMs,
		lastUpdateMs,
		endMs,
		slaID,
		extraFields,
		clusterIDs[rng.Intn(len(clusterIDs))],
		objLogicalSize,
	)

	// Status distribution: predominantly Success
	statuses := []string{"Success", "Failure", "Warning", "Canceled"}
	stWeights := []float64{0.85, 0.08, 0.05, 0.02}
	r := rng.Float64()
	st := statuses[0]
	cum := 0.0
	for i, w := range stWeights {
		cum += w
		if r < cum {
			st = statuses[i]
			break
		}
	}

	query := `INSERT INTO sd.event_series (
		organization_id, event_series_id, event_series_stats,
		event_series_status,
		latest_event_id, latest_event_name, latest_event_severity,
		latest_event_status, latest_event_type,
		latest_object_id, latest_object_name, latest_object_type,
		latest_time, warning_count
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) ON CONFLICT DO NOTHING`
	return db.execQuery(ctx, query,
		orgID, key, stats,
		st,
		randUUID4(rng), t.eventName, "Informational",
		st, t.eventType,
		objectID, objectName, t.objectType,
		lastUpdateMs, int64(0),
	)
}

func (db *cockroachDB) insertReportStats(ctx context.Context, key string, rng *rand.Rand) error {
	// creation_time: ISO 8601 format like 2025-10-09T05:13:55+0000
	baseTime := time.Now().Add(-time.Duration(rng.Int63n(int64(180 * 24 * time.Hour))))
	creationTime := baseTime.UTC().Format("2006-01-02T15:04:05+0000")

	// local: JSON blob (~200-400 bytes) with storage stats
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

	// archival: JSON blob (~50-80 bytes)
	archivalBytes := rng.Int63n(10) * 1073741824
	var archival string
	if rng.Float64() < 0.80 {
		archival = fmt.Sprintf(`{"archivalBytes":%d,"ingestedBytes":%d,"logicalBytes":%d}`,
			archivalBytes, archivalBytes/2, archivalBytes)
	} else {
		archival = `{"perLocationStats":{}}`
	}

	// Use key as month_object_id (keeps read/update/delete paths working)
	// but creationTime and local/archival give production-realistic row width
	query := `INSERT INTO sd.report_stats (
		token__month_object_id, month_object_id, creation_time,
		local, archival
	) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`
	return db.execQuery(ctx, query,
		hashToken(key), key, creationTime,
		local, archival,
	)
}

func (db *cockroachDB) Delete(ctx context.Context, table string, key string) error {
	state := ctx.Value(stateKey).(*cockroachState)
	switch pickTable(state.rng, runWeights) {
	case tableFiles:
		query := `DELETE FROM sd.files WHERE token__uuid = $1 AND uuid = $2 AND stripe_id = -1`
		return db.execQuery(ctx, query, hashToken(key), key)
	case tableEvent:
		query := `DELETE FROM sd.event WHERE event_id = $1`
		return db.execQuery(ctx, query, key)
	case tableEventSeries:
		query := `DELETE FROM sd.event_series WHERE organization_id = $1 AND event_series_id = $2`
		return db.execQuery(ctx, query, orgIDs[state.rng.Intn(len(orgIDs))], key)
	default:
		query := `DELETE FROM sd.report_stats WHERE token__month_object_id = $1 AND month_object_id = $2 AND creation_time = $3`
		return db.execQuery(ctx, query, hashToken(key), key, derivedTime(key))
	}
}

func init() {
	ycsb.RegisterDBCreator("rubrik_cockroachdb", cockroachCreator{})
}
