package main

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

type Config struct {
	// SFTP
	SFTPHost       string
	SFTPPort       string
	SFTPUser       string
	SFTPPassword   string
	SFTPPrivateKey string
	SFTPWatchDir   string

	// ClickHouse
	CHHost     string
	CHPort     int
	CHDatabase string
	CHTable    string
	CHUser     string
	CHPassword string

	// Local
	LocalDir string

	// Processing
	PollInterval    time.Duration
	LineChannelSize int
	BatchSize       int
	NumWorkers      int
	ShutdownTimeout time.Duration // hard deadline for graceful drain
}

func loadConfig() Config {
	return Config{
		SFTPHost:       envOr("SFTP_HOST", "localhost"),
		SFTPPort:       envOr("SFTP_PORT", "22"),
		SFTPUser:       envOr("SFTP_USER", "user"),
		SFTPPassword:   envOr("SFTP_PASSWORD", ""),
		SFTPPrivateKey: envOr("SFTP_PRIVATE_KEY", ""),
		SFTPWatchDir:   envOr("SFTP_WATCH_DIR", "/upload"),

		CHHost:     envOr("CH_HOST", "localhost"),
		CHPort:     atoi(envOr("CH_PORT", "9000")),
		CHDatabase: envOr("CH_DATABASE", "default"),
		CHTable:    envOr("CH_TABLE", "zip_lines"),
		CHUser:     envOr("CH_USER", "default"),
		CHPassword: envOr("CH_PASSWORD", ""),

		LocalDir: envOr("LOCAL_DIR", "/tmp/sftp-zips"),

		PollInterval:    mustDuration(envOr("POLL_INTERVAL", "10s")),
		LineChannelSize: atoi(envOr("LINE_CHANNEL_SIZE", "2000")),
		BatchSize:       atoi(envOr("BATCH_SIZE", "1000")),
		NumWorkers:      atoi(envOr("NUM_WORKERS", "4")),
		ShutdownTimeout: mustDuration(envOr("SHUTDOWN_TIMEOUT", "30s")),
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func mustDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		log.Fatalf("bad duration %q: %v", s, err)
	}
	return d
}

func atoi(s string) int {
	var n int
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		log.Fatalf("bad int %q: %v", s, err)
	}
	return n
}

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

// LineRecord is a single JSON object destined for ClickHouse.
type LineRecord struct {
	ZipName  string
	FileName string
	LineNo   int
	Line     string
}

// ---------------------------------------------------------------------------
// ClickHouse (native protocol via clickhouse-go v2)
// ---------------------------------------------------------------------------

func newClickHouseConn(cfg Config) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.CHHost, cfg.CHPort)},
		Auth: clickhouse.Auth{
			Database: cfg.CHDatabase,
			Username: cfg.CHUser,
			Password: cfg.CHPassword,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		MaxOpenConns: cfg.NumWorkers + 2,
		MaxIdleConns: cfg.NumWorkers,
		DialTimeout:  10 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}
	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("clickhouse ping: %w", err)
	}
	return conn, nil
}

func ensureTable(ctx context.Context, conn driver.Conn, cfg Config) error {
	ddl := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			zip_name    String,
			file_name   String,
			line_no     UInt32,
			line        String,
			inserted_at DateTime DEFAULT now()
		) ENGINE = MergeTree()
		ORDER BY (zip_name, file_name, line_no)
	`, cfg.CHDatabase, cfg.CHTable)
	return conn.Exec(ctx, ddl)
}

func sendBatch(ctx context.Context, conn driver.Conn, cfg Config, rows []LineRecord) error {
	if len(rows) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx,
		fmt.Sprintf("INSERT INTO %s.%s (zip_name, file_name, line_no, line)", cfg.CHDatabase, cfg.CHTable))
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for i := range rows {
		if err := batch.Append(
			rows[i].ZipName,
			rows[i].FileName,
			uint32(rows[i].LineNo),
			rows[i].Line,
		); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("append row %d: %w", i, err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch (%d rows): %w", len(rows), err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// SFTP
// ---------------------------------------------------------------------------

func newSFTPClient(cfg Config) (*sftp.Client, *ssh.Client, error) {
	var auths []ssh.AuthMethod

	if cfg.SFTPPrivateKey != "" {
		key, err := os.ReadFile(cfg.SFTPPrivateKey)
		if err != nil {
			return nil, nil, fmt.Errorf("read key: %w", err)
		}
		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			return nil, nil, fmt.Errorf("parse key: %w", err)
		}
		auths = append(auths, ssh.PublicKeys(signer))
	}
	if cfg.SFTPPassword != "" {
		auths = append(auths, ssh.Password(cfg.SFTPPassword))
	}

	sshCfg := &ssh.ClientConfig{
		User:            cfg.SFTPUser,
		Auth:            auths,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         15 * time.Second,
	}

	addr := fmt.Sprintf("%s:%s", cfg.SFTPHost, cfg.SFTPPort)
	sshConn, err := ssh.Dial("tcp", addr, sshCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("ssh dial: %w", err)
	}

	sc, err := sftp.NewClient(sshConn)
	if err != nil {
		sshConn.Close()
		return nil, nil, fmt.Errorf("sftp new client: %w", err)
	}
	return sc, sshConn, nil
}

func downloadFile(sc *sftp.Client, remotePath, localPath string) error {
	remote, err := sc.Open(remotePath)
	if err != nil {
		return fmt.Errorf("sftp open: %w", err)
	}
	defer remote.Close()

	local, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("create local: %w", err)
	}
	defer local.Close()

	n, err := io.Copy(local, remote)
	if err != nil {
		os.Remove(localPath)
		return fmt.Errorf("copy: %w", err)
	}

	log.Printf("[download] %s → %s (%d bytes)", remotePath, localPath, n)
	return nil
}

// ---------------------------------------------------------------------------
// Pipeline Stage 1: Watch SFTP → download → zip.OpenReader → json.NewDecoder → lineCh
//
// Graceful shutdown strategy:
//   - stopCtx is cancelled by signal → watcher stops polling for NEW zips
//   - The current zip being processed is ALWAYS finished completely
//     (all files in the zip, all JSON objects in each file)
//   - Only after the current zip is fully drained does the watcher close lineCh
//   - hardCtx is a deadline that force-kills everything if drain takes too long
// ---------------------------------------------------------------------------

// shuttingDown is set to 1 when the signal is received. The watcher checks
// this between zips to decide whether to stop picking up new ones.
var shuttingDown atomic.Int32

func watchZips(ctx context.Context, hardCtx context.Context, cfg Config, sc *sftp.Client, lineCh chan<- LineRecord) {
	defer close(lineCh)

	if err := os.MkdirAll(cfg.LocalDir, 0o755); err != nil {
		log.Fatalf("[watch] create local dir %s: %v", cfg.LocalDir, err)
	}

	seen := make(map[string]struct{})
	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	poll := func() {
		entries, err := sc.ReadDir(cfg.SFTPWatchDir)
		if err != nil {
			log.Printf("[watch] readdir: %v", err)
			return
		}

		for _, e := range entries {
			// Between each zip: check whether we're shutting down.
			if shuttingDown.Load() == 1 {
				log.Println("[watch] shutdown requested, not picking up new zips")
				return
			}

			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(strings.ToLower(name), ".zip") {
				continue
			}
			if _, ok := seen[name]; ok {
				continue
			}
			log.Printf("[watch] new zip: %s", name)
			seen[name] = struct{}{}

			remotePath := filepath.Join(cfg.SFTPWatchDir, name)
			localPath := filepath.Join(cfg.LocalDir, name)

			if err := downloadFile(sc, remotePath, localPath); err != nil {
				log.Printf("[watch] download %s: %v", name, err)
				delete(seen, name)
				continue
			}
			log.Printf("[watch] downloaded %s → %s", remotePath, localPath)

			// Process the ENTIRE zip — all files, all JSON objects.
			// Uses hardCtx so it only aborts on the hard deadline.
			if err := extractLocalZip(hardCtx, localPath, name, lineCh); err != nil {
				log.Printf("[watch] extract %s: %v", name, err)
				delete(seen, name)
				os.Remove(localPath)
			}
		}
	}

	poll()
	for {
		select {
		case <-ctx.Done():
			log.Println("[watch] stop signal received, finishing current work...")
			// Do NOT poll for new zips. Any in-flight extractLocalZip has
			// already returned by now (it runs synchronously in poll()).
			// Close lineCh so workers know no more data is coming.
			return
		case <-ticker.C:
			poll()
		}
	}
}

// extractLocalZip opens a local zip with zip.OpenReader, then for each file
// calls f.Open() and streams JSON with json.NewDecoder / decoder.More().
//
// On graceful shutdown (hardCtx cancelled): the function will still try to
// finish the CURRENT file being decoded, then stop processing remaining files
// in the zip. This ensures no partial file is left half-decoded.
func extractLocalZip(ctx context.Context, localPath, zipName string, lineCh chan<- LineRecord) error {
	zr, err := zip.OpenReader(localPath)
	if err != nil {
		return fmt.Errorf("zip.OpenReader %s: %w", localPath, err)
	}
	defer zr.Close()

	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}

		// Between files: check hard deadline.
		select {
		case <-ctx.Done():
			log.Printf("[extract] hard deadline — stopping after partial zip %s (%s not processed)", zipName, f.Name)
			return ctx.Err()
		default:
		}

		file, err := f.Open()
		if err != nil {
			log.Printf("[extract] skip %s/%s: %v", zipName, f.Name, err)
			continue
		}

		// Decode the ENTIRE file — all JSON objects — before moving on.
		n, err := decodeFile(ctx, zipName, f.Name, file, lineCh)
		file.Close()
		if err != nil {
			log.Printf("[extract] decode %s/%s after %d objects: %v", zipName, f.Name, n, err)
			// If the hard context is done, stop processing more files.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}

		log.Printf("[extract] %s/%s: streamed %d JSON objects", zipName, f.Name, n)
	}
	return nil
}

// decodeFile streams individual JSON objects from an io.Reader where each
// line is a separate JSON object (NDJSON / JSON Lines format).
// Uses json.NewDecoder and decoder.More() to read objects one by one.
// Returns the number of objects decoded.
func decodeFile(ctx context.Context, zipName, fileName string, file io.Reader, lineCh chan<- LineRecord) (int, error) {
	decoder := json.NewDecoder(file)
	lineNo := 0

	for decoder.More() {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return lineNo, fmt.Errorf("decode object #%d: %w", lineNo+1, err)
		}
		lineNo++

		// Send to lineCh; only the hard deadline can interrupt.
		select {
		case <-ctx.Done():
			return lineNo, fmt.Errorf("hard deadline after %d objects: %w", lineNo, ctx.Err())
		case lineCh <- LineRecord{
			ZipName:  zipName,
			FileName: fileName,
			LineNo:   lineNo,
			Line:     string(raw),
		}:
		}
	}

	return lineNo, nil
}

// ---------------------------------------------------------------------------
// Pipeline Stage 2: lineCh → ClickHouse (native batch, 1000 rows)
//
// Graceful shutdown: workers keep consuming from lineCh until it is closed
// AND fully drained, then flush whatever remains in their buffer.
// The hard deadline forcefully stops them if the drain takes too long.
// ---------------------------------------------------------------------------

func insertWorker(
	ctx context.Context,
	id int,
	conn driver.Conn,
	cfg Config,
	lineCh <-chan LineRecord,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	buf := make([]LineRecord, 0, cfg.BatchSize)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	flush := func() {
		if len(buf) == 0 {
			return
		}
		// Use a background context for the final flush so it isn't cancelled
		// by the hard deadline prematurely. ClickHouse batch sends are fast.
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer flushCancel()
		if err := sendBatch(flushCtx, conn, cfg, buf); err != nil {
			log.Printf("[worker-%d] flush error (%d rows): %v", id, len(buf), err)
		} else {
			log.Printf("[worker-%d] flushed %d rows", id, len(buf))
		}
		buf = buf[:0]
	}

	for {
		select {
		case <-ctx.Done():
			// Hard deadline hit. Drain whatever is immediately available
			// in lineCh, then flush and exit.
			log.Printf("[worker-%d] hard deadline, draining lineCh...", id)
			for {
				select {
				case rec, ok := <-lineCh:
					if !ok {
						flush()
						return
					}
					buf = append(buf, rec)
					if len(buf) >= cfg.BatchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}

		case <-ticker.C:
			flush()

		case rec, ok := <-lineCh:
			if !ok {
				// Channel closed — producer is done. Final flush.
				log.Printf("[worker-%d] lineCh closed, final flush", id)
				flush()
				return
			}
			buf = append(buf, rec)
			if len(buf) >= cfg.BatchSize {
				flush()
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Main
//
// Shutdown flow:
//   1. SIGINT/SIGTERM → cancel stopCtx
//   2. watchZips sees stopCtx.Done():
//      - stops polling for new zips
//      - finishes the current zip completely (all files, all objects)
//      - closes lineCh
//   3. insert workers drain lineCh until closed, flush remaining batches
//   4. wg.Wait() returns → clean exit
//   5. Workers continue draining lineCh with no fixed timeout
//   6. If draining takes longer than 5 minutes, hardCtx fires as emergency safeguard
// ---------------------------------------------------------------------------

func main() {
	cfg := loadConfig()

	// stopCtx: cancelled on signal → stops polling for new zips.
	stopCtx, stopCancel := context.WithCancel(context.Background())
	defer stopCancel()

	// hardCtx: fires ShutdownTimeout after signal → force-kills everything.
	hardCtx, hardCancel := context.WithCancel(context.Background())
	defer hardCancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-sig
		log.Printf("[main] received %v — graceful shutdown (waiting for lineCh to drain)", s)
		shuttingDown.Store(1)
		stopCancel()

		// Second signal → immediate exit.
		s = <-sig
		log.Printf("[main] received %v again — immediate exit", s)
		os.Exit(1)
	}()

	// ClickHouse.
	conn, err := newClickHouseConn(cfg)
	if err != nil {
		log.Fatalf("[main] clickhouse: %v", err)
	}
	defer conn.Close()
	if err := ensureTable(hardCtx, conn, cfg); err != nil {
		log.Fatalf("[main] ensure table: %v", err)
	}
	log.Println("[main] clickhouse ready")

	// SFTP.
	sc, sshConn, err := newSFTPClient(cfg)
	if err != nil {
		log.Fatalf("[main] sftp: %v", err)
	}
	defer sc.Close()
	defer sshConn.Close()
	log.Println("[main] sftp connected")

	// Buffered channel.
	lineCh := make(chan LineRecord, cfg.LineChannelSize)

	var wg sync.WaitGroup

	// Stage 1: watcher → lineCh
	// Uses stopCtx to stop polling, hardCtx to force-stop mid-zip.
	wg.Add(1)
	go func() {
		defer wg.Done()
		watchZips(stopCtx, hardCtx, cfg, sc, lineCh)
	}()

	// Stage 2: workers drain lineCh → ClickHouse.
	// Use hardCtx so they force-stop on the hard deadline.
	var workerWg sync.WaitGroup
	for i := 0; i < cfg.NumWorkers; i++ {
		workerWg.Add(1)
		go insertWorker(hardCtx, i, conn, cfg, lineCh, &workerWg)
	}

	log.Printf("[main] pipeline running — poll=%s workers=%d batch=%d lineBuf=%d",
		cfg.PollInterval, cfg.NumWorkers, cfg.BatchSize, cfg.LineChannelSize)

	// Wait for watcher to finish (it closes lineCh when done).
	wg.Wait()
	log.Println("[main] watcher done, waiting for workers to drain lineCh...")

	// Wait for workers to drain and flush.
	// Workers will keep consuming from lineCh until it's empty, regardless of time.
	// Use a long hard deadline only as an emergency safeguard.
	done := make(chan struct{})
	go func() {
		workerWg.Wait()
		close(done)
	}()

	hardDeadline := time.NewTimer(5 * time.Minute)
	select {
	case <-done:
		log.Println("[main] all workers done — shutdown complete")
		hardDeadline.Stop()
	case <-hardDeadline.C:
		log.Println("[main] worker drain exceeded hard deadline (5 min) — forcing exit")
		hardCancel()
		<-done
		log.Println("[main] workers stopped after hard deadline")
	}
}
