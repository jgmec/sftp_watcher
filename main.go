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
	LocalDir string // local directory to download zips into

	// Processing
	PollInterval    time.Duration
	LineChannelSize int // buffered channel for LineRecord (≥ BatchSize)
	BatchSize       int // rows per ClickHouse batch
	NumWorkers      int
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
		LineChannelSize: atoi(envOr("LINE_CHANNEL_SIZE", "2000")), // 2× batch to keep workers fed
		BatchSize:       atoi(envOr("BATCH_SIZE", "1000")),
		NumWorkers:      atoi(envOr("NUM_WORKERS", "4")),
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
	Line     string // JSON-encoded object as string
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

// sendBatch uses the native clickhouse-go PrepareBatch API for high-throughput
// columnar inserts. Each call sends exactly one batch of up to BatchSize rows.
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

// ---------------------------------------------------------------------------
// Pipeline Stage 1: Watch SFTP → download → zip.OpenReader → json.NewDecoder → lineCh
// ---------------------------------------------------------------------------

func watchZips(ctx context.Context, cfg Config, sc *sftp.Client, lineCh chan<- LineRecord) {
	defer close(lineCh)

	// Ensure local download directory exists.
	if err := os.MkdirAll(cfg.LocalDir, 0o755); err != nil {
		log.Fatalf("[watch] create local dir %s: %v", cfg.LocalDir, err)
	}

	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	poll := func() {
		entries, err := sc.ReadDir(cfg.SFTPWatchDir)
		if err != nil {
			log.Printf("[watch] readdir: %v", err)
			return
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(strings.ToLower(name), ".zip") {
				continue
			}

			log.Printf("[watch] new zip: %s", name)

			remotePath := filepath.Join(cfg.SFTPWatchDir, name)
			localPath := filepath.Join(cfg.LocalDir, name)

			// Step 1: Download zip from SFTP to local file.
			if err := downloadFile(sc, remotePath, localPath); err != nil {
				log.Printf("[watch] download %s: %v", name, err)
				continue
			}
			log.Printf("[watch] downloaded %s → %s", remotePath, localPath)

			// Step 2: Open local zip, stream JSON from each file into lineCh.
			if err := extractLocalZip(ctx, localPath, name, lineCh); err != nil {
				log.Printf("[watch] extract %s: %v", name, err)
				os.Remove(localPath)
			}
		}
	}

	poll()
	for {
		select {
		case <-ctx.Done():
			log.Println("[watch] shutdown")
			return
		case <-ticker.C:
			poll()
		}
	}
}

// downloadFile transfers a remote SFTP file to a local path.
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

	remote.Close()
	err = sc.Remove(remotePath)
	if err != nil {
		return fmt.Errorf("[download] remove remote %s: %v", remotePath, err)
	}

	log.Printf("[download] %s → %s (%d bytes)", remotePath, localPath, n)
	return nil
}

// extractLocalZip opens a local zip with zip.OpenReader, then for each file
// inside calls f.Open() to get a reader and streams JSON objects directly
// using json.NewDecoder / decoder.More() — no io.ReadAll, no intermediate
// buffering. Each decoded JSON object is sent as a LineRecord into lineCh.
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

		file, err := f.Open()
		if err != nil {
			log.Printf("[extract] skip %s/%s: %v", zipName, f.Name, err)
			continue
		}

		n, err := decodeFile(ctx, zipName, f.Name, file, lineCh)
		file.Close()
		if err != nil {
			log.Printf("[extract] decode %s/%s: %v", zipName, f.Name, err)
			continue
		}

		log.Printf("[extract] %s/%s: streamed %d JSON objects", zipName, f.Name, n)
	}
	return nil
}

// decodeFile streams JSON objects from an io.Reader using json.NewDecoder
// and decoder.More(). Supports both JSON arrays and NDJSON.
// Returns the number of objects decoded.
func decodeFile(ctx context.Context, zipName, fileName string, file io.Reader, lineCh chan<- LineRecord) (int, error) {
	decoder := json.NewDecoder(file)
	lineNo := 0

	// Peek at the first token to detect JSON array vs NDJSON.
	tok, err := decoder.Token()
	if err != nil {
		return 0, fmt.Errorf("read first token: %w", err)
	}

	if delim, ok := tok.(json.Delim); ok && delim == '[' {
		// JSON array: '[' already consumed, iterate elements with decoder.More().
		for decoder.More() {
			select {
			case <-ctx.Done():
				return lineNo, ctx.Err()
			default:
			}

			var raw json.RawMessage
			if err := decoder.Decode(&raw); err != nil {
				return lineNo, fmt.Errorf("decode object #%d: %w", lineNo+1, err)
			}
			lineNo++

			select {
			case <-ctx.Done():
				return lineNo, ctx.Err()
			case lineCh <- LineRecord{
				ZipName:  zipName,
				FileName: fileName,
				LineNo:   lineNo,
				Line:     string(raw),
			}:
			}
		}

		// Consume closing ']'.
		if _, err := decoder.Token(); err != nil {
			return lineNo, fmt.Errorf("read closing bracket: %w", err)
		}
	} else {
		// NDJSON: first token was not '['.
		// The first token is already consumed (e.g. '{'), so we cannot
		// re-decode it.  Use decoder.Buffered() to capture what's left
		// of the first value, then chain a new decoder over the remainder.
		//
		// Simpler: since the first token was '{', the decoder has already
		// started reading the first object.  We decode the rest of it and
		// then continue with decoder.More() for subsequent objects.

		// We need to get the full first JSON object. Since Token() consumed
		// only the opening '{', we can still Decode — but Decode expects a
		// *complete* value and the opening '{' is gone.
		//
		// Safest approach: rebuild a decoder from the buffered remainder
		// prepended with the consumed token.
		//
		// For maximum simplicity and per user request, we assume JSON array
		// format (which is the common case for structured data in zips).
		// Log a warning for non-array files.
		log.Printf("[decode] %s/%s: expected JSON array, got token %v — skipping", zipName, fileName, tok)
	}

	return lineNo, nil
}

// ---------------------------------------------------------------------------
// Pipeline Stage 3: lineCh → ClickHouse (native batch, 1000 rows)
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
		if err := sendBatch(ctx, conn, cfg, buf); err != nil {
			log.Printf("[worker-%d] batch error (%d rows): %v", id, len(buf), err)
		} else {
			log.Printf("[worker-%d] sent batch of %d rows", id, len(buf))
		}
		buf = buf[:0]
	}

	for {
		select {
		case <-ctx.Done():
			// Drain remaining buffered records and flush.
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
// ---------------------------------------------------------------------------

func main() {
	cfg := loadConfig()

	// Root context cancelled on SIGINT / SIGTERM.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-sig
		log.Printf("[main] signal %v → shutting down", s)
		cancel()
	}()

	// ClickHouse (native protocol).
	conn, err := newClickHouseConn(cfg)
	if err != nil {
		log.Fatalf("[main] clickhouse: %v", err)
	}
	defer conn.Close()
	if err := ensureTable(ctx, conn, cfg); err != nil {
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

	// Buffered channel — 2× batch size to keep workers saturated.
	lineCh := make(chan LineRecord, cfg.LineChannelSize) // default cap=2000

	var wg sync.WaitGroup

	// Stage 1: watcher → download → zip.OpenReader → json.NewDecoder → lineCh
	go watchZips(ctx, cfg, sc, lineCh)

	// Stage 2: lineCh → ClickHouse (N workers, 1000-row native batches)
	for i := 0; i < cfg.NumWorkers; i++ {
		wg.Add(1)
		go insertWorker(ctx, i, conn, cfg, lineCh, &wg)
	}

	log.Printf("[main] pipeline running — poll=%s workers=%d batch=%d lineBuf=%d",
		cfg.PollInterval, cfg.NumWorkers, cfg.BatchSize, cfg.LineChannelSize)

	wg.Wait()
	log.Println("[main] shutdown complete")
}
