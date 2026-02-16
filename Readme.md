# SFTP Zip Watcher → ClickHouse Uploader

A Go application that watches a remote SFTP directory for `.zip` files, extracts every file inside each zip, and inserts every line into ClickHouse — using a clean pipeline of buffered channels and `context.Context` for graceful shutdown.

## Architecture

```
┌──────────────┐       ┌────────────────────┐       ┌────────────────────┐       ┌──────────────┐
│  SFTP Server │──────▶│  watchZips()        │──────▶│  parseLines()      │──────▶│  insertWorker│
│  (zip files) │       │  polls & extracts   │       │  splits into lines │       │  (×N workers)│
└──────────────┘       │                     │       │                     │       │  batch INSERT│
                       └───────┬─────────────┘       └───────┬────────────┘       └──────┬───────┘
                               │                             │                           │
                        fileCh (buffered)             lineCh (buffered)            ClickHouse
                        chan ZipEntry                  chan LineRecord              zip_lines table
```

### Pipeline Stages

| Stage | Goroutine | Channel Out | Description |
|-------|-----------|-------------|-------------|
| 1 – Watch | `watchZips` | `fileCh` (buffered) | Polls SFTP dir, downloads new zips, extracts entries |
| 2 – Parse | `parseLines` | `lineCh` (buffered) | Splits file content into individual lines |
| 3 – Insert | `insertWorker` ×N | — | Batches lines and bulk-inserts into ClickHouse |

### Graceful Shutdown

- `SIGINT` / `SIGTERM` cancels the root `context.Context`
- The watcher stops polling and closes `fileCh`
- The parser drains remaining entries, then closes `lineCh`
- Each insert worker flushes its current batch before exiting
- `main()` waits on `sync.WaitGroup` for all workers to finish

## ClickHouse Table

Auto-created on startup:

```sql
CREATE TABLE IF NOT EXISTS default.zip_lines (
    zip_name    String,
    file_name   String,
    line_no     UInt32,
    line        String,
    inserted_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (zip_name, file_name, line_no)
```

## Configuration

All settings via environment variables (see `.env.example`):

| Variable | Default | Description |
|----------|---------|-------------|
| `SFTP_HOST` | `localhost` | SFTP server hostname |
| `SFTP_PORT` | `22` | SFTP server port |
| `SFTP_USER` | `user` | SFTP username |
| `SFTP_PASSWORD` | | SFTP password |
| `SFTP_PRIVATE_KEY` | | Path to SSH private key (optional) |
| `SFTP_WATCH_DIR` | `/upload` | Remote directory to watch |
| `CH_ADDR` | `clickhouse://localhost:9000` | ClickHouse address |
| `CH_DATABASE` | `default` | ClickHouse database |
| `CH_TABLE` | `zip_lines` | Target table |
| `CH_USER` | `default` | ClickHouse user |
| `CH_PASSWORD` | | ClickHouse password |
| `POLL_INTERVAL` | `10s` | How often to check for new zips |
| `FILE_CHANNEL_SIZE` | `100` | Buffered channel size for zip entries |
| `LINE_CHANNEL_SIZE` | `10000` | Buffered channel size for parsed lines |
| `BATCH_SIZE` | `5000` | Rows per ClickHouse batch insert |
| `NUM_WORKERS` | `4` | Concurrent insert workers |

## Quick Start

```bash
# 1. Clone and build
go mod tidy
go build -o sftp-zip-watcher .

# 2. Configure
cp .env.example .env
# edit .env with your SFTP and ClickHouse credentials

# 3. Run
source .env && ./sftp-zip-watcher

# Or with Docker
docker build -t sftp-zip-watcher .
docker run --env-file .env sftp-zip-watcher
```