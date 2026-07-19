package blocking

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/bloom"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const blockedTorrentsBloomFilterKeyBase = "blocked_torrents"

// perInstanceBloomFilterKey returns a hostname-specific key for bloom filter isolation.
// Multiple bitmagnet instances sharing one PG database each get their own bloom filter row.
func perInstanceBloomFilterKey() string {
	host := os.Getenv("HOSTNAME")
	if host == "" {
		return blockedTorrentsBloomFilterKeyBase
	}
	return blockedTorrentsBloomFilterKeyBase + "_" + host
}

type Manager interface {
	Filter(ctx context.Context, hashes []protocol.ID) ([]protocol.ID, error)
	Block(ctx context.Context, hashes []protocol.ID, flush bool) error
	Flush(ctx context.Context) error
}

type manager struct {
	mutex         sync.Mutex
	pool          *pgxpool.Pool
	buffer        map[protocol.ID]struct{}
	filter        *bloom.StableBloomFilter
	maxBufferSize int
	lastFlushedAt time.Time
	maxFlushWait  time.Duration
}

func (m *manager) Filter(ctx context.Context, hashes []protocol.ID) ([]protocol.ID, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.filter == nil || m.shouldFlush() {
		if flushErr := m.flush(ctx); flushErr != nil {
			return nil, flushErr
		}
	}

	// Guard against nil/uninitialized filter (e.g. after corrupted bloom load)
	if m.filter == nil {
		def := bloom.NewDefaultStableBloomFilter()
		m.filter = def
	}

	filtered := make([]protocol.ID, 0, len(hashes))

	for _, hash := range hashes {
		if _, ok := m.buffer[hash]; ok {
			continue
		}

		if m.safeTest(hash) {
			continue
		}

		filtered = append(filtered, hash)
	}

	return filtered, nil
}

// safeTest wraps filter.Test with a recover to guard against panics
// from corrupted or uninitialized internal filter state.
func (m *manager) safeTest(hash protocol.ID) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			// Reinitialize the filter on panic
			def := bloom.NewDefaultStableBloomFilter()
			m.filter = def
			ok = false
		}
	}()
	return m.filter.Test(hash[:])
}

func (m *manager) Block(ctx context.Context, hashes []protocol.ID, flush bool) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for _, hash := range hashes {
		m.buffer[hash] = struct{}{}
	}

	if flush || m.shouldFlush() {
		if flushErr := m.flush(ctx); flushErr != nil {
			return flushErr
		}
	}

	return nil
}

func (m *manager) Flush(ctx context.Context) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.buffer) == 0 {
		return nil
	}

	return m.flush(ctx)
}

func (m *manager) flush(ctx context.Context) error {
	hashes := slices.Collect(maps.Keys(m.buffer))
	bfKey := perInstanceBloomFilterKey()

	tx, err := m.pool.BeginTx(ctx, pgx.TxOptions{
		AccessMode: pgx.ReadWrite,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if len(hashes) > 0 {
		_, err = tx.Exec(ctx, "DELETE FROM torrents WHERE info_hash = any($1)", hashes)
		if err != nil {
			return fmt.Errorf("failed to delete from torrents table: %w", err)
		}
	}

	bf := bloom.NewDefaultStableBloomFilter()

	lobs := tx.LargeObjects()

	found := false

	var oid uint32

	var nullOid sql.NullInt32

	err = tx.QueryRow(ctx, "SELECT oid FROM bloom_filters WHERE key = $1", bfKey).
		Scan(&nullOid)
	if err == nil {
		found = true

		if nullOid.Valid {
			oid = uint32(nullOid.Int32)

			obj, err := lobs.Open(ctx, oid, pgx.LargeObjectModeRead)
			if err != nil {
				return fmt.Errorf("failed to open large object for reading: %w", err)
			}

			_, err = bf.ReadFrom(obj)
			obj.Close()

			if err != nil {
				return fmt.Errorf("failed to read current bloom filter: %w", err)
			}
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("failed to get bloom filter object ID: %w", err)
	}

	if oid == 0 {
		oid, err = lobs.Create(ctx, 0)
		if err != nil {
			return fmt.Errorf("failed to create large object: %w", err)
		}
	}

	for _, hash := range hashes {
		bf.Add(hash[:])
	}

	obj, err := lobs.Open(ctx, oid, pgx.LargeObjectModeWrite)
	if err != nil {
		return fmt.Errorf("failed to open large object for writing: %w", err)
	}

	_, err = bf.WriteTo(obj)
	if err != nil {
		return fmt.Errorf("failed to write to large object: %w", err)
	}

	now := time.Now()
	if !found {
		_, err = tx.Exec(ctx,
			"INSERT INTO bloom_filters (key, oid, created_at, updated_at) VALUES ($1, $2, $3, $4)",
			bfKey, oid, now, now)
		if err != nil {
			return fmt.Errorf("failed to save new bloom filter record: %w", err)
		}
	} else if !nullOid.Valid {
		_, err = tx.Exec(ctx,
			"UPDATE bloom_filters SET oid = $1, updated_at = $2 WHERE key = $3",
			oid, now, bfKey)
		if err != nil {
			return fmt.Errorf("failed to update bloom filter record: %w", err)
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	m.buffer = make(map[protocol.ID]struct{})
	m.filter = bf
	m.lastFlushedAt = now

	return nil
}

func (m *manager) shouldFlush() bool {
	return len(m.buffer) >= m.maxBufferSize || time.Since(m.lastFlushedAt) >= m.maxFlushWait
}
