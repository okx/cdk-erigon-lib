package memdb

import (
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"fmt"
)

func (m *MemoryMutation) Snapshot(id uint64) error {
	m.snapshotsMutex.Lock()
	defer m.snapshotsMutex.Unlock()

	if _, exists := m.snapshots[id]; exists {
		return fmt.Errorf("snapshot with id %d already exists", id)
	}
	snapshot, err := m.Clone(m.db, m.tmpDir, m)
	if err != nil {
		return err
	}
	m.snapshots[id] = snapshot
	return nil
}

func (m *MemoryMutation) SnapshotFlush(id uint64, tx kv.RwTx) error {
	m.snapshotsMutex.Lock()
	snapshot, exists := m.snapshots[id]
	m.snapshotsMutex.Unlock()

	if !exists {
		return fmt.Errorf("snapshot with id %d does not exist", id)
	}

	err := snapshot.Flush(tx)
	if err != nil {
		return fmt.Errorf("failed to flush snapshot with id %d: %w", id, err)
	}

	m.snapshotsMutex.Lock()
	delete(m.snapshots, id)
	m.snapshotsMutex.Unlock()

	return nil
}

func (m *MemoryMutation) Clone(tx kv.Tx, tmpDir string, src *MemoryMutation) (*MemoryMutation, error) {
	dst := NewMemoryBatch(tx, tmpDir)

	// deleted
	for table, entries := range src.deletedEntries {
		if _, ok := dst.deletedEntries[table]; !ok {
			dst.deletedEntries[table] = make(map[string]struct{})
		}
		for key := range entries {
			dst.deletedEntries[table][key] = struct{}{}
		}
	}

	// cleared
	for table := range src.clearedTables {
		dst.clearedTables[table] = struct{}{}
	}

	// updated
	buckets, err := src.memTx.ListBuckets()
	if err != nil {
		return nil, err
	}
	for _, bucket := range buckets {
		c, err := src.memTx.Cursor(bucket)
		if err != nil {
			return nil, err
		}
		defer c.Close()

		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				return nil, err
			}
			if err := dst.memTx.Put(bucket, k, v); err != nil {
				return nil, err
			}
		}
	}

	return dst, nil
}
