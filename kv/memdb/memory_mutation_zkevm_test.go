package memdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
)

type SnapshotScenario struct {
	initialData    map[string]string
	snapshots      map[uint64]map[string]string
	deletes        map[uint64][]string
	expectedData   map[uint64]map[string]string
	finalSnapshots []uint64
}

func initializeMutationSnapshots(rwTx kv.RwTx, initialData map[string]string) {
	for key, value := range initialData {
		rwTx.Put(kv.HashedAccounts, []byte(key), []byte(value))
	}
}

func applySnapshots(t *testing.T, m *MemoryMutation, snapshots map[uint64]map[string]string, deletes map[uint64][]string) {
	for i := 1; i <= len(snapshots); i++ {
		snapshotID := uint64(i)
		data := snapshots[snapshotID]
		for key, value := range data {
			err := m.Put(kv.HashedAccounts, []byte(key), []byte(value))
			require.NoError(t, err)
		}
		for _, key := range deletes[snapshotID] {
			err := m.Delete(kv.HashedAccounts, []byte(key))
			require.NoError(t, err)
		}
		err := m.Snapshot(snapshotID)
		require.NoError(t, err)
	}
}

func TestMemoryMutationSnapshot(t *testing.T) {
	db := NewTestDB(t)
	defer db.Close()

	scenarios := map[string]SnapshotScenario{
		"basic": {
			initialData: map[string]string{
				"NOSNAP": "data",
			},
			snapshots: map[uint64]map[string]string{
				1: {"SNAP1": "data-snap1"},
				2: {"SNAP2": "data-snap2"},
				3: {"SNAP3": "data-snap3"},
			},
			expectedData: map[uint64]map[string]string{
				1: {"NOSNAP": "data", "SNAP1": "data-snap1"},
				2: {"NOSNAP": "data", "SNAP1": "data-snap1", "SNAP2": "data-snap2"},
				3: {"NOSNAP": "data", "SNAP1": "data-snap1", "SNAP2": "data-snap2", "SNAP3": "data-snap3"},
			},
			finalSnapshots: []uint64{1, 2, 3},
		},
		"complex": {
			initialData: map[string]string{
				"KEY1": "initial1",
				"KEY2": "initial2",
				"KEY3": "initial3",
				"KEY4": "initial4",
			},
			snapshots: map[uint64]map[string]string{
				1: {
					"KEY1": "snap1-mod1",
					"KEY2": "snap1-mod2",
				},
				2: {
					"KEY2": "snap2-mod2",
					"KEY3": "snap2-mod3",
				},
				3: {
					"KEY1": "snap3-mod1",
					"KEY4": "snap3-mod4",
				},
				4: {
					"KEY1": "snap4-mod1",
					"KEY3": "snap4-mod3",
				},
			},
			deletes: map[uint64][]string{
				2: {"KEY1"},
				3: {"KEY2"},
				4: {"KEY4"},
			},
			expectedData: map[uint64]map[string]string{
				1: {
					"KEY1": "snap1-mod1",
					"KEY2": "snap1-mod2",
					"KEY3": "initial3",
					"KEY4": "initial4",
				},
				2: {
					"KEY2": "snap2-mod2",
					"KEY3": "snap2-mod3",
					"KEY4": "initial4",
				},
				3: {
					"KEY1": "snap3-mod1",
					"KEY3": "snap2-mod3",
					"KEY4": "snap3-mod4",
				},
				4: {
					"KEY1": "snap4-mod1",
					"KEY3": "snap4-mod3",
				},
			},
			finalSnapshots: []uint64{1, 2, 3, 4},
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			db = NewTestDB(t)
			defer db.Close()
			// initialize db
			rwTx := BeginRw(t, db)
			initializeMutationSnapshots(rwTx, scenario.initialData)
			require.NoError(t, rwTx.Commit())

			// initialize memory mutation
			roTx := BeginRo(t, db)
			tmpDir := t.TempDir()
			m := NewMemoryBatch(roTx, tmpDir)

			// apply snapshots
			applySnapshots(t, m, scenario.snapshots, scenario.deletes)

			// close read-only transaction to avoid overlapping
			require.NoError(t, roTx.Commit())

			// flush and verify snapshots
			for _, snapshotID := range scenario.finalSnapshots {
				newRwTx := BeginRw(t, db)
				require.NoError(t, m.SnapshotFlush(snapshotID, newRwTx))
				require.NoError(t, newRwTx.Commit())

				verifyDbContents(t, db, scenario.expectedData[snapshotID])
			}
		})
	}
}

func verifyDbContents(t *testing.T, db kv.RoDB, expectedData map[string]string) {
	roTx := BeginRo(t, db)
	defer roTx.Rollback()

	for key, expectedValue := range expectedData {
		val, err := roTx.GetOne(kv.HashedAccounts, []byte(key))
		if expectedValue == "" {
			require.Error(t, err)
			assert.Nil(t, val)
		} else {
			require.NoError(t, err)
			assert.Equal(t, []byte(expectedValue), val)
		}
	}
}
