package bcdb

import (
	"io/ioutil"
	"path"
	"testing"

	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestDataJSONQuery(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	txDB, err := adminSession.DBsTx()
	require.NoError(t, err)

	index := map[string]types.IndexAttributeType{
		"attr1": types.IndexAttributeType_BOOLEAN,
		"attr2": types.IndexAttributeType_NUMBER,
		"attr3": types.IndexAttributeType_STRING,
	}
	err = txDB.CreateDB("testDB", index)
	require.NoError(t, err)

	txId, receipt, err := txDB.Commit(true)
	require.NoError(t, err)
	require.True(t, len(txId) > 0)
	require.NotNil(t, receipt)

	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	dbPerm := map[string]types.Privilege_Access{
		"testDB": 1,
	}
	addUser(t, "alice", adminSession, pemUserCert, dbPerm)
	userSession := openUserSession(t, bcdb, "alice", clientCertTemDir)

	v1 := `{"attr1":true, "attr2": -10, "attr3": "name1", "attr4": "abc"}`
	putKeySync(t, "testDB", "key1", v1, "alice", userSession)

	v2 := `{"attr1":false, "attr2": -110, "attr3": "name2", "attr4": "def"}`
	putKeySync(t, "testDB", "key2", v2, "alice", userSession)

	v3 := `{"attr1":true, "attr2": -1, "attr3": "name3", "attr4": "ghi"}`
	putKeySync(t, "testDB", "key3", v3, "alice", userSession)

	v4 := `{"attr1":false, "attr2": 0, "attr3": "name4", "attr4": "jkl"}`
	putKeySync(t, "testDB", "key4", v4, "alice", userSession)

	v5 := `{"attr1":true, "attr2": 110, "attr3": "name5", "attr4": "mno"}`
	putKeySync(t, "testDB", "key5", v5, "alice", userSession)

	v6 := `{"attr1":true, "attr2": 100, "attr3": "name6", "attr4": "pqr"}`
	putKeySync(t, "testDB", "key6", v6, "alice", userSession)

	acl := &types.AccessControl{
		ReadUsers: map[string]bool{
			"alice": true,
		},
		ReadWriteUsers: map[string]bool{
			"alice": true,
		},
	}

	t.Run("equal on boolean", func(t *testing.T) {
		q, err := userSession.JSONQuery()
		require.NoError(t, err)
		require.NotNil(t, q)
		query := `
		{
			"selector": {
				"attr1": {"$eq": true}
			}
		}
	`
		kvs, err := q.Execute("testDB", query)
		require.NoError(t, err)
		expectedKVs := []*types.KVWithMetadata{
			{
				Key:   "key1",
				Value: []byte(v1),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 4,
						TxNum:    0,
					},
					AccessControl: acl,
				},
			},
			{
				Key:   "key3",
				Value: []byte(v3),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 6,
						TxNum:    0,
					},
					AccessControl: acl,
				},
			},
			{
				Key:   "key5",
				Value: []byte(v5),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 8,
						TxNum:    0,
					},
					AccessControl: acl,
				},
			},
			{
				Key:   "key6",
				Value: []byte(v6),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 9,
						TxNum:    0,
					},
					AccessControl: acl,
				},
			},
		}
		require.ElementsMatch(t, expectedKVs, kvs)
	})

	t.Run("equal on string", func(t *testing.T) {
		q, err := userSession.JSONQuery()
		require.NoError(t, err)
		require.NotNil(t, q)
		query := `
		{
			"selector": {
				"attr3": {"$eq": "name3"}
			}
		}
	`
		kvs, err := q.Execute("testDB", query)
		require.NoError(t, err)
		expectedKVs := []*types.KVWithMetadata{
			{
				Key:   "key3",
				Value: []byte(v3),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 6,
						TxNum:    0,
					},
					AccessControl: acl,
				},
			},
		}
		require.ElementsMatch(t, expectedKVs, kvs)
	})

	t.Run("equal on number", func(t *testing.T) {
		q, err := userSession.JSONQuery()
		require.NoError(t, err)
		require.NotNil(t, q)
		query := `
		{
			"selector": {
				"attr2": {"$eq": 100}
			}
		}
	`
		kvs, err := q.Execute("testDB", query)
		require.NoError(t, err)
		expectedKVs := []*types.KVWithMetadata{
			{
				Key:   "key6",
				Value: []byte(v6),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 9,
						TxNum:    0,
					},
					AccessControl: acl,
				},
			},
		}
		require.ElementsMatch(t, expectedKVs, kvs)
	})

	t.Run("not equal on number", func(t *testing.T) {
		q, err := userSession.JSONQuery()
		require.NoError(t, err)
		require.NotNil(t, q)
		query := `
		{
			"selector": {
				"attr2": {
					"$neq": [100, 110, 0]
				}
			}
		}
	`
		kvs, err := q.Execute("testDB", query)
		require.NoError(t, err)
		expectedKVs := []*types.KVWithMetadata{
			{
				Key:   "key1",
				Value: []byte(v1),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 4,
						TxNum:    0,
					},
					AccessControl: acl,
				},
			},
			{
				Key:   "key2",
				Value: []byte(v2),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 5,
						TxNum:    0,
					},
					AccessControl: acl,
				},
			},
			{
				Key:   "key3",
				Value: []byte(v3),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 6,
						TxNum:    0,
					},
					AccessControl: acl,
				},
			},
		}
		require.ElementsMatch(t, expectedKVs, kvs)
	})

	t.Run("multiple conditions", func(t *testing.T) {
		q, err := userSession.JSONQuery()
		require.NoError(t, err)
		require.NotNil(t, q)
		query := `
		{
			"selector": {
				"$and": {
					"attr1": {
						"$neq": [false]
					},
					"attr2": {
						"$gte": -1,
						"$lte": 100
					},
					"attr3": {
						"$gt": "name1",
						"$lte": "name6"
					}
				}
			}
		}
	`
		kvs, err := q.Execute("testDB", query)
		require.NoError(t, err)
		expectedKVs := []*types.KVWithMetadata{
			{
				Key:   "key3",
				Value: []byte(v3),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 6,
						TxNum:    0,
					},
					AccessControl: acl,
				},
			},
			{
				Key:   "key6",
				Value: []byte(v6),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 9,
						TxNum:    0,
					},
					AccessControl: acl,
				},
			},
		}
		require.ElementsMatch(t, expectedKVs, kvs)
	})

	t.Run("syntax error", func(t *testing.T) {
		q, err := userSession.JSONQuery()
		require.NoError(t, err)
		require.NotNil(t, q)
		query := `
		{
			"selector": {
				"attr2": {
					"$neq": 100
				}
			}
		}
	`
		kvs, err := q.Execute("testDB", query)
		require.Contains(t, err.Error(), "attribute [attr2] is indexed but incorrect value type provided in the query: query syntex error: array should be used for $neq condition")
		require.Nil(t, kvs)
	})
}
