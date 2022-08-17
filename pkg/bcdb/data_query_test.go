// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package bcdb

import (
	"fmt"
	"io/ioutil"
	"path"
	"sort"
	"testing"

	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestDataJSONQuery(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
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

	txId, receiptEnv, err := txDB.Commit(true)
	require.NoError(t, err)
	require.True(t, len(txId) > 0)
	require.NotNil(t, receiptEnv)

	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	dbPerm := map[string]types.Privilege_Access{
		"testDB": 1,
	}
	addUser(t, "alice", adminSession, pemUserCert, dbPerm)
	userSession := openUserSession(t, bcdb, "alice", clientCertTemDir)

	v1 := `{"attr1":true, "attr2": -10, "attr3": "name1"}`
	putKeySync(t, "testDB", "key1", v1, "alice", userSession)

	v2 := `{"attr1":false, "attr2": -110, "attr3": "name2"}`
	putKeySync(t, "testDB", "key2", v2, "alice", userSession)

	v3 := `{"attr1":true, "attr2": -1, "attr3": "name3"}`
	putKeySync(t, "testDB", "key3", v3, "alice", userSession)

	v4 := `{"attr1":false, "attr2": 0, "attr3": "name4"}`
	putKeySync(t, "testDB", "key4", v4, "alice", userSession)

	v5 := `{"attr1":true, "attr2": 110, "attr3": "name5"}`
	putKeySync(t, "testDB", "key5", v5, "alice", userSession)

	v6 := `{"attr1":true, "attr2": 100, "attr3": "name6"}`
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
		q, err := userSession.Query()
		require.NoError(t, err)
		require.NotNil(t, q)
		query := `
		{
			"selector": {
				"attr1": {"$eq": true}
			}
		}
	`
		kvs, err := q.ExecuteJSONQuery("testDB", query)
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

		sort.Slice(kvs, func(i, j int) bool {
			return kvs[i].GetKey() < kvs[j].GetKey()
		})
		require.True(t, proto.Equal(
			&types.DataQueryResponse{
				KVs: expectedKVs,
			},
			&types.DataQueryResponse{
				KVs: kvs,
			},
		))
	})

	t.Run("equal on string", func(t *testing.T) {
		q, err := userSession.Query()
		require.NoError(t, err)
		require.NotNil(t, q)
		query := `
		{
			"selector": {
				"attr3": {"$eq": "name3"}
			}
		}
	`
		kvs, err := q.ExecuteJSONQuery("testDB", query)
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

		sort.Slice(kvs, func(i, j int) bool {
			return kvs[i].GetKey() < kvs[j].GetKey()
		})
		require.True(t, proto.Equal(
			&types.DataQueryResponse{
				KVs: expectedKVs,
			},
			&types.DataQueryResponse{
				KVs: kvs,
			},
		))
	})

	t.Run("equal on number", func(t *testing.T) {
		q, err := userSession.Query()
		require.NoError(t, err)
		require.NotNil(t, q)
		query := `
		{
			"selector": {
				"attr2": {"$eq": 100}
			}
		}
	`
		kvs, err := q.ExecuteJSONQuery("testDB", query)
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

		sort.Slice(kvs, func(i, j int) bool {
			return kvs[i].GetKey() < kvs[j].GetKey()
		})
		require.True(t, proto.Equal(
			&types.DataQueryResponse{
				KVs: expectedKVs,
			},
			&types.DataQueryResponse{
				KVs: kvs,
			},
		))
	})

	t.Run("not equal on number", func(t *testing.T) {
		q, err := userSession.Query()
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
		kvs, err := q.ExecuteJSONQuery("testDB", query)
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

		sort.Slice(kvs, func(i, j int) bool {
			return kvs[i].GetKey() < kvs[j].GetKey()
		})
		require.True(t, proto.Equal(
			&types.DataQueryResponse{
				KVs: expectedKVs,
			},
			&types.DataQueryResponse{
				KVs: kvs,
			},
		))
	})

	t.Run("multiple conditions", func(t *testing.T) {
		q, err := userSession.Query()
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
		kvs, err := q.ExecuteJSONQuery("testDB", query)
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

		sort.Slice(kvs, func(i, j int) bool {
			return kvs[i].GetKey() < kvs[j].GetKey()
		})
		require.True(t, proto.Equal(
			&types.DataQueryResponse{
				KVs: expectedKVs,
			},
			&types.DataQueryResponse{
				KVs: kvs,
			},
		))
	})

	t.Run("syntax error", func(t *testing.T) {
		q, err := userSession.Query()
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
		kvs, err := q.ExecuteJSONQuery("testDB", query)
		require.Contains(t, err.Error(), "attribute [attr2] is indexed but incorrect value type provided in the query: query syntex error: array should be used for $neq condition")
		require.Nil(t, kvs)
	})
}

func TestRangeQuery(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	txDB, err := adminSession.DBsTx()
	require.NoError(t, err)

	err = txDB.CreateDB("testDB", nil)
	require.NoError(t, err)

	txId, receiptEnv, err := txDB.Commit(true)
	require.NoError(t, err)
	require.True(t, len(txId) > 0)
	require.NotNil(t, receiptEnv)

	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	dbPerm := map[string]types.Privilege_Access{
		"testDB": 1,
	}
	addUser(t, "alice", adminSession, pemUserCert, dbPerm)
	userSession := openUserSession(t, bcdb, "alice", clientCertTemDir)

	for i := 0; i < 13; i++ {
		putKeySync(t, "testDB", fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i), "alice", userSession)
	}

	t.Run("full scan", func(t *testing.T) {
		q, err := userSession.Query()
		require.NoError(t, err)
		require.NotNil(t, q)

		var expectedResult []*types.KVWithMetadata
		for i := 0; i < 13; i++ {
			expectedResult = append(expectedResult, &types.KVWithMetadata{
				Key:   fmt.Sprintf("key%d", i),
				Value: []byte(fmt.Sprintf("value%d", i)),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: uint64(i + 4),
						TxNum:    0,
					},
					AccessControl: &types.AccessControl{
						ReadUsers:          map[string]bool{"alice": true},
						ReadWriteUsers:     map[string]bool{"alice": true},
						SignPolicyForWrite: 0,
					},
				},
			})
		}

		var actualResult []*types.KVWithMetadata
		itr, err := q.GetDataByRange("testDB", "", "", 0)
		require.NoError(t, err)

		for {
			kv, ok, err := itr.Next()
			require.NoError(t, err)
			if !ok {
				break
			}
			actualResult = append(actualResult, kv)
		}

		require.Equal(t, len(expectedResult), len(actualResult))
		sort.Slice(expectedResult, func(i, j int) bool {
			return expectedResult[i].GetKey() < expectedResult[j].GetKey()
		})
		sort.Slice(actualResult, func(i, j int) bool {
			return actualResult[i].GetKey() < actualResult[j].GetKey()
		})
		require.True(t, proto.Equal(
			&types.DataQueryResponse{
				KVs: expectedResult,
			},
			&types.DataQueryResponse{
				KVs: actualResult,
			},
		))
	})

	t.Run("full scan with a higher limit", func(t *testing.T) {
		q, err := userSession.Query()
		require.NoError(t, err)
		require.NotNil(t, q)

		var expectedResult []*types.KVWithMetadata
		for _, i := range []int{0, 1, 10, 11, 12, 2} {
			expectedResult = append(expectedResult, &types.KVWithMetadata{
				Key:   fmt.Sprintf("key%d", i),
				Value: []byte(fmt.Sprintf("value%d", i)),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: uint64(i + 4),
						TxNum:    0,
					},
					AccessControl: &types.AccessControl{
						ReadUsers: map[string]bool{
							"alice": true,
						},
						ReadWriteUsers: map[string]bool{
							"alice": true,
						},
					},
				},
			})
		}

		var actualResult []*types.KVWithMetadata
		itr, err := q.GetDataByRange("testDB", "", "", 6)
		require.NoError(t, err)

		for {
			kv, ok, err := itr.Next()
			require.NoError(t, err)
			if !ok {
				break
			}
			actualResult = append(actualResult, kv)
		}

		require.Equal(t, len(expectedResult), len(actualResult))
		sort.Slice(expectedResult, func(i, j int) bool {
			return expectedResult[i].GetKey() < expectedResult[j].GetKey()
		})
		sort.Slice(actualResult, func(i, j int) bool {
			return actualResult[i].GetKey() < actualResult[j].GetKey()
		})
		require.True(t, proto.Equal(
			&types.DataQueryResponse{
				KVs: expectedResult,
			},
			&types.DataQueryResponse{
				KVs: actualResult,
			},
		))
	})

	t.Run("full scan with a lower limit", func(t *testing.T) {
		q, err := userSession.Query()
		require.NoError(t, err)
		require.NotNil(t, q)

		var expectedResult []*types.KVWithMetadata
		for _, i := range []int{0} {
			expectedResult = append(expectedResult, &types.KVWithMetadata{
				Key:   fmt.Sprintf("key%d", i),
				Value: []byte(fmt.Sprintf("value%d", i)),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: uint64(i + 4),
						TxNum:    0,
					},
					AccessControl: &types.AccessControl{
						ReadUsers: map[string]bool{
							"alice": true,
						},
						ReadWriteUsers: map[string]bool{
							"alice": true,
						},
					},
				},
			})
		}

		var actualResult []*types.KVWithMetadata
		itr, err := q.GetDataByRange("testDB", "", "", 1)
		require.NoError(t, err)

		for {
			kv, ok, err := itr.Next()
			require.NoError(t, err)
			if !ok {
				break
			}
			actualResult = append(actualResult, kv)
		}

		require.Equal(t, len(expectedResult), len(actualResult))
		sort.Slice(expectedResult, func(i, j int) bool {
			return expectedResult[i].GetKey() < expectedResult[j].GetKey()
		})
		sort.Slice(actualResult, func(i, j int) bool {
			return actualResult[i].GetKey() < actualResult[j].GetKey()
		})
		require.True(t, proto.Equal(
			&types.DataQueryResponse{
				KVs: expectedResult,
			},
			&types.DataQueryResponse{
				KVs: actualResult,
			},
		))
	})

	t.Run("empty scan", func(t *testing.T) {
		q, err := userSession.Query()
		require.NoError(t, err)
		require.NotNil(t, q)

		var actualResult []*types.KVWithMetadata

		itr, err := q.GetDataByRange("testDB", "p", "q", 0)
		require.NoError(t, err)

		for {
			kv, ok, err := itr.Next()
			require.NoError(t, err)
			if !ok {
				break
			}
			actualResult = append(actualResult, kv)
		}

		require.Len(t, actualResult, 0)
		require.Nil(t, actualResult)
	})

	t.Run("partial scan", func(t *testing.T) {
		q, err := userSession.Query()
		require.NoError(t, err)
		require.NotNil(t, q)

		var expectedResult []*types.KVWithMetadata
		for i := 2; i < 9; i++ {
			expectedResult = append(expectedResult, &types.KVWithMetadata{
				Key:   fmt.Sprintf("key%d", i),
				Value: []byte(fmt.Sprintf("value%d", i)),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: uint64(i + 4),
						TxNum:    0,
					},
					AccessControl: &types.AccessControl{
						ReadUsers: map[string]bool{
							"alice": true,
						},
						ReadWriteUsers: map[string]bool{
							"alice": true,
						},
					},
				},
			})
		}

		var actualResult []*types.KVWithMetadata
		itr, err := q.GetDataByRange("testDB", "key2", "key9", 0)
		require.NoError(t, err)

		for {
			kv, ok, err := itr.Next()
			require.NoError(t, err)
			if !ok {
				break
			}
			actualResult = append(actualResult, kv)
		}

		require.Equal(t, len(expectedResult), len(actualResult))
		sort.Slice(expectedResult, func(i, j int) bool {
			return expectedResult[i].GetKey() < expectedResult[j].GetKey()
		})
		sort.Slice(actualResult, func(i, j int) bool {
			return actualResult[i].GetKey() < actualResult[j].GetKey()
		})
		require.True(t, proto.Equal(
			&types.DataQueryResponse{
				KVs: expectedResult,
			},
			&types.DataQueryResponse{
				KVs: actualResult,
			},
		))
	})

	t.Run("partial scan with a limit", func(t *testing.T) {
		q, err := userSession.Query()
		require.NoError(t, err)
		require.NotNil(t, q)

		var expectedResult []*types.KVWithMetadata
		for i := 2; i < 6; i++ {
			expectedResult = append(expectedResult, &types.KVWithMetadata{
				Key:   fmt.Sprintf("key%d", i),
				Value: []byte(fmt.Sprintf("value%d", i)),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: uint64(i + 4),
						TxNum:    0,
					},
					AccessControl: &types.AccessControl{
						ReadUsers: map[string]bool{
							"alice": true,
						},
						ReadWriteUsers: map[string]bool{
							"alice": true,
						},
					},
				},
			})
		}

		var actualResult []*types.KVWithMetadata
		itr, err := q.GetDataByRange("testDB", "key2", "key9", 4)
		require.NoError(t, err)

		for {
			kv, ok, err := itr.Next()
			require.NoError(t, err)
			if !ok {
				break
			}
			actualResult = append(actualResult, kv)
		}

		require.Equal(t, len(expectedResult), len(actualResult))
		sort.Slice(expectedResult, func(i, j int) bool {
			return expectedResult[i].GetKey() < expectedResult[j].GetKey()
		})
		sort.Slice(actualResult, func(i, j int) bool {
			return actualResult[i].GetKey() < actualResult[j].GetKey()
		})
		require.True(t, proto.Equal(
			&types.DataQueryResponse{
				KVs: expectedResult,
			},
			&types.DataQueryResponse{
				KVs: actualResult,
			},
		))
	})
}
