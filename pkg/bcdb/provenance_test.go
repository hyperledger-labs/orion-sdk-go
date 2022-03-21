// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestGetHistoricalData(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServerWithParams(t, clientCertTemDir, time.Second, 1, false, false)
	defer testServer.Stop()
	require.NoError(t, err)
	_, _, aliceSession := startServerConnectOpenAdminCreateUserAndUserSession(t, testServer, clientCertTemDir, "alice")

	// 15 blocks, 1 tx each
	for i := 0; i < 5; i++ {
		keys := make([]string, 0)
		values := make([]string, 0)
		for j := 0; j <= i; j++ {
			keys = append(keys, fmt.Sprintf("key%d", j))
			values = append(values, fmt.Sprintf("value%d_%d", i, j))
		}
		putMultipleKeysAndValues(t, keys, values, "alice", aliceSession)
	}

	tests := []struct {
		name    string
		key     string
		values  [][]byte
		wantErr bool
	}{
		{
			name:    "key0 - 5 prevValues",
			key:     "key0",
			values:  [][]byte{[]byte("value0_0"), []byte("value1_0"), []byte("value2_0"), []byte("value3_0"), []byte("value4_0")},
			wantErr: false,
		},
		{
			name:    "key2 - 3 prevValues",
			key:     "key2",
			values:  [][]byte{[]byte("value2_2"), []byte("value3_2"), []byte("value4_2")},
			wantErr: false,
		},
		{
			name:    "key4 - 1 value",
			key:     "key4",
			values:  [][]byte{[]byte("value4_4")},
			wantErr: false,
		},
		{
			name:    "key6 - 0 value",
			key:     "key6",
			values:  nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := aliceSession.Provenance()
			require.NoError(t, err)
			hData, err := p.GetHistoricalData("bdb", tt.key)
			if !tt.wantErr {
				require.NoError(t, err)
				if tt.values == nil {
					require.Nil(t, hData)
					return
				}
				require.NotNil(t, hData)
				resValues := make([][]byte, 0)
				for _, d := range hData {
					resValues = append(resValues, d.Value)
				}
				require.ElementsMatch(t, tt.values, resValues)
			} else {
				require.Error(t, err)
				require.Nil(t, hData)
			}
		})
	}
}

func TestGetHistoricalDataAt(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServerWithParams(t, clientCertTemDir, time.Second, 5, false, false)
	defer testServer.Stop()
	require.NoError(t, err)
	_, _, aliceSession := startServerConnectOpenAdminCreateUserAndUserSession(t, testServer, clientCertTemDir, "alice")

	// 5 blocks, 5 tx each
	for i := 0; i < 5; i++ {
		keys := make([]string, 0)
		values := make([]string, 0)
		for j := 0; j < 5; j++ {
			keys = append(keys, fmt.Sprintf("key%d", j))
			values = append(values, fmt.Sprintf("value%d_%d", i, j))
		}
		putMultipleKeysAndValues(t, keys, values, "alice", aliceSession)
	}

	tests := []struct {
		name    string
		key     string
		version *types.Version
		want    *types.ValueWithMetadata
		wantErr bool
	}{
		{
			name: "key0, block 3, index 0",
			key:  "key0",
			version: &types.Version{
				BlockNum: 3,
				TxNum:    0,
			},
			want: &types.ValueWithMetadata{
				Value: []byte("value0_0"),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 3,
						TxNum:    0,
					},
					AccessControl: &types.AccessControl{
						ReadUsers:      map[string]bool{"alice": true},
						ReadWriteUsers: map[string]bool{"alice": true},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "key1, block 5, index 1",
			key:  "key1",
			version: &types.Version{
				BlockNum: 5,
				TxNum:    1,
			},
			want: &types.ValueWithMetadata{
				Value: []byte("value2_1"),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 5,
						TxNum:    1,
					},
					AccessControl: &types.AccessControl{
						ReadUsers:      map[string]bool{"alice": true},
						ReadWriteUsers: map[string]bool{"alice": true},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "key2, block 10, block not exist",
			key:  "key1",
			version: &types.Version{
				BlockNum: 12,
				TxNum:    2,
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "key2, block 5, index 12, index not exist",
			key:  "key1",
			version: &types.Version{
				BlockNum: 5,
				TxNum:    12,
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := aliceSession.Provenance()
			require.NoError(t, err)
			got, err := p.GetHistoricalDataAt("bdb", tt.key, tt.version)
			if !tt.wantErr {
				require.NoError(t, err)
				gotStr, err := json.Marshal(got)
				require.NoError(t, err)
				wantStr, err := json.Marshal(tt.want)
				require.NoError(t, err)
				require.True(t, proto.Equal(tt.want, got), fmt.Sprintf("expected \n%s, got \n%s ", wantStr, gotStr))
			} else {
				require.Error(t, err)
				require.Nil(t, got)
			}
		})
	}
}

func TestGetPreviousOrNextHistoricalData(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServerWithParams(t, clientCertTemDir, time.Second, 5, false, false)
	defer testServer.Stop()
	require.NoError(t, err)
	_, _, aliceSession := startServerConnectOpenAdminCreateUserAndUserSession(t, testServer, clientCertTemDir, "alice")

	// 25 blocks, 5 tx each
	for i := 0; i < 5; i++ {
		keys := make([]string, 0)
		values := make([]string, 0)
		for j := 0; j < 5; j++ {
			keys = append(keys, fmt.Sprintf("key%d", j))
			values = append(values, fmt.Sprintf("value%d_%d", i, j))
		}
		putMultipleKeysAndValues(t, keys, values, "alice", aliceSession)
	}

	tests := []struct {
		name       string
		key        string
		version    *types.Version
		prevValues [][]byte
		nextValues [][]byte
		wantErr    bool
	}{
		{
			name: "key1 first data tx block, no previous, 4 next values",
			key:  "key1",
			version: &types.Version{
				BlockNum: 3,
				TxNum:    1,
			},
			prevValues: nil,
			nextValues: [][]byte{[]byte("value1_1"), []byte("value2_1"), []byte("value3_1"), []byte("value4_1")},
			wantErr:    false,
		},
		{
			name: "key1 third data tx block, 3 previous, 1 next value",
			key:  "key1",
			version: &types.Version{
				BlockNum: 6,
				TxNum:    1,
			},
			prevValues: [][]byte{[]byte("value0_1"), []byte("value1_1"), []byte("value2_1")},
			nextValues: [][]byte{[]byte("value4_1")},
			wantErr:    false,
		},
		{
			name: "key1 , no such version exist",
			key:  "key1",
			version: &types.Version{
				BlockNum: 15,
				TxNum:    1,
			},
			prevValues: nil,
			nextValues: nil,
			wantErr:    false,
		},
		{
			name: "key2, 4 prev, 0 next",
			key:  "key2",
			version: &types.Version{
				BlockNum: 7,
				TxNum:    2,
			},
			prevValues: [][]byte{[]byte("value0_2"), []byte("value1_2"), []byte("value2_2"), []byte("value3_2")},
			nextValues: nil,
			wantErr:    false,
		},
		{
			name: "key1, version associated with another key, no prevValues",
			key:  "key1",
			version: &types.Version{
				BlockNum: 7,
				TxNum:    2,
			},
			prevValues: nil,
			nextValues: nil,
			wantErr:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := aliceSession.Provenance()
			require.NoError(t, err)
			hPrevData, err := p.GetPreviousHistoricalData("bdb", tt.key, tt.version)
			require.NoError(t, err)
			hNextData, err := p.GetNextHistoricalData("bdb", tt.key, tt.version)
			require.NoError(t, err)
			if !tt.wantErr {
				require.NoError(t, err)
				if tt.prevValues == nil {
					require.Nil(t, hPrevData)
				} else {
					require.NotNil(t, hPrevData)
					resValues := make([][]byte, 0)
					for _, d := range hPrevData {
						resValues = append(resValues, d.Value)
					}
					require.ElementsMatch(t, tt.prevValues, resValues)
				}
				if tt.nextValues == nil {
					require.Nil(t, hNextData)
				} else {
					require.NotNil(t, hNextData)
					resValues := make([][]byte, 0)
					for _, d := range hNextData {
						resValues = append(resValues, d.Value)
					}
					require.ElementsMatch(t, tt.nextValues, resValues)
				}
			} else {
				require.Error(t, err)
				require.Nil(t, hPrevData)
			}
		})
	}
}

func TestReadWriteAccessBytUserAndKey(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "bob", "eve", "server"})
	testServer, _, _, err := SetupTestServerWithParams(t, clientCertTemDir, time.Second, 1, false, false)
	defer testServer.Stop()
	require.NoError(t, err)
	bcdb, adminSession, aliceSession := startServerConnectOpenAdminCreateUserAndUserSession(t, testServer, clientCertTemDir, "alice")

	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "bob.pem"))
	dbPerm := map[string]types.Privilege_Access{
		"bdb": 1,
	}
	require.NoError(t, err)
	addUser(t, "bob", adminSession, pemUserCert, dbPerm)
	pemUserCert, err = ioutil.ReadFile(path.Join(clientCertTemDir, "eve.pem"))
	require.NoError(t, err)
	addUser(t, "eve", adminSession, pemUserCert, dbPerm)
	bobSession := openUserSession(t, bcdb, "bob", clientCertTemDir)
	eveSession := openUserSession(t, bcdb, "eve", clientCertTemDir)

	// 20 blocks, 1 tx each, 10 keys
	for i := 0; i < 2; i++ {
		keys := make([]string, 0)
		values := make([]string, 0)
		for j := 0; j < 10; j++ {
			keys = append(keys, fmt.Sprintf("key%d", j))
			values = append(values, fmt.Sprintf("value%d_%d", i, j))
		}
		putMultipleKeysAndValidateMultipleUsers(t, keys, values, []string{"alice", "bob", "eve"}, aliceSession)
	}

	users := []string{"bob", "eve"}
	usersSession := []DBSession{bobSession, eveSession}
	usersReadKey := make([][]string, len(users))
	usersWrittenKey := make([][]string, len(users))
	usersTxReceipt := make([][]*types.TxReceipt, len(users))

	for i := 0; i < 5; i++ {
		userIdx := i % (len(users))
		readKey := fmt.Sprintf("key%d", i*2)
		writeKey := fmt.Sprintf("key%d", i*2+1)
		usersReadKey[userIdx] = append(usersReadKey[userIdx], readKey)
		usersWrittenKey[userIdx] = append(usersWrittenKey[userIdx], writeKey)
		usersTxReceipt[userIdx] = append(usersTxReceipt[userIdx], runUpdateTx(t, users[userIdx], usersSession[userIdx], readKey, writeKey))
	}

	userTests := []struct {
		name        string
		user        string
		readKeys    []string
		writtenKeys []string
		txReceipt   []*types.TxReceipt
		wantErr     bool
	}{
		{
			name:        "bob test, 3 reads, 3 writes",
			user:        "bob",
			readKeys:    usersReadKey[0],
			writtenKeys: usersWrittenKey[0],
			txReceipt:   usersTxReceipt[0],
			wantErr:     false,
		},
		{
			name:        "eve test, 2 reads, 2 writes",
			user:        "eve",
			readKeys:    usersReadKey[1],
			writtenKeys: usersWrittenKey[1],
			txReceipt:   usersTxReceipt[1],
			wantErr:     false,
		},
	}
	for _, tt := range userTests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := aliceSession.Provenance()
			require.NoError(t, err)
			keys := make([]string, 0)
			reads, err := p.GetDataReadByUser(tt.user)
			require.NoError(t, err)
			for _, k := range reads {
				keys = append(keys, k.GetKey())
			}
			require.ElementsMatch(t, tt.readKeys, keys)
			keys = make([]string, 0)
			writes, err := p.GetDataWrittenByUser(tt.user)
			require.NoError(t, err)
			for _, k := range writes {
				keys = append(keys, k.GetKey())
			}
			require.ElementsMatch(t, tt.writtenKeys, keys)
		})
	}

	keyTests := []struct {
		name    string
		key     string
		readers []string
		writers []string
		wantErr bool
	}{
		{
			name:    "key0",
			key:     "key0",
			readers: []string{"bob"},
			writers: []string{"alice"},
		},
		{
			name:    "key5",
			key:     "key5",
			readers: nil,
			writers: []string{"alice", "bob"},
		},
		{
			name:    "key6",
			key:     "key6",
			readers: []string{"eve"},
			writers: []string{"alice"},
		},
		{
			name:    "key3",
			key:     "key3",
			readers: nil,
			writers: []string{"alice", "eve"},
		},
		{
			name:    "key11",
			key:     "key11",
			readers: nil,
			writers: nil,
		},
	}

	for _, tt := range keyTests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := aliceSession.Provenance()
			require.NoError(t, err)
			readers, err := p.GetReaders("bdb", tt.key)
			require.NoError(t, err)
			require.ElementsMatch(t, tt.readers, readers)
			writers, err := p.GetWriters("bdb", tt.key)
			require.NoError(t, err)
			require.ElementsMatch(t, tt.writers, writers)
		})
	}
}

func TestGetTxIDsSubmittedByUser(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "bob", "eve", "server"})
	testServer, _, _, err := SetupTestServerWithParams(t, clientCertTemDir, time.Second, 10, false, false)
	defer testServer.Stop()
	require.NoError(t, err)
	bcdb, adminSession, aliceSession := startServerConnectOpenAdminCreateUserAndUserSession(t, testServer, clientCertTemDir, "alice")

	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "bob.pem"))
	require.NoError(t, err)
	dbPerm := map[string]types.Privilege_Access{
		"bdb": 1,
	}
	addUser(t, "bob", adminSession, pemUserCert, dbPerm)
	bobSession := openUserSession(t, bcdb, "bob", clientCertTemDir)

	pemUserCert, err = ioutil.ReadFile(path.Join(clientCertTemDir, "eve.pem"))
	require.NoError(t, err)
	addUser(t, "eve", adminSession, pemUserCert, dbPerm)
	eveSession := openUserSession(t, bcdb, "eve", clientCertTemDir)

	users := []string{"alice", "bob"}
	userSessions := []DBSession{aliceSession, bobSession}
	userTx := make(map[string][]proto.Message)

	// 4 blocks, 10 tx each, 10 keys, each key repeated each block
	// odd blocks tx submitted by Alice, even by Bob
	for i := 0; i < 3; i++ {
		keys := make([]string, 0)
		values := make([]string, 0)
		for j := 0; j < 10; j++ {
			keys = append(keys, fmt.Sprintf("key%d", j))
			values = append(values, fmt.Sprintf("value%d_%d", i, j))
		}
		userTx[users[i%2]] = append(userTx[users[i%2]], putMultipleKeysAndValidateMultipleUsers(t, keys, values, users, userSessions[i%2])...)
	}

	tests := []struct {
		name        string
		user        string
		userSession DBSession
		userTxNum   int
		userTx      []proto.Message
		wantErr     bool
	}{
		{
			name:        "user alice - multiple tx",
			user:        "alice",
			userSession: aliceSession,
			userTxNum:   20,
			userTx:      userTx["alice"],
			wantErr:     false,
		},
		{
			name:        "user bob - multiple tx",
			user:        "bob",
			userSession: bobSession,
			userTxNum:   10,
			userTx:      userTx["bob"],
			wantErr:     false,
		},
		{
			name:        "user eve - no tx",
			user:        "eve",
			userSession: eveSession,
			userTxNum:   0,
			userTx:      nil,
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := tt.userSession.Provenance()
			require.NoError(t, err)
			txs, err := p.GetTxIDsSubmittedByUser(tt.user)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.userTxNum, len(txs))
			expectedTxIds := make([]string, 0)
			for _, tx := range tt.userTx {
				expectedTxIds = append(expectedTxIds, tx.(*types.DataTxEnvelope).GetPayload().GetTxId())
			}
			require.ElementsMatch(t, expectedTxIds, txs)
		})
	}
}

func runUpdateTx(t *testing.T, user string, userSession DBSession, readKey string, writeKey string) *types.TxReceipt {
	userTx, err := userSession.DataTx()
	require.NoError(t, err)
	rVal, _, err := userTx.Get("bdb", readKey)
	require.NoError(t, err)
	wVal := string(rVal) + "Updated"
	err = userTx.Put("bdb", writeKey, []byte(wVal), &types.AccessControl{
		ReadUsers:      map[string]bool{"alice": true, user: true},
		ReadWriteUsers: map[string]bool{user: true},
	})
	require.NoError(t, err)

	_, receiptEnv, err := userTx.Commit(true)
	require.NoError(t, err)
	require.NotNil(t, receiptEnv)
	return receiptEnv.GetResponse().GetReceipt()
}
