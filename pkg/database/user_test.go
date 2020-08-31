package database

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/protos/types"
	server "github.ibm.com/blockchaindb/sdk/pkg/database/mock"
)

type userTestData struct {
	u          []*types.User
	op         string
	res        bool
	equal      bool
	equalRes   []bool
	errStr     string
	totalNil   int
	totalNoNil int
}

func TestDbUserManagement_User(t *testing.T) {
	addTestCases := []*userTestData{
		{
			u: []*types.User{
				{
					ID:          "testUser",
					Certificate: make([]byte, 0),
					Privilege: &types.Privilege{
						DBAdministration: false,
					},
				},
				{
					ID:          "testUser2",
					Certificate: make([]byte, 0),
					Privilege:   &types.Privilege{},
				},
			},
			op:         "add",
			res:        true,
			equal:      true,
			equalRes:   []bool{true, true},
			errStr:     "",
			totalNoNil: 2,
		},
		{
			u: []*types.User{
				{
					ID:          "testUser",
					Certificate: make([]byte, 0),
					Privilege: &types.Privilege{
						DBAdministration: true,
					},
				},
				{
					ID:          "testUser3",
					Certificate: make([]byte, 0),
					Privilege:   &types.Privilege{},
				},
			},
			op:         "add",
			res:        true,
			equal:      true,
			equalRes:   []bool{false, true},
			errStr:     "",
			totalNoNil: 3,
		},
	}

	updateTestCases := []*userTestData{
		{
			u: []*types.User{
				{
					ID:          "testUser",
					Certificate: make([]byte, 0),
					Privilege: &types.Privilege{
						DBAdministration: true,
					},
				},
			},
			op:         "update",
			res:        true,
			totalNoNil: 0,
			equal:      false,
			errStr:     "",
		},
		{
			u: []*types.User{
				{
					ID:          "testUser",
					Certificate: make([]byte, 0),
					Privilege: &types.Privilege{
						DBAdministration: false,
					},
				},
			},
			op:         "add",
			res:        true,
			totalNoNil: 1,
			equal:      true,
			equalRes:   []bool{true},
			errStr:     "",
		},
		{
			u: []*types.User{
				{
					ID:          "testUser",
					Certificate: make([]byte, 0),
					Privilege: &types.Privilege{
						DBAdministration: true,
					},
				},
				{
					ID:          "testUser2",
					Certificate: make([]byte, 0),
					Privilege: &types.Privilege{
						DBAdministration: false,
					},
				},
			},
			op:         "update",
			res:        true,
			totalNoNil: 1,
			equal:      true,
			equalRes:   []bool{true, false},
			errStr:     "",
		},
	}

	deleteTestCases := []*userTestData{
		{
			u: []*types.User{
				{
					ID:          "testUser",
					Certificate: make([]byte, 0),
					Privilege:   &types.Privilege{},
				},
			},
			op:         "delete",
			res:        true,
			totalNoNil: 0,
			totalNil:   0,
			equal:      false,
			errStr:     "",
		},
		{
			u: []*types.User{
				{
					ID:          "testUser",
					Certificate: make([]byte, 0),
					Privilege:   &types.Privilege{},
				},
				{
					ID:          "testUser2",
					Certificate: make([]byte, 0),
					Privilege: &types.Privilege{
						DBAdministration: true,
					},
				},
			},
			op:         "add",
			res:        true,
			totalNoNil: 2,
			totalNil:   0,
			equal:      true,
			equalRes:   []bool{true, true},
			errStr:     "",
		},
		{
			u: []*types.User{
				{
					ID:          "testUser",
					Certificate: make([]byte, 0),
					Privilege:   &types.Privilege{},
				},
				{
					ID:          "testUser2",
					Certificate: make([]byte, 0),
					Privilege:   &types.Privilege{},
				},
			},
			op:         "delete",
			res:        true,
			totalNoNil: 0,
			totalNil:   2,
			equal:      false,
			errStr:     "",
		},
		{
			u: []*types.User{
				{
					ID:          "testUser2",
					Certificate: make([]byte, 0),
					Privilege:   &types.Privilege{},
				},
			},
			op:         "delete",
			res:        true,
			totalNoNil: 0,
			totalNil:   2,
			equal:      false,
			errStr:     "",
		},
	}

	t.Run("Add user", func(t *testing.T) {
		t.Parallel()
		s := server.NewTestServer()
		defer s.Stop()
		port, err := s.Port()
		require.NoError(t, err)

		options := createOptions(port)
		connector := createDBConnector(t, options)

		um := connector.GetUserManagement()

		runUserTestCases(t, addTestCases, um.(*dbUserManagement), s)

		err = um.AddUsers([]*types.User{nil})
		require.Error(t, err)
		require.Contains(t, err.Error(), "json: cannot unmarshal")
	})

	t.Run("Update user", func(t *testing.T) {
		t.Parallel()
		s := server.NewTestServer()
		defer s.Stop()
		port, err := s.Port()
		require.NoError(t, err)

		options := createOptions(port)
		connector := createDBConnector(t, options)

		um := connector.GetUserManagement()
		runUserTestCases(t, updateTestCases, um.(*dbUserManagement), s)

		err = um.UpdateUsers([]*types.User{nil})
		require.Error(t, err)
		require.Contains(t, err.Error(), "json: cannot unmarshal")
	})

	t.Run("Delete user", func(t *testing.T) {
		t.Parallel()
		s := server.NewTestServer()
		defer s.Stop()
		port, err := s.Port()
		require.NoError(t, err)

		options := createOptions(port)
		connector := createDBConnector(t, options)

		um := connector.GetUserManagement()
		runUserTestCases(t, deleteTestCases, um.(*dbUserManagement), s)

		err = um.DeleteUsers([]*types.User{nil})
		require.Error(t, err)
		require.Contains(t, err.Error(), "json: cannot unmarshal")
	})

	t.Run("Error cases internal users db access", func(t *testing.T) {
		t.Parallel()
		s := server.NewTestServer()
		defer s.Stop()
		port, err := s.Port()
		require.NoError(t, err)

		options := createOptions(port)
		options.User.Signer.KeyFilePath = "non_exist_path"
		connector := createDBConnector(t, options)

		um := connector.GetUserManagement()
		_, err = um.GetUser("testUser")
		require.Error(t, err)
		err = um.AddUsers([]*types.User{
			{
				ID:          "testGroup",
				Certificate: make([]byte, 0),
				Privilege:   &types.Privilege{},
			},
		})
		require.Error(t, err)
		err = um.UpdateUsers([]*types.User{
			{
				ID:          "testGroup",
				Certificate: make([]byte, 0),
				Privilege:   &types.Privilege{},
			},
		})
		require.Error(t, err)
		err = um.DeleteUsers([]*types.User{
			{
				ID:          "testGroup",
				Certificate: make([]byte, 0),
				Privilege:   &types.Privilege{},
			},
		})
		require.Error(t, err)

		options = createOptions(port)
		connector = createDBConnector(t, options)
		um = connector.GetUserManagement()
		um.(*dbUserManagement).initInternalUserManagementDatabase()
		um.(*dbUserManagement).internalUserManagementDatabase.Close()

		err = um.AddUsers([]*types.User{
			{
				ID:          "testGroup",
				Certificate: make([]byte, 0),
				Privilege:   &types.Privilege{},
			},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "db closed")
	})

	t.Run("Error cases Get", func(t *testing.T) {
		t.Parallel()
		s := server.NewTestServer()
		defer s.Stop()
		port, err := s.Port()
		require.NoError(t, err)

		options := createOptions(port)
		connector := createDBConnector(t, options)

		um := connector.GetUserManagement()
		u, err := um.GetUser("testUser")
		require.NoError(t, err)
		require.Nil(t, u)
		um.(*dbUserManagement).internalUserManagementDatabase.Close()
		_, err = um.GetUser("testUser")
		require.Error(t, err)
		require.Contains(t, err.Error(), "db closed")
	})
}

func runUserTestCases(t *testing.T, testCases []*userTestData, um *dbUserManagement, s *server.TestServer) {
	for _, testCase := range testCases {
		var err error
		switch testCase.op {
		case "add":
			err = um.AddUsers(testCase.u)
		case "update":
			err = um.UpdateUsers(testCase.u)
		case "delete":
			err = um.DeleteUsers(testCase.u)
		}

		if testCase.res {
			require.NoError(t, err)
			if testCase.equal {
				for i, u := range testCase.u {
					storedUser, err := um.GetUser(u.ID)
					require.NoError(t, err)
					require.Equal(t, testCase.equalRes[i], proto.Equal(u, storedUser))
				}
			}
			totalNil := 0
			totalNonNil := 0
			for _, v := range s.GetAllKeysForDB("_users") {
				if v == nil {
					totalNil++
				} else {
					totalNonNil++
				}
			}
			require.Equal(t, testCase.totalNoNil, totalNonNil)
			require.Equal(t, testCase.totalNil, totalNil)
		} else {
			require.Error(t, err)
			if len(testCase.errStr) != 0 {
				require.Contains(t, err.Error(), testCase.errStr)
			}
		}
	}
}
