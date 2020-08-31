package database

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/protos/types"
)

type dbUserManagement struct {
	// Internal database that stores all users
	internalUserManagementDatabase      DBSession
	internalUserManagementDatabaseMutex sync.RWMutex

	connector *dbConnector
}

func (u *dbUserManagement) GetUser(userID string) (*types.User, error) {
	if err := u.initInternalUserManagementDatabase(); err != nil {
		return nil, err
	}
	value, err := u.internalUserManagementDatabase.Get(userID)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	user := &types.User{}
	err = json.Unmarshal(value, user)
	if err != nil {
		return nil, err
	}
	return user, nil
}

func (u *dbUserManagement) AddUsers(users []*types.User) error {
	if err := u.initInternalUserManagementDatabase(); err != nil {
		return err
	}
	createCallback := func(user *types.User, orgExist bool, tx TxContext) error {
		if orgExist {
			return &ExistErr{user}
		}
		msgBytes, err := json.Marshal(user)
		if err != nil {
			return err
		}
		return tx.Put(user.GetID(), msgBytes)
	}
	return update(users, u.internalUserManagementDatabase, u.connector, createCallback, "create")
}

func (u *dbUserManagement) UpdateUsers(users []*types.User) error {
	if err := u.initInternalUserManagementDatabase(); err != nil {
		return err
	}
	updateCallback := func(user *types.User, orgExist bool, tx TxContext) error {
		if !orgExist {
			return &NonExistErr{user}
		}
		msgBytes, err := json.Marshal(user)
		if err != nil {
			return err
		}
		return tx.Put(user.GetID(), msgBytes)
	}
	return update(users, u.internalUserManagementDatabase, u.connector, updateCallback, "update")
}

func (u *dbUserManagement) DeleteUsers(users []*types.User) error {
	if err := u.initInternalUserManagementDatabase(); err != nil {
		return err
	}
	deleteCallback := func(user *types.User, orgExist bool, tx TxContext) error {
		if !orgExist {
			return &NonExistErr{user}
		}
		return tx.Delete(user.GetID())
	}
	return update(users, u.internalUserManagementDatabase, u.connector, deleteCallback, "delete")
}

type updateCallbackFunc func(user *types.User, orgExist bool, tx TxContext) error

type NonExistErr struct {
	user *types.User
}

func (e *NonExistErr) Error() string {
	return fmt.Sprintf("can't update object %v, no object with its id exist", e.user)
}

type ExistErr struct {
	user *types.User
}

func (e *ExistErr) Error() string {
	return fmt.Sprintf("can't create new object for key %s, object with this key already exist", e.user.GetID())
}

func update(users []*types.User, db DBSession, connector *dbConnector, updateCallback updateCallbackFunc, op string) error {
	tx, err := db.Begin(connector.options.TxOptions)
	if err != nil {
		return err
	}
	for _, user := range users {
		org, err := tx.Get(user.GetID())
		if err != nil {
			return err
		}
		var nonExistErr *NonExistErr
		var existErr *ExistErr
		if err := updateCallback(user, org != nil, tx); err != nil {
			if errors.As(err, &nonExistErr) || errors.As(err, &existErr) {
				log.Printf("%s \n", err.Error())
				continue
			}
			return err
		}
	}
	_, err = tx.Commit()
	return errors.WithMessagef(err, "can't commit %s transaction for %v", op, users)
}

func (u *dbUserManagement) initInternalUserManagementDatabase() error {
	u.internalUserManagementDatabaseMutex.Lock()
	defer u.internalUserManagementDatabaseMutex.Unlock()
	if u.internalUserManagementDatabase != nil {
		return nil
	}
	var err error
	u.internalUserManagementDatabase, err = u.connector.OpenDBSession("_users", u.connector.options.TxOptions)
	if err != nil {
		return err
	}
	return nil
}
