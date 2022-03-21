package main

import (
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"

	"github.com/hyperledger-labs/orion-sdk-go/examples/manufacture/util"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("create_users user_names crypto_folder")
	}
	argsWithoutProg := os.Args[1:]
	userNames := argsWithoutProg[:len(argsWithoutProg) - 1]
	cryptoFolder := argsWithoutProg[len(argsWithoutProg) - 1]

	session, err := util.OpenDBSession("../util/admin_config.yml")
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println("\n===Creating a Database machines===")
	fmt.Println()
	if err = createDBs(session, "machines"); err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println("\n===Creating Users===")
	fmt.Println()
	if err = createUsers(session, cryptoFolder, userNames...); err != nil {
		fmt.Println(err.Error())
		return
	}

	if err = printUsers(session, userNames...); err != nil {
		fmt.Println(err.Error())
		return
	}
}

func createDBs(s bcdb.DBSession, name string) error {
	dbtx, err := s.DBsTx()
	if err != nil {
		return err
	}

	exist, err := dbtx.Exists(name)
	if err != nil {
		return err
	}
	if exist {
		return errors.New("database db1 already exist")
	}

	if err := dbtx.CreateDB(name, nil); err != nil {
		return err
	}

	txID, receiptEnv, err := dbtx.Commit(true)
	if err != nil {
		return err
	}

	fmt.Println("transaction with txID " + txID + " got committed in the block " + strconv.Itoa(int(receiptEnv.GetResponse().GetReceipt().GetHeader().GetBaseHeader().GetNumber())))
	return nil
}

func createUsers(s bcdb.DBSession, cryptoLocation string, names ...string ) error {
	tx, err := s.UsersTx()
	if err != nil {
		return err
	}
	for _, name := range names {
		userPemCert, err:= os.ReadFile(path.Join(cryptoLocation, name, name + ".pem"))
		if err != nil {
			return err
		}
		userCertBlock, _ := pem.Decode(userPemCert)
		user := &types.User{
			Id:          name,
			Certificate: userCertBlock.Bytes,
			Privilege: &types.Privilege{
				DbPermission: map[string]types.Privilege_Access{
					"machines": types.Privilege_ReadWrite,
				},
			},
		}
		tx.PutUser(user, nil)
	}

	txID, receiptEnv, err := tx.Commit(true)
	if err != nil {
		return err
	}

	fmt.Println("transaction with txID " + txID + " got committed in the block " + strconv.Itoa(int(receiptEnv.GetResponse().GetReceipt().GetHeader().GetBaseHeader().GetNumber())))
	return nil
}

func printUsers(s bcdb.DBSession, names ...string ) error {
	tx, err := s.UsersTx()
	if err != nil {
		return err
	}
	for _, name := range names {
		user, err := tx.GetUser(name)
		if err != nil {
			return err
		}
		fmt.Printf("User %s = %+v \n", name, user.Privilege)
	}

	err = tx.Abort()
	if err != nil {
		return err
	}

	return nil
}
