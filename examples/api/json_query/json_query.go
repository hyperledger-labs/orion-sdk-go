// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"sort"

	"github.com/hyperledger-labs/orion-sdk-go/examples/util"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
)

/*
	- Creating database 'db' with 3 indexes associated with it (name, age, gender)
	- Adding multiple key, value pairs to the database
	- Execute 6 valid queries and 1 invalid query
*/
func main() {
	if err := executeJsonQueryExample("../../util/config.yml"); err != nil {
		os.Exit(1)
	}
}

func executeJsonQueryExample(configLocation string) error {
	session, err := prepareData(configLocation)
	if session == nil || err != nil {
		return err
	}

	err = clearData(session)
	if err != nil {
		return err
	}

	err = createDatabase(session)
	if err != nil {
		return err
	}

	err = insertData(session)
	if err != nil {
		return err
	}

	err = validQueries(session)
	if err != nil {
		return err
	}

	err = invalidQuery(session)
	if err != nil {
		return err
	}

	return nil
}

func prepareData(configLocation string) (bcdb.DBSession, error) {
	c, err := util.ReadConfig(configLocation)
	if err != nil {
		fmt.Printf(err.Error())
		return nil, err
	}

	logger, err := logger.New(
		&logger.Config{
			Level:         "debug",
			OutputPath:    []string{"stdout"},
			ErrOutputPath: []string{"stderr"},
			Encoding:      "console",
			Name:          "bcdb-client",
		},
	)
	if err != nil {
		fmt.Printf(err.Error())
		return nil, err
	}

	conConf := &config.ConnectionConfig{
		ReplicaSet: c.ConnectionConfig.ReplicaSet,
		RootCAs:    c.ConnectionConfig.RootCAs,
		Logger:     logger,
	}

	fmt.Println("Opening connection to database, configuration: ", c.ConnectionConfig)
	db, err := bcdb.Create(conConf)
	if err != nil {
		fmt.Printf("Database connection creating failed, reason: %s\n", err.Error())
		return nil, err
	}

	sessionConf := &config.SessionConfig{
		UserConfig:   c.SessionConfig.UserConfig,
		TxTimeout:    c.SessionConfig.TxTimeout,
		QueryTimeout: c.SessionConfig.QueryTimeout}

	fmt.Println("Opening session to database, configuration: ", c.SessionConfig)
	session, err := db.Session(sessionConf)
	if err != nil {
		fmt.Printf("Database session creating failed, reason: %s\n", err.Error())
		return nil, err
	}

	return session, nil
}

func clearData(session bcdb.DBSession) error {
	fmt.Println("Opening database transaction")
	dbTx, err := session.DBsTx()
	if err != nil {
		fmt.Printf("Database transaction creation failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Checking if database 'db' already exists")
	exists, err := dbTx.Exists("db")
	if err != nil {
		fmt.Printf("Checking the existence of database failed, reason: %s\n", err.Error())
		return err
	}
	if exists {
		fmt.Println("Deleting database 'db'")
		err = dbTx.DeleteDB("db")
		if err != nil {
			fmt.Printf("Deleting database failed, reason: %s\n", err.Error())
			return err
		}
	}

	fmt.Println("Committing transaction")
	txID, _, err := dbTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	return nil
}

func createDatabase(session bcdb.DBSession) error {
	fmt.Println("Opening database transaction")
	dbTx, err := session.DBsTx()
	if err != nil {
		fmt.Printf("Database transaction creation failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Creating database 'db' with name, age and gender as indexes")
	index := map[string]types.IndexAttributeType{
		"name":   types.IndexAttributeType_STRING,
		"age":    types.IndexAttributeType_NUMBER,
		"gender": types.IndexAttributeType_BOOLEAN, //female 1, male 0
	}
	err = dbTx.CreateDB("db", index)
	if err != nil {
		fmt.Printf("Database creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Committing transaction")
	txID, _, err := dbTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	return nil
}

func insertData(session bcdb.DBSession) error {
	fmt.Println("Opening data transaction")
	tx, err := session.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	v0 := `{"name": "alice", "age": 18, "gender": true, "education": "high-schoo"}`
	v1 := `{"name": "bob", "age": 30, "gender": false, "education": "bachelor"}`
	v2 := `{"name": "charlie", "age": 40, "gender": false, "education": "master"}`
	v3 := `{"name": "dan", "age": 20, "gender": false, "education": "doctorate"}`
	v4 := `{"name": "eve", "age": 30, "gender": true, "education": "bachelor"}`
	v5 := `{"name": "alice", "age": 35, "gender": true}`
	v6 := `{"name": "bob", "age": 40, "gender": false}`
	v7 := `{"name": "charlie", "age": 22, "gender": true}`
	v8 := `{"name": "dan", "age": 30}`
	v9 := `{"name": "eve", "age":40}`

	values := [10]string{v0, v1, v2, v3, v4, v5, v6, v7, v8, v9}
	keys := [10]string{"id0", "id1", "id2", "id3", "id4", "id5", "id6", "id7", "id8", "id9"}

	for i := 0; i < 10; i++ {
		fmt.Println("Adding key, value: " + keys[i] + ", " + values[i] + " to the database")
		err = tx.Put("db", keys[i], []byte(values[i]), nil)
		if err != nil {
			fmt.Printf("Adding new key to database failed, reason: %s\n", err.Error())
			return err
		}
	}

	fmt.Println("Committing transaction")
	txID, _, err := tx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	return nil
}

func validQueries(session bcdb.DBSession) error {
	q, err := session.Query()
	if err != nil {
		fmt.Printf("Failed to return handler to access bcdb data through JSON query, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("")
	fmt.Println("=== query1 - returns women named alice who are over 25 ===")
	query1 := `
	{
		"selector": {
			"$and": {  
				"name": {
					"$eq": "alice"
				},
				"age": {
					"$gt": 25
				},
				"gender": {
					"$eq": true
				}
			}
		}
	}
	`
	kvs, err := q.ExecuteJSONQuery("db", query1)
	if err != nil {
		fmt.Printf("Failed to execute JSON query, reason: %s\n", err.Error())
		return err
	}
	if kvs == nil {
		fmt.Println("kvs nil")
		return errors.New("kvs nil")
	}
	if len(kvs) != 1 || kvs[0].Key != "id5" {
		fmt.Println("Query results are not as expected")
		return errors.New("Query results are not as expected")
	}
	fmt.Printf("As expected the keys that returned are: %v\n", kvs)

	fmt.Println("")
	fmt.Println("=== query2 - returns people named eve who are under 40 (including 40) ===")
	query2 := `
	{
		"selector": {
			"$and": {  
				"name": {
					"$eq": "eve"
				},
				"age": {
					"$lte": 40
				}
			}
		}
	}
	`
	kvs, err = q.ExecuteJSONQuery("db", query2)
	if err != nil {
		fmt.Printf("Failed to execute JSON query, reason: %s\n", err.Error())
		return err
	}
	if kvs == nil {
		fmt.Println("kvs nil")
		return errors.New("kvs nil")
	}

	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})
	if len(kvs) != 2 || kvs[0].Key != "id4" || kvs[1].Key != "id9" {
		fmt.Println("Query results are not as expected")
		return errors.New("Query results are not as expected")
	}
	fmt.Printf("As expected the keys that returned are: %v\n", kvs)

	fmt.Println("")
	fmt.Println("=== query3 - returns people named charlie and people under 25 ===")
	query3 := `
	{
		"selector": {
			"$or": {  
				"name": {
					"$eq": "charlie"
				},
				"age": {
					"$lt": 25
				}
			}
		}
	}
	`
	kvs, err = q.ExecuteJSONQuery("db", query3)
	if err != nil {
		fmt.Printf("Failed to execute JSON query, reason: %s\n", err.Error())
		return err
	}
	if kvs == nil {
		fmt.Println("kvs nil")
		return errors.New("kvs nil")
	}

	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})
	if len(kvs) != 4 || kvs[0].Key != "id0" || kvs[1].Key != "id2" || kvs[2].Key != "id3" || kvs[3].Key != "id7" {
		fmt.Println("Query results are not as expected")
		return errors.New("Query results are not as expected")
	}
	fmt.Printf("As expected the keys that returned are: %v\n", kvs)

	fmt.Println("")
	fmt.Println("=== query4 - returns all people who are not 30 or 40 years old ===")
	query4 := `
	{
		"selector": {
			"age": {
				"$neq": [30, 40] 
			}
		}
	}
	`
	kvs, err = q.ExecuteJSONQuery("db", query4)
	if err != nil {
		fmt.Printf("Failed to execute JSON query, reason: %s\n", err.Error())
		return err
	}
	if kvs == nil {
		fmt.Println("kvs nil")
		return errors.New("kvs nil")
	}

	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})
	if len(kvs) != 4 || kvs[0].Key != "id0" || kvs[1].Key != "id3" || kvs[2].Key != "id5" || kvs[3].Key != "id7" {
		fmt.Println("Query results are not as expected")
		return errors.New("Query results are not as expected")
	}
	fmt.Printf("As expected the keys that returned are: %v\n", kvs)

	fmt.Println("")
	fmt.Println("=== query5 - returns all people between the ages of 25 and 40 ===")
	query5 := `
	{
		"selector": {
			"age": {
				"$gt": 25,
				"$lt": 40
			}
		}
	}
	`
	kvs, err = q.ExecuteJSONQuery("db", query5)
	if err != nil {
		fmt.Printf("Failed to execute JSON query, reason: %s\n", err.Error())
		return err
	}
	if kvs == nil {
		fmt.Println("kvs nil")
		return errors.New("kvs nil")
	}

	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})
	if len(kvs) != 4 || kvs[0].Key != "id1" || kvs[1].Key != "id4" || kvs[2].Key != "id5" || kvs[3].Key != "id8" {
		fmt.Println("Query results are not as expected")
		return errors.New("Query results are not as expected")
	}
	fmt.Printf("As expected the keys that returned are: %v\n", kvs)

	fmt.Println("")
	fmt.Println("=== query6 - returns people between the ages of 25 and 45 without those aged 30 or 40 ===")
	query6 := `
	{
		"selector": {
			"age": {
				"$gt": 25,
				"$lt": 45, 
				"$neq": [30,40]
			}
		}
	}
	`
	kvs, err = q.ExecuteJSONQuery("db", query6)
	if err != nil {
		fmt.Printf("Failed to execute JSON query, reason: %s\n", err.Error())
		return err
	}
	if kvs == nil {
		fmt.Println("kvs nil")
		return errors.New("kvs nil")
	}

	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})
	if len(kvs) != 1 || kvs[0].Key != "id5" {
		fmt.Println("Query results are not as expected")
		return errors.New("Query results are not as expected")
	}
	fmt.Printf("As expected the keys that returned are: %v\n", kvs)

	return nil
}

func invalidQuery(session bcdb.DBSession) error {
	q, err := session.Query()
	if err != nil {
		fmt.Printf("Failed to return handler to access bcdb data through JSON query, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("")
	fmt.Println("=== query7 - will fail because the attribute education given in the query condition is not indexed ===")
	query := `
	{
		"selector": {
			"education": {
				"$eq": "bachelor"
			}
		}
	}
	`
	_, err = q.ExecuteJSONQuery("db", query)
	if err != nil {
		fmt.Printf("As expected, failed to execute JSON query, reason: %s\n", err.Error())
	} else {
		return errors.New("Unexpectedly the query did not fail")
	}

	return nil
}
