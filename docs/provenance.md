### Provenance API

Provenance API give user access to following BCDB data
- `key->value` history, in different views and directions
- info about users who accessed specific piece of data
- info, including history, about data accessed by user
- history of user transactions

SDK API
```go
// Provenance access to historical data
type Provenance interface {
	// GetHistoricalData return all historical values for specific dn and key
	// Value returned with its associated metadata, including block number, tx index, etc
	GetHistoricalData(dbName, key string) ([]*types.ValueWithMetadata, error)
	// GetHistoricalDataAt returns value for specific version, if exist
	GetHistoricalDataAt(dbName, key string, version *types.Version) (*types.ValueWithMetadata, error)
	// GetPreviousHistoricalData returns value precedes given version, including its metadata, i.e version
	GetPreviousHistoricalData(dbName, key string, version *types.Version) ([]*types.ValueWithMetadata, error)
	// GetNextHistoricalData returns value succeeds given version, including its metadata
	GetNextHistoricalData(dbName, key string, version *types.Version) ([]*types.ValueWithMetadata, error)
	// GetDataReadByUser returns all user reads
	GetDataReadByUser(userID string) ([]*types.KVWithMetadata, error)
	// GetDataWrittenByUser returns all user writes
	GetDataWrittenByUser(userID string) ([]*types.KVWithMetadata, error)
	// GetReaders returns all users who read value associated with the key
	GetReaders(dbName, key string) ([]string, error)
	// GetWriters returns all users who wrote value associated with the key
	GetWriters(dbName, key string) ([]string, error)
	// GetTxIDsSubmittedByUser IDs of all tx submitted by user
	GetTxIDsSubmittedByUser(userID string) ([]string, error)
}
```

### Provenance example
Attached simple code snippet to show how to run all Provenance API from SDK:
```go
	
    // Create db connection
    db, err := bcdb.Create(conConf)

	// Create db session for user
	session, err := db.Session(&c.SessionConfig)

	// Get provenance access object
	provenance, err := session.Provenance()

	...
	
	// Get full history of changes in value of key in database.
	allHistory, err := provenance.GetHistoricalData("db2", "key1")
    
	...
	
	// Get history from specific version
	afterHistory, err := provenance.GetNextHistoricalData("db2", "key2", &types.Version{
            BlockNum: 6,
            TxNum:    0,
        })

	...
	
	// Get history before specific version
	beforeHistory, err := provenance.GetPreviousHistoricalData("db2", "key3", &types.Version{
            BlockNum: 5,
            TxNum:    0,
        })

	...

	// Get historical data for version
	historyVersion, err := provenance.GetHistoricalDataAt("db2", "key1", &types.Version{
            BlockNum: 6,
            TxNum:    0,
        })

	...
	
	// Get history of user transactions
	userTxHistory, err := provenance.GetTxIDsSubmittedByUser("alice")

	...
	
    // Get all user reads
	userReads, err := provenance.GetDataReadByUser("alice")

	...
	
	// Get all user writes 
	userWrites, err := provenance.GetDataWrittenByUser("alice")
	
	...
	
	//Get key readers
	keyReaders, err := provenance.GetReaders("db2", "key1")
	
	...

	//Get key writers
	keyWritess, err := provenance.GetWriters("db2", "key1")
```

For more concrete example, see [provenance.go](../examples/api/provenance/provenance.go)