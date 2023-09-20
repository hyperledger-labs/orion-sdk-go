package commands

import (
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type cliConnectionConfig struct {
	ConnectionConfig config.ConnectionConfig `yaml:"connection"`
	SessionConfig    config.SessionConfig    `yaml:"session"`
}

type cliConfigParams struct {
	cliConfigPath string
	cliConfig     cliConnectionConfig
	db            bcdb.BCDB
	session       bcdb.DBSession
}

// CreateDbAndOpenSession read connection and session configurations to create a db instance and open a session with the server.
func (c *cliConfigParams) CreateDbAndOpenSession() error {
	var err error
	if err = c.cliConfig.ReadAndConstructCliConnConfig(c.cliConfigPath); err != nil {
		return errors.Wrapf(err, "failed to read CLI configuration file")
	}

	c.db, err = bcdb.Create(&c.cliConfig.ConnectionConfig)
	if err != nil {
		return errors.Wrapf(err, "failed to instanciate a databse connection")
	}

	c.session, err = c.db.Session(&c.cliConfig.SessionConfig)
	if err != nil {
		return errors.Wrapf(err, "failed to instanciate a databse session")
	}

	return nil
}

// ReadAndConstructCliConnConfig read unmarshal the yaml config file into a cliConnectionConfig object
func (c *cliConnectionConfig) ReadAndConstructCliConnConfig(filePath string) error {
	if filePath == "" {
		return errors.New("path to the shared configuration file is empty")
	}

	v := viper.New()
	v.SetConfigFile(filePath)

	if err := v.ReadInConfig(); err != nil {
		return errors.Wrapf(err, "error reading shared config file: %s", filePath)
	}

	if err := v.UnmarshalExact(c); err != nil {
		return errors.Wrapf(err, "unable to unmarshal shared config file: '%s' into struct", filePath)
	}

	clientLogger, err := logger.New(
		&logger.Config{
			Level:         "debug",
			OutputPath:    []string{"stdout"},
			ErrOutputPath: []string{"stderr"},
			Encoding:      "console",
			Name:          "bcdb-client",
		},
	)
	if err != nil {
		return err
	}
	c.ConnectionConfig.Logger = clientLogger

	return nil
}
