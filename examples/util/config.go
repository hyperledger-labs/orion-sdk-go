// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type Config struct {
	ConnectionConfig config.ConnectionConfig
	SessionConfig    config.SessionConfig
}

func ReadConfig(configFilePath string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(configFilePath)

	if err := v.ReadInConfig(); err != nil {
		return nil, errors.Wrap(err, "error reading the config file")
	}

	c := &Config{}
	if err := v.UnmarshalExact(c); err != nil {
		return nil, errors.Wrap(err, "error while unmarshaling config")
	}

	return c, nil
}
