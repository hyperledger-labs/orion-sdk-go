// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package commands

import (
	"encoding/base64"
	"fmt"

	"github.com/hyperledger-labs/orion-server/pkg/crypto"
)

const (
	CarRecordKeyPrefix         = "car~"
	MintRequestRecordKeyPrefix = "mint-request~"
)

type MintRequestRecord struct {
	Dealer          string
	CarRegistration string
}

func (r *MintRequestRecord) Key() string {
	return MintRequestRecordKeyPrefix + r.RequestID()
}

func (r *MintRequestRecord) RequestID() string {
	str := r.Dealer + "_" + r.CarRegistration
	sha256Hash, _ := crypto.ComputeSHA256Hash([]byte(str))
	return base64.URLEncoding.EncodeToString(sha256Hash)
}

type CarRecord struct {
	Owner           string
	CarRegistration string
}

func (r *CarRecord) String() string {
	return fmt.Sprintf("{CarRegistration: %s, Owner: %s}", r.CarRegistration, r.Owner)
}

func (r *CarRecord) Key() string {
	return CarRecordKeyPrefix + r.CarRegistration
}
