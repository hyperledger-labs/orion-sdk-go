// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package bcdb_test

import (
	"fmt"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestSafeCharsForTxID(t *testing.T) {
	type testCase struct {
		name string
		id   string
		pass bool
	}

	// from https://www.ietf.org/rfc/rfc3986.txt
	// pchar         = unreserved / pct-encoded / sub-delims / ":" / "@"
	// unreserved  = ALPHA / DIGIT / "-" / "." / "_" / "~"
	// sub-delims  = "!" / "$" / "&" / "'" / "(" / ")" / "*" / "+" / "," / ";" / "="

	num := "1234567890"
	lower := "abcdefghijklmnopqrstuvwxyz"
	upper := strings.ToUpper(lower)
	unreserverd := "-._~"
	prct := "%"
	subDelims := "!$&'()*+,;="
	subDelimsPlus := ":@"

	allSafe := num + lower + upper + unreserverd + prct + subDelims + subDelimsPlus

	testCases := []testCase{
		{
			name: "alpha-numeric",
			id:   num + lower + upper,
			pass: true,
		},
		{
			name: "unreserved",
			id:   unreserverd,
			pass: true,
		},
		{
			name: "sub-delims",
			id:   subDelims,
			pass: true,
		},
		{
			name: "sub-delims-plus",
			id:   subDelimsPlus,
			pass: true,
		},
		{
			name: "percent-encoded",
			id:   "has%20%20space",
			pass: true,
		},
		{
			name: "non-ascii - utf8",
			id:   string([]byte{0xe2, 0x8c, 0x98}),
			pass: false,
		},
		{
			name: "non-printable",
			id:   string([]byte{0xbd, 0xb2}),
			pass: false,
		},
		{
			name: "space",
			id:   " \t\n",
			pass: false,
		},
	}

	// every byte other than the safe
	for i := 0; i <= 0xff; i++ {
		s := string([]byte{byte(i)})
		if !strings.Contains(allSafe, s) {
			testCases = append(testCases,
				testCase{
					name: fmt.Sprintf("unsafe-%q", s),
					id:   s,
					pass: false,
				})
		}
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			err := bcdb.SafeCharsForTxID(tt.id)
			if tt.pass {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), "TxID contains un-safe characters:")
			}
		})
	}
}
