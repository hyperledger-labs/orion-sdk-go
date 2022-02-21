// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package test

import "sync"

var portMutex sync.Mutex
var nodePortBase uint32 = 32000
var peerPortBase uint32 = 33000

func GetPorts() (nodePort, peerPort uint32) {
	portMutex.Lock()
	defer portMutex.Unlock()

	nodePort = nodePortBase
	peerPort = peerPortBase
	nodePortBase++
	peerPortBase++

	return
}
