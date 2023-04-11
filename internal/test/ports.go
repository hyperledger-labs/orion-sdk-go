// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"fmt"
	"net"
	"sync"
)

var portMutex sync.Mutex
var nodePortBase uint32 = 32000
var peerPortBase uint32 = 33000

const PortLimit = 1 << 16

func testPort(port uint32) bool {
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	_ = l.Close()
	return err == nil
}

func GetPorts() (nodePort, peerPort uint32) {
	portMutex.Lock()
	defer portMutex.Unlock()

	for nodePortBase < PortLimit && peerPortBase < PortLimit {
		nodePort = nodePortBase
		peerPort = peerPortBase
		nodePortBase++
		peerPortBase++

		if testPort(nodePort) && testPort(peerPort) {
			return
		}
	}

	panic("Could not find available port for testing")
}
