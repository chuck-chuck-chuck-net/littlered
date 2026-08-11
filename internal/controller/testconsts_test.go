/*
Copyright 2026 The littlered Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

// Shared string constants for the controller-package unit tests. Centralised here so
// repeated literals (pod IPs, StatefulSet/pod names, node/replication IDs) are named
// once rather than scattered across the recovery-plan test tables.
const (
	ipMaster  = "10.0.0.1"
	ipReplica = "10.0.0.2"
	ipNode3   = "10.0.0.3"
	ipNode5   = "10.0.0.5"
	ipNode7   = "10.0.0.7"
	ipNode9   = "10.0.0.9"
	ipTest    = "1.1.1.1"
	roleSlave = "slave"

	podRedis0 = "r-redis-0"
	podRedis1 = "r-redis-1"
	podRedis2 = "r-redis-2"

	stsMrCluster0 = "mr-cluster-0"
	stsMrCluster1 = "mr-cluster-1"
	stsMrShard00  = "mr-shard-0-0"
	stsCShard00   = "c-shard-0-0"
	stsCShard10   = "c-shard-1-0"

	testNodeID0 = "id0"
	testReplid0 = "716d42"
	testReplid1 = "1cc4b7"
	testReplidA = "AAA"
	testReplidB = "BBB"

	diskTypeSSD = "ssd"
)
