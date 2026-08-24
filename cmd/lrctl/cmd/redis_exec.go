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

package cmd

import "strings"

// authShellPrefix gates every redis-cli invocation on the pod's OWN $REDIS_PASSWORD env
// var, exactly like debug_dump.go/import.go's pre-existing idiom (mirrored here, not
// reinvented): when it's unset the instance is auth-free and $AUTH stays empty; when set
// it appends `-a $REDIS_PASSWORD --no-auth-warning`. This is the single choke point argv
// construction goes through for every redis-cli exec in this package — see gatherer.go's
// former six unauthenticated call sites and inspect.go's two, which is exactly the LR-041
// shape ("a required value held elsewhere has no enforcement"): fixing debug-dump alone
// left every other exec site silently NOAUTH-blind against an auth-enabled instance.
//
// The password is read by a shell running INSIDE the target container from that
// container's own environment — it is never known to, or passed as a literal from, the
// lrctl process, so it cannot appear in lrctl's own argv/output. It does appear in the
// argv of the `redis-cli` process the script execs *inside that pod's own container*
// (visible to `ps` only within that container's PID namespace, same as debug_dump.go's
// existing usage), which is the same exposure debug-dump/import already accept.
const authShellPrefix = `AUTH=""; [ -n "$REDIS_PASSWORD" ] && AUTH="-a $REDIS_PASSWORD --no-auth-warning";`

// redisCliArgs builds a container-exec argv for a single authenticated redis-cli
// invocation. args are the redis-cli subcommand and its arguments, e.g.
// redisCliArgs("info", "replication") or redisCliArgs("-p", "26379", "sentinel", "master", name).
func redisCliArgs(args ...string) []string {
	return []string{"sh", "-c", authShellPrefix + " redis-cli $AUTH " + shellJoin(args)}
}

// redisCliChainArgs builds a container-exec argv running several authenticated redis-cli
// invocations in sequence, each separated by `&& echo "---" &&`, matching the multi-command
// chains debug_dump.go and inspect.go already build by hand (e.g. INFO replication then
// CLUSTER NODES then CLUSTER INFO in one exec round-trip).
func redisCliChainArgs(cmds ...[]string) []string {
	parts := make([]string, len(cmds))
	for i, c := range cmds {
		parts[i] = "redis-cli $AUTH " + shellJoin(c)
	}
	return []string{"sh", "-c", authShellPrefix + " " + strings.Join(parts, ` && echo "---" && `)}
}

// shellJoin quotes each argument for safe inclusion in a POSIX shell command line
// (single-quoted, with embedded single quotes escaped), then joins them with spaces.
func shellJoin(args []string) string {
	quoted := make([]string, len(args))
	for i, a := range args {
		quoted[i] = "'" + strings.ReplaceAll(a, "'", `'"'"'`) + "'"
	}
	return strings.Join(quoted, " ")
}
