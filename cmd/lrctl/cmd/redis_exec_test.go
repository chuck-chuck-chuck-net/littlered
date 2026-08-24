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

import (
	"os/exec"
	"strings"
	"testing"
)

// TestRedisCliArgs_ShellsOutWithAuthGate is the red-first assertion for the defect: the
// CLI's exec argv for redis-cli must gate on the pod's OWN $REDIS_PASSWORD, exactly like
// debug_dump.go's authShell idiom, instead of a bare `redis-cli <args>` invocation that
// is silently NOAUTH-blind against an auth-enabled instance (the live t3e finding).
//
// Before the fix, gatherer.go built argv directly as []string{"redis-cli", "info"} with
// no shell and no $AUTH gate at all — this test fails against that shape because argv[0]
// is "redis-cli", not "sh".
func TestRedisCliArgs_ShellsOutWithAuthGate(t *testing.T) {
	argv := redisCliArgs(infoSubcommand, "replication")

	if len(argv) != 3 || argv[0] != "sh" || argv[1] != "-c" {
		t.Fatalf("redisCliArgs(...) = %#v, want a [sh -c <script>] wrapper", argv)
	}
	script := argv[2]

	if !strings.Contains(script, `$REDIS_PASSWORD`) {
		t.Errorf("script %q does not gate on $REDIS_PASSWORD — cannot authenticate against an auth-enabled pod", script)
	}
	if !strings.Contains(script, "--no-auth-warning") {
		t.Errorf("script %q missing --no-auth-warning", script)
	}
	if !strings.Contains(script, "redis-cli $AUTH") {
		t.Errorf("script %q does not invoke redis-cli with the gated $AUTH var", script)
	}
}

// TestRedisCliArgs_QuotesArguments proves that an argument containing shell metacharacters
// (as a Sentinel masterName or similar could) cannot break out of its quoting or be
// misinterpreted, by actually running the produced script through /bin/sh.
func TestRedisCliArgs_QuotesArguments(t *testing.T) {
	argv := redisCliArgs("ECHO", "it's a test; rm -rf /tmp/should-not-run")

	// Replace "redis-cli" with a stub that just echoes its args, so we can run the real
	// script through a real shell without a live Redis server.
	script := strings.Replace(argv[2], "redis-cli $AUTH", "echo STUB $AUTH", 1)

	out, err := exec.Command("sh", "-c", script).CombinedOutput()
	if err != nil {
		t.Fatalf("sh -c failed: %v, output: %s", err, out)
	}
	got := strings.TrimSpace(string(out))
	want := "STUB ECHO it's a test; rm -rf /tmp/should-not-run"
	if got != want {
		t.Errorf("quoting broke argument passing: got %q, want %q", got, want)
	}
}

// TestRedisCliArgs_MutationCheck_AuthOffLeavesArgsBare is the mutation-style guard for the
// auth-free path: with $REDIS_PASSWORD unset, the script must invoke redis-cli with no -a
// flag at all (proving the auth-disabled case is not regressed by the fix). Uses a stub
// redis-cli that dumps its argv so we can inspect exactly what was passed.
func TestRedisCliArgs_MutationCheck_AuthOffLeavesArgsBare(t *testing.T) {
	argv := redisCliArgs(infoSubcommand, "replication")
	script := strings.Replace(argv[2], "redis-cli $AUTH", "echo ARGV:$AUTH:", 1)

	cmd := exec.Command("sh", "-c", script)
	cmd.Env = []string{} // explicitly no REDIS_PASSWORD
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("sh -c failed: %v, output: %s", err, out)
	}
	got := strings.TrimSpace(string(out))
	if got != "ARGV:: info replication" {
		t.Errorf("auth-disabled path was not bare: got %q, want empty $AUTH slot", got)
	}

	// Mutation check: force REDIS_PASSWORD to be set and confirm the same script now
	// DOES inject -a, proving the branch is actually load-bearing and not vacuously true.
	cmd = exec.Command("sh", "-c", script)
	cmd.Env = []string{"REDIS_PASSWORD=secret123"}
	out, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("sh -c failed: %v, output: %s", err, out)
	}
	got = strings.TrimSpace(string(out))
	if !strings.Contains(got, "-a") || !strings.Contains(got, "secret123") {
		t.Errorf("mutation check failed: with REDIS_PASSWORD set, expected -a secret123 in %q", got)
	}
}
