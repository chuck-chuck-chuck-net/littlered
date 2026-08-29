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

package redis

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"syscall"

	goredis "github.com/redis/go-redis/v9"
)

// ProbeFailure classifies WHY a probe of a pod failed (LR-051).
//
// The whole point of this type is that `Reachable: false` conflated three
// different facts — "cannot route", "process dead", "wrong credential" — and
// different rules must act differently on each. In particular a node the operator
// cannot AUTHENTICATE to is emphatically not a node that holds no data: it
// answered, in the protocol, to say we are not allowed in. Because DataHolders()
// filters on Reachable, such a node read as "0 keys, not a data holder" and
// silently voided Rule L's >=2-holder REFUSE — the one gate that exists to stop
// the operator discarding data. See the auth design §3.5/§3.5a and LR-051.
//
// Only ProbeAuthFailed changes a decision. The other values are diagnostic, and
// exist so the enum is closed and an auth failure can never be quietly absorbed
// into a neighbouring bucket.
type ProbeFailure string

const (
	// ProbeOK: no failure. The zero value, so a state built without a probe error
	// reads as "nothing went wrong", which is what every existing caller means.
	ProbeOK ProbeFailure = ""

	// ProbeUnroutable: the address did not accept a connection — refused, no route,
	// network unreachable. On a cluster whose dead pod IPs RST fast (local kubeadm)
	// this is what a departed pod looks like.
	ProbeUnroutable ProbeFailure = "Unroutable"

	// ProbeTimedOut: the probe hit its deadline — the LR-017 blackhole shape, where
	// a dead pod IP swallows packets rather than answering. Named ProbeTimedOut, not
	// ProbeTimeout, because ProbeTimeout is already this package's 3s duration.
	ProbeTimedOut ProbeFailure = "Timeout"

	// ProbeAuthFailed: we reached a live server and it refused our credential. THE
	// load-bearing value: such a pod is a live process that may be holding the only
	// copy of the data, so it must veto every decision that discards data.
	ProbeAuthFailed ProbeFailure = "AuthFailed"

	// ProbeProtocolError: we reached it, spoke the protocol, and got an answer we
	// cannot use — a non-auth server error reply (e.g. LOADING, an unknown command)
	// or a reply we could not parse.
	ProbeProtocolError ProbeFailure = "ProtocolError"

	// ProbeUnknown: an error none of the above explains. Deliberately its own value
	// rather than being folded into ProbeProtocolError: reporting "we could not
	// parse the answer" for something that never got an answer is a lie, and this
	// type exists to stop exactly that kind of conflation.
	ProbeUnknown ProbeFailure = "Unknown"
)

// authFailureMarkers are the server replies that mean "your credential is wrong or
// unexpected", matched case-insensitively as substrings.
//
// The list is exhaustive for the three directions this operator can produce:
//   - NOAUTH — the DISABLE direction: we hold "", the pod enforces a password.
//   - WRONGPASS — the ROTATION direction: we hold the new password, the pod has the old.
//   - "ERR Client sent AUTH, but no password is set" — a two-argument AUTH against a
//     nopass server.
//   - "ERR invalid password" — the older single-password form of WRONGPASS.
//
// There is deliberately NO marker for the ENABLEMENT direction, because it produces
// no failure at all: go-redis sends the THREE-argument `HELLO <ver> AUTH default
// <pw>`, which hits ACL's nopass short-circuit and succeeds against a password-less
// server (auth design §0, source- and lab-confirmed). Classifying is about being
// correct on what is seen, not about manufacturing a failure that does not occur.
var authFailureMarkers = []string{
	"noauth",
	"wrongpass",
	"client sent auth, but no password is set",
	"invalid password",
}

// unroutableMarkers are the textual forms of a refused/unroutable address, used as
// a fallback for errors that carry no unwrapped syscall errno (go-redis flattens
// its dial-retry exhaustion into a string).
var unroutableMarkers = []string{
	"connection refused",
	"no route to host",
	"network is unreachable",
	"no such host",
}

// ClassifyProbeError maps a probe error onto the closed ProbeFailure enum.
//
// Order matters and is not arbitrary: AUTH is checked FIRST, because an auth
// failure is the only classification that changes a decision and because it can
// arrive wrapped in almost anything — as a proto.RedisError off the wire, as a
// go-redis handshake error, or (on lrctl's exec path) as redis-cli's stdout in a
// plain error. Structured checks are preferred everywhere they exist; the string
// matches are fallbacks for the surfaces that flatten their causes, and they are
// substring matches because every layer between here and the socket adds a prefix.
func ClassifyProbeError(err error) ProbeFailure {
	if err == nil {
		return ProbeOK
	}
	msg := strings.ToLower(err.Error())

	// 1. Authentication. Checked before everything else — see above.
	for _, marker := range authFailureMarkers {
		if strings.Contains(msg, marker) {
			return ProbeAuthFailed
		}
	}

	// 2. Deadlines. context.DeadlineExceeded is what LR-017's ProbeTimeout produces;
	// os.ErrDeadlineExceeded is what a socket deadline produces; net.Error.Timeout()
	// covers the rest.
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return ProbeTimedOut
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return ProbeTimedOut
	}

	// 3. Routing. Structured errno first, then the flattened string forms.
	for _, errno := range []syscall.Errno{syscall.ECONNREFUSED, syscall.EHOSTUNREACH, syscall.ENETUNREACH} {
		if errors.Is(err, errno) {
			return ProbeUnroutable
		}
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return ProbeUnroutable
	}

	// 4. The string fallbacks, timeout before routing so "i/o timeout" is not read
	// as a routing failure by a later marker.
	if strings.Contains(msg, "i/o timeout") || strings.Contains(msg, "context deadline exceeded") {
		return ProbeTimedOut
	}
	for _, marker := range unroutableMarkers {
		if strings.Contains(msg, marker) {
			return ProbeUnroutable
		}
	}

	// 5. We reached it and it answered with something unusable.
	var redisErr goredis.Error
	if errors.As(err, &redisErr) {
		return ProbeProtocolError
	}

	return ProbeUnknown
}

// DescribeProbeError renders a probe error for an operator-facing message, bounded
// so a condition message can never be dominated by one pod's error text.
//
// The raw text is carried (rather than only the classification) because the whole
// value of the OperatorCannotAuthenticate condition is that it says what the SERVER
// said: "WRONGPASS" and "NOAUTH" point at opposite remedies (the Secret was rotated
// under a running fleet, versus the operator lost a password the pods still
// enforce), and a bare "AuthFailed" leaves the reader to guess which. Redis auth
// error replies never echo the credential, so nothing secret is surfaced.
func DescribeProbeError(err error) string {
	if err == nil {
		return ""
	}
	const maxLen = 160
	msg := strings.TrimSpace(err.Error())
	if len(msg) > maxLen {
		msg = msg[:maxLen] + "…"
	}
	return msg
}
