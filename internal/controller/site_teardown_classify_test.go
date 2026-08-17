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

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// The delete path used to put three states in one bucket named "unreachable" and
// label the condition "SiteUnreachable". Unlike the reconcile path LR-034 fixed,
// here the consequence is that the CR can never be deleted — and two of the three
// states will never succeed no matter how long the operator retries. These tests
// pin the classification and the message's honesty. The envtest repro in
// multisite_teardown_test.go drives the same thing through the real reconciler;
// this tier is the fast, repeatable guard (CLAUDE.md §7 test discipline).

// Fixture site names, one per block class, so a failing assertion says which
// class it was about.
const (
	siteCfgFault   = "s-cfg"
	siteSweepFail  = "s-swp"
	siteUnreach    = "s-unr"
	causeIOTimeout = "i/o timeout"
)

// TestClassifySiteClientBlock: a failure to build the site client is a hub-local
// CONFIGURATION fault whenever siteClient marked it as one, and unreachability
// only otherwise.
func TestClassifySiteClientBlock(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantReason string
	}{
		{
			name:       "secret unreadable is configuration",
			err:        siteConfigFault(siteBeta, siteFaultSecretUnreadable, errors.New(`secrets "kc" not found`)),
			wantReason: reasonTeardownConfigFault,
		},
		{
			name:       "secret malformed is configuration",
			err:        siteConfigFault(siteBeta, siteFaultSecretMalformed, errors.New("no kubeconfig key")),
			wantReason: reasonTeardownConfigFault,
		},
		{
			name:       "operator namespace unresolvable is configuration",
			err:        siteConfigFault(siteBeta, siteFaultOperatorNamespace, errors.New("no downward API")),
			wantReason: reasonTeardownConfigFault,
		},
		{
			// The classification must survive wrapping: callers up the stack add
			// context, and errors.As is what makes that safe.
			name:       "wrapped configuration fault is still configuration",
			err:        fmt.Errorf("building site client: %w", siteConfigFault(siteBeta, siteFaultSecretUnreadable, errors.New("nope"))),
			wantReason: reasonTeardownConfigFault,
		},
		{
			name:       "anything else is unreachability",
			err:        errors.New("dial tcp 10.2.1.10:6443: i/o timeout"),
			wantReason: reasonTeardownSiteUnreachable,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classifySiteClientBlock(siteBeta, tc.err)
			if got.reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", got.reason, tc.wantReason)
			}
			if got.site != siteBeta {
				t.Errorf("site = %q, want %q", got.site, siteBeta)
			}
			// The cause must survive: it is the only thing that tells an operator
			// WHICH Secret, and it is what the condition's Details render.
			if got.cause == "" {
				t.Error("cause must carry the underlying error text")
			}
		})
	}
}

// TestSweepBlockReason: a sweep failure is only "the site answered and refused"
// when the site actually answered. Claiming otherwise about a dial timeout would
// be the same species of false statement this change removes.
func TestSweepBlockReason(t *testing.T) {
	gr := schema.GroupResource{Group: "apps", Resource: "statefulsets"}

	tests := []struct {
		name       string
		err        error
		wantReason string
	}{
		{
			name:       "transport failure never reached the API server",
			err:        &url.Error{Op: "Get", URL: "https://10.2.1.10:6443/apis", Err: &net.DNSError{IsTimeout: true}},
			wantReason: reasonTeardownSiteUnreachable,
		},
		{
			name:       "server timeout is ask-again-later",
			err:        apierrors.NewServerTimeout(gr, "list", 1),
			wantReason: reasonTeardownSiteUnreachable,
		},
		{
			name:       "service unavailable is ask-again-later",
			err:        apierrors.NewServiceUnavailable("apiserver is shutting down"),
			wantReason: reasonTeardownSiteUnreachable,
		},
		{
			name:       "forbidden means the site answered and refused",
			err:        apierrors.NewForbidden(gr, "lr-shard-0", errors.New("no delete verb")),
			wantReason: reasonTeardownSweepFailed,
		},
		{
			name:       "admission rejection means the site answered and refused",
			err:        apierrors.NewInternalError(errors.New("admission webhook denied the request")),
			wantReason: reasonTeardownSweepFailed,
		},
		{
			name:       "conflict means the site answered",
			err:        apierrors.NewConflict(gr, "lr-shard-0", errors.New("object was modified")),
			wantReason: reasonTeardownSweepFailed,
		},
		{
			// sweepSite wraps every error with site/object context, so the
			// classification has to see through fmt.Errorf.
			name:       "wrapped transport failure is still unreachability",
			err:        fmt.Errorf(`site "beta": listing %T: %w`, nil, &url.Error{Op: "Get", URL: "u", Err: &net.DNSError{IsTimeout: true}}),
			wantReason: reasonTeardownSiteUnreachable,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := sweepBlockReason(tc.err); got != tc.wantReason {
				t.Errorf("sweepBlockReason = %q, want %q", got, tc.wantReason)
			}
		})
	}
}

// TestSummarizeSiteTeardownBlocks: the aggregate keeps spec order, splits the
// classes, and picks the condition reason by actionability.
func TestSummarizeSiteTeardownBlocks(t *testing.T) {
	cfg := siteTeardownBlock{site: siteCfgFault, reason: reasonTeardownConfigFault, cause: "secret missing"}
	swp := siteTeardownBlock{site: siteSweepFail, reason: reasonTeardownSweepFailed, cause: "forbidden"}
	unr := siteTeardownBlock{site: siteUnreach, reason: reasonTeardownSiteUnreachable, cause: causeIOTimeout}

	t.Run("no blocks is not blocked", func(t *testing.T) {
		s := summarizeSiteTeardownBlocks(nil)
		if s.Any() {
			t.Fatal("Any() must be false with no blocks")
		}
		if msg := describeSiteTeardownBlocks(s); msg != "" {
			t.Errorf("message = %q, want empty", msg)
		}
	})

	t.Run("splits the classes and keeps spec order", func(t *testing.T) {
		s := summarizeSiteTeardownBlocks([]siteTeardownBlock{unr, cfg, swp})
		if want := []string{siteUnreach, siteCfgFault, siteSweepFail}; !equalStrings(s.Blocked, want) {
			t.Errorf("Blocked = %v, want %v (spec order)", s.Blocked, want)
		}
		if !equalStrings(s.ConfigFaulted, []string{siteCfgFault}) {
			t.Errorf("ConfigFaulted = %v", s.ConfigFaulted)
		}
		if !equalStrings(s.SweepFailed, []string{siteSweepFail}) {
			t.Errorf("SweepFailed = %v", s.SweepFailed)
		}
		if !equalStrings(s.Unreachable, []string{siteUnreach}) {
			t.Errorf("Unreachable = %v", s.Unreachable)
		}
		if len(s.Details) != 3 {
			t.Fatalf("Details = %v, want one line per block", s.Details)
		}
	})

	// The reason is what an alert rule matches on, so precedence is behaviour, not
	// cosmetics: a pass that is PARTLY permanent must not advertise the class that
	// might fix itself.
	t.Run("reason precedence is by actionability", func(t *testing.T) {
		tests := []struct {
			name   string
			blocks []siteTeardownBlock
			want   string
		}{
			{"config alone", []siteTeardownBlock{cfg}, reasonTeardownConfigFault},
			{"sweep alone", []siteTeardownBlock{swp}, reasonTeardownSweepFailed},
			{"unreachable alone", []siteTeardownBlock{unr}, reasonTeardownSiteUnreachable},
			{"config beats sweep", []siteTeardownBlock{swp, cfg}, reasonTeardownConfigFault},
			{"config beats unreachable", []siteTeardownBlock{unr, cfg}, reasonTeardownConfigFault},
			{"sweep beats unreachable", []siteTeardownBlock{unr, swp}, reasonTeardownSweepFailed},
			{"all three", []siteTeardownBlock{unr, swp, cfg}, reasonTeardownConfigFault},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				if got := summarizeSiteTeardownBlocks(tc.blocks).Reason(); got != tc.want {
					t.Errorf("Reason() = %q, want %q", got, tc.want)
				}
			})
		}
	})
}

// TestDescribeSiteTeardownBlocks: the message must name the real cause, must not
// promise self-healing it cannot deliver, and must not deny it where it applies.
func TestDescribeSiteTeardownBlocks(t *testing.T) {
	const neverClears = "will NOT clear on its own"
	const mayClear = "may clear on its own"

	t.Run("configuration fault points at the hub, not the site", func(t *testing.T) {
		s := summarizeSiteTeardownBlocks([]siteTeardownBlock{
			{site: siteBeta, reason: reasonTeardownConfigFault, cause: `secrets "site-beta-kubeconfig" not found`},
		})
		msg := describeSiteTeardownBlocks(s)
		for _, want := range []string{
			siteBeta,
			"kubeconfig Secret",
			"operator's namespace",
			neverClears,
			multiSiteFinalizerName,
			`site-beta-kubeconfig`, // the Details line, i.e. WHICH Secret
		} {
			if !strings.Contains(msg, want) {
				t.Errorf("message must contain %q; got: %s", want, msg)
			}
		}
		if strings.Contains(msg, mayClear) {
			t.Errorf("a configuration fault must not be described as possibly self-healing: %s", msg)
		}
	})

	t.Run("unreachable site keeps the honest maybe and its escape hatch", func(t *testing.T) {
		s := summarizeSiteTeardownBlocks([]siteTeardownBlock{
			{site: siteBeta, reason: reasonTeardownSiteUnreachable, cause: causeIOTimeout},
		})
		msg := describeSiteTeardownBlocks(s)
		if !strings.Contains(msg, mayClear) {
			t.Errorf("an unreachable site may recover and the message must say so: %s", msg)
		}
		if strings.Contains(msg, neverClears) {
			t.Errorf("must not claim an unreachable site is permanent: %s", msg)
		}
		// The documented escape hatch for a permanently dead site predates this
		// change and must survive it.
		if !strings.Contains(msg, "permanently gone") || !strings.Contains(msg, multiSiteFinalizerName) {
			t.Errorf("the dead-site escape hatch must remain documented in the message: %s", msg)
		}
	})

	t.Run("sweep failure blames the delete, not the credential", func(t *testing.T) {
		s := summarizeSiteTeardownBlocks([]siteTeardownBlock{
			{site: siteBeta, reason: reasonTeardownSweepFailed, cause: "statefulsets.apps is forbidden"},
		})
		msg := describeSiteTeardownBlocks(s)
		if !strings.Contains(msg, "the site answered") || !strings.Contains(msg, neverClears) {
			t.Errorf("message = %s", msg)
		}
		if strings.Contains(msg, "kubeconfig Secret") {
			t.Errorf("a sweep failure is not a credential problem and must not send the reader there: %s", msg)
		}
	})

	t.Run("a mixed pass describes every class it holds", func(t *testing.T) {
		s := summarizeSiteTeardownBlocks([]siteTeardownBlock{
			{site: siteCfgFault, reason: reasonTeardownConfigFault, cause: "secret missing"},
			{site: siteUnreach, reason: reasonTeardownSiteUnreachable, cause: causeIOTimeout},
		})
		msg := describeSiteTeardownBlocks(s)
		for _, want := range []string{siteCfgFault, siteUnreach, neverClears, mayClear} {
			if !strings.Contains(msg, want) {
				t.Errorf("message must contain %q; got: %s", want, msg)
			}
		}
	})

	// A condition message is a bounded API field; the sweep runs against every
	// site every pass, so this must not be able to grow without limit.
	t.Run("message fits the condition field", func(t *testing.T) {
		const sites = 32
		blocks := make([]siteTeardownBlock, 0, sites)
		for i := range sites {
			blocks = append(blocks, siteTeardownBlock{
				site:   fmt.Sprintf("site-%02d", i),
				reason: reasonTeardownConfigFault,
				cause:  strings.Repeat("x", 200),
			})
		}
		msg := describeSiteTeardownBlocks(summarizeSiteTeardownBlocks(blocks))
		if len(msg) > maxConditionMessageBytes {
			t.Errorf("message is %d bytes, over the %d-byte condition limit", len(msg), maxConditionMessageBytes)
		}
	})
}

// maxConditionMessageBytes is the API server's limit on metav1.Condition.Message.
const maxConditionMessageBytes = 32768

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
