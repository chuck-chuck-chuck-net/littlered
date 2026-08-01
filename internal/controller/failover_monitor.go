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
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/event"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// ============================================================================
// Failover-mode master watcher (ADR-011 §4, M5).
//
// A per-instance background goroutine — the failover-mode replacement for
// sentinel_monitor.go's +switch-master subscription as the FAST path. It
// probes the current master IP on a ~1s cadence with a ProbeTimeout-bounded
// INFO and, when a failure streak crosses downAfterMilliseconds, pushes ONE
// GenericEvent onto the shared mode-agnostic reconcile-trigger channel
// (r.monitorEvents) so a reconcile looks immediately instead of at the steady
// requeue interval.
//
// The watcher NEVER decides topology (LR-016): declaring the master dead —
// with its kubelet-readiness evidence and replica-link corroboration — stays
// exclusively with planMasterDeath inside reconcile. Firing early costs one
// wasted reconcile; firing never replaces the reconcile-side detection window.
//
// Probe-target source: the watcher re-resolves status.master.ip from the
// (informer-cached) CR on EVERY tick — it has no subscription to learn
// topology changes from, and a 1s tick makes per-tick resolution trivially
// fresh: after a completed failover the next tick probes the new IP, and a
// master-IP change re-arms the streak (no stale probes leak across masters).
// status.master.ip mirrors intent+observation, so during a transition it is
// empty and the watcher idles — exactly the phase where reconcile is already
// fast-requeueing. downAfterMilliseconds is read from spec on the same
// per-tick Get, so a spec change is picked up within one tick (~1s + informer
// lag); an in-flight streak keeps its start time and is judged against the
// new window on the next tick.
// ============================================================================

// failoverMonitorInterval is the watcher's probe cadence (ADR-011 §4: ~1s).
const failoverMonitorInterval = time.Second

// failoverProbeStreak is the watcher's per-instance probe-failure streak state
// (pure data; all decisions live in advanceFailoverProbeStreak).
type failoverProbeStreak struct {
	// masterIP is the IP the streak was observed against; a change re-arms.
	masterIP string
	// failingSince is the first failed probe of the current streak (zero when
	// the last probe succeeded / there is nothing to probe).
	failingSince time.Time
	// fired records that the event for this streak was already pushed.
	fired bool
}

// advanceFailoverProbeStreak is the watcher's pure fire/re-arm decision
// (ADR-011 §4): given the previous streak state and the current observation,
// it returns the next state and whether to push the accelerating event NOW.
//
//   - No master IP (bootstrap/transition): full reset, never fire.
//   - Probe OK: healthy, streak cleared, fire re-armed.
//   - Master IP changed: the streak restarts against the new IP — evidence
//     gathered against the old master never carries over.
//   - Probe failed: the streak starts on the first failure and fires exactly
//     once when now-failingSince >= downAfter (hysteresis: once fired, the
//     streak stays silent until a success or IP change re-arms it, so a dead
//     master does not push an event every second).
//
// Time is injected; the function does no I/O.
func advanceFailoverProbeStreak(prev failoverProbeStreak, masterIP string, probeOK bool, now time.Time, downAfter time.Duration) (failoverProbeStreak, bool) {
	if masterIP == "" {
		return failoverProbeStreak{}, false
	}
	if probeOK {
		return failoverProbeStreak{masterIP: masterIP}, false
	}
	next := prev
	if prev.masterIP != masterIP || prev.failingSince.IsZero() {
		next = failoverProbeStreak{masterIP: masterIP, failingSince: now}
	}
	if !next.fired && now.Sub(next.failingSince) >= downAfter {
		next.fired = true
		return next, true
	}
	return next, false
}

// ensureFailoverMonitor ensures the background master watcher is running for
// the given failover-mode instance (the reconcileFailover analog of
// ensureSentinelMonitor, same dedupe/kill-switch contract).
func (r *LittleRedReconciler) ensureFailoverMonitor(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) {
	log := r.getLogger(ctx, littleRed, LogCategoryRecon)
	nn := types.NamespacedName{Name: littleRed.Name, Namespace: littleRed.Namespace}

	// Annotation kill switch — exact sentinel-mode parity (the e2e suite's
	// polling-only tier depends on it).
	if littleRed.Annotations[AnnotationDisableEventMonitoring] == annotationValueTrue {
		log.Info("Failover event monitoring disabled via annotation")
		r.stopFailoverMonitor(nn)
		return
	}

	// No event channel means the reconciler was constructed without
	// SetupWithManager (unit/envtest): the watcher's only output does not
	// exist, so there is nothing to accelerate — reconcile-cadence detection
	// still works. This also keeps envtest reconciles from leaking probe
	// goroutines.
	if r.monitorEvents == nil {
		return
	}

	r.monitorsMu.Lock()
	defer r.monitorsMu.Unlock()

	if _, exists := r.failoverMonitors[nn]; exists {
		return
	}

	log.Info("Starting failover master watcher")

	// Detached from the reconcile's ctx, like the sentinel monitor: the
	// watcher outlives individual reconciles and is stopped via
	// stopFailoverMonitor (deletion, mode change, annotation).
	monCtx, cancel := context.WithCancel(context.Background())
	r.failoverMonitors[nn] = cancel

	go r.monitorFailoverMaster(monCtx, littleRed)
}

// stopFailoverMonitor stops the background master watcher for the given
// instance (idempotent; no-op when none is running).
func (r *LittleRedReconciler) stopFailoverMonitor(nn types.NamespacedName) {
	r.monitorsMu.Lock()
	defer r.monitorsMu.Unlock()

	if cancel, exists := r.failoverMonitors[nn]; exists {
		cancel()
		delete(r.failoverMonitors, nn)
	}
}

// monitorFailoverMaster is the watcher goroutine: probe the current master
// every tick, advance the pure streak state, push at most one GenericEvent per
// failure streak. Exits cleanly on context cancel.
func (r *LittleRedReconciler) monitorFailoverMaster(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) {
	log := r.getLogger(ctx, littleRed, LogCategoryRecon)
	nn := types.NamespacedName{Name: littleRed.Name, Namespace: littleRed.Namespace}

	// Password fetch copied from monitorSentinel: once per goroutine start,
	// with a bounded setup context. A secret rotation is picked up when the
	// watcher is restarted (same staleness contract as the sentinel monitor).
	password := ""
	if littleRed.Spec.Auth.Enabled {
		setupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		secret := &corev1.Secret{}
		err := r.Get(setupCtx, types.NamespacedName{
			Name:      littleRed.Spec.Auth.ExistingSecret,
			Namespace: littleRed.Namespace,
		}, secret)

		if err == nil {
			password = string(secret.Data["password"])
		} else {
			log.Error(err, "Failed to get auth secret for failover master watcher")
			// Continue like the sentinel monitor: probes will fail closed
			// (streak fires, reconcile decides with its own full gather).
		}
	}

	ticker := time.NewTicker(failoverMonitorInterval)
	defer ticker.Stop()

	var streak failoverProbeStreak
	for {
		select {
		case <-ctx.Done():
			log.Info("Stopping failover master watcher")
			return
		case <-ticker.C:
		}

		// Re-resolve the probe target and window per tick (informer-cached
		// read — no API round trip). See the file header for why per-tick
		// resolution replaces the sentinel monitor's subscription.
		latest := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, nn, latest); err != nil {
			// Deleted or cache hiccup: idle this tick. Deletion also cancels
			// this context via stopFailoverMonitor.
			streak = failoverProbeStreak{}
			continue
		}
		masterIP := ""
		if latest.Status.Master != nil {
			masterIP = latest.Status.Master.IP
		}
		downAfter := time.Duration(failoverSpecOrDefault(latest).DownAfterMilliseconds) * time.Millisecond

		probeOK := false
		if masterIP != "" {
			probeOK = r.probeFailoverMaster(ctx, masterIP, password, latest.Spec.TLS.Enabled)
		}

		var fire bool
		streak, fire = advanceFailoverProbeStreak(streak, masterIP, probeOK, time.Now(), downAfter)
		if !fire {
			continue
		}

		log.Info("Failover master watcher: master unreachable past downAfterMilliseconds, triggering reconcile",
			"masterIP", masterIP, "failingSince", streak.failingSince.Format(time.RFC3339), "downAfter", downAfter.String())
		select {
		case r.monitorEvents <- event.GenericEvent{
			Object: &littleredv1alpha1.LittleRed{
				ObjectMeta: ctrl.ObjectMeta{
					Name:      littleRed.Name,
					Namespace: littleRed.Namespace,
				},
			},
		}:
		case <-ctx.Done():
			log.Info("Stopping failover master watcher")
			return
		}
	}
}

// probeFailoverMaster is one ProbeTimeout-bounded INFO probe of the master
// (ADR-003a hardening: INFO, not bare PING, so a wedged-but-accepting master
// counts as down; LR-017 discipline: a blackholing IP fails in <= ProbeTimeout).
func (r *LittleRedReconciler) probeFailoverMaster(ctx context.Context, masterIP, password string, tlsEnabled bool) bool {
	pctx, cancel := context.WithTimeout(ctx, redisclient.ProbeTimeout)
	defer cancel()
	addr := fmt.Sprintf("%s:%d", masterIP, littleredv1alpha1.RedisPort)
	_, err := redisclient.GetReplicationInfo(pctx, addr, password, tlsEnabled)
	return err == nil
}
