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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/event"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	"github.com/littlered-operator/littlered-operator/internal/redis"
)

// ensureSentinelMonitor ensures that a background monitor is running for the given LittleRed instance
func (r *LittleRedReconciler) ensureSentinelMonitor(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) {
	log := r.getLogger(ctx, littleRed, LogCategoryRecon)
	nn := types.NamespacedName{Name: littleRed.Name, Namespace: littleRed.Namespace}

	if littleRed.Annotations[AnnotationDisableEventMonitoring] == annotationValueTrue {
		log.Info("Sentinel event monitoring disabled via annotation")
		r.stopSentinelMonitor(nn)
		return
	}

	// No event channel means the reconciler was constructed without
	// SetupWithManager (unit/envtest): the monitor's only output does not
	// exist, so there is nothing to accelerate — reconcile-cadence detection
	// still works. This also keeps envtest reconciles from leaking
	// subscription goroutines. (Parity with ensureFailoverMonitor.)
	if r.monitorEvents == nil {
		log.V(1).Info("No event channel wired (reconciler built without SetupWithManager), skipping Sentinel monitor")
		return
	}

	r.monitorsMu.Lock()
	defer r.monitorsMu.Unlock()

	// If already monitoring, do nothing
	if _, exists := r.monitors[nn]; exists {
		return
	}

	log.Info("Starting Sentinel monitor")

	// Create context for cancellation
	ctx, cancel := context.WithCancel(context.Background())
	r.monitors[nn] = cancel

	// Start monitor in background
	go r.monitorSentinel(ctx, littleRed)
}

// stopSentinelMonitor stops the background monitor for the given LittleRed instance
func (r *LittleRedReconciler) stopSentinelMonitor(nn types.NamespacedName) {
	r.monitorsMu.Lock()
	defer r.monitorsMu.Unlock()

	if cancel, exists := r.monitors[nn]; exists {
		cancel()
		delete(r.monitors, nn)
	}
}

// monitorSentinel runs the main loop for monitoring Sentinel events
func (r *LittleRedReconciler) monitorSentinel(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) {
	log := r.getLogger(ctx, littleRed, LogCategoryRecon)
	stateLog := r.getLogger(ctx, littleRed, LogCategoryState)

	// Get password if auth is enabled
	password := ""
	if littleRed.Spec.Auth.Enabled {
		// We need to fetch the secret. Note: We use a new context because the passed ctx might be cancelled
		// However, for the monitor loop, we want to respect the cancellation.
		// For the initial setup, we can use a separate timeout context.
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
			log.Error(err, "Failed to get auth secret for sentinel monitor")
			// We continue, maybe auth isn't actually enforced or we'll retry later
		}
	}

	// Retry loop
	for {
		select {
		case <-ctx.Done():
			log.Info("Stopping Sentinel monitor")
			return
		default:
			// Refresh addresses on each retry to catch new pods
			addresses := r.getSentinelAddresses(ctx, littleRed)
			sentinelClient := redis.NewSentinelClient(addresses, password, littleRed.Spec.TLS.Enabled)

			// +switch-master is the event we care about
			log.Info("Connecting to Sentinel for monitoring", "addresses", addresses)

			// Use a dedicated context for the subscription so we can cancel it if the parent ctx is cancelled
			subCtx, cancelSub := context.WithCancel(ctx)
			msgChan, cleanup, err := sentinelClient.Subscribe(subCtx, "+switch-master")

			if err != nil {
				log.Error(err, "Failed to subscribe to Sentinel, retrying in 10s")
				cancelSub()
				select {
				case <-ctx.Done():
					return
				case <-time.After(10 * time.Second):
					continue
				}
			}

			log.Info("Subscribed to Sentinel events")

			// Process messages
			for msg := range msgChan {
				stateLog.Info("Received Sentinel event", "channel", msg.Channel, "payload", msg.Payload)

				// Trigger reconciliation. Cancel-guarded like the failover
				// watcher's send: if the consumer is gone (manager shut down),
				// a bare send would block this goroutine forever.
				log.Info("Triggering reconciliation via Sentinel event")
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
					cleanup()
					cancelSub()
					log.Info("Stopping Sentinel monitor")
					return
				}
			}

			cleanup() // Close connection
			cancelSub()
			log.Info("Sentinel subscription ended, reconnecting...")
			time.Sleep(5 * time.Second) // Wait before reconnecting
		}
	}
}
