//go:build e2e
// +build e2e

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

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
	"github.com/littlered-operator/littlered-operator/test/utils"
)

// ADR-013 — in-place legacy → per-shard cluster migration, end to end.
//
// The feature: upgrading the 0.3 operator over a pre-0.3 single-StatefulSet cluster
// ({name}-cluster, pods {name}-cluster-N) no longer refuses (ADR-007 §5's terminal
// LegacyClusterTopology) — it drives an online, data-safe, in-cluster migration into
// the 0.3 per-shard layout ({name}-shard-K, pods {name}-shard-K-M) on the SAME running
// Redis Cluster.
//
// Mechanism (LR-025, replicate-then-failover — ADR-013 Decision 4, §10): the migration
// does NOT move slots. Each new pod joins as a slot-less REPLICATE of the legacy master
// owning its range and full-syncs; then {name}-shard-K-0 is promoted by a coordinated
// CLUSTER FAILOVER — an atomic ownership flip to an already-synced node whose old master
// demotes to a live replica of it. Phases:
//
//	Standup → Meet → Replicate → Failover → Decommission → Complete.
//
// A new node reaches the slot-owning state only after a clean, already-redundant handoff,
// so a new master always has a synced replica to fail over to (the demoted legacy master,
// then its own new replicas) and never trips the startup STEP-3 guard (LR-003). No -ASK/
// -MOVED churn during the transfer — new nodes sync as invisible replicas, and each shard
// sees exactly ONE atomic -MOVED at its failover.
//
// The harness's distinctive need is a REAL legacy layout to migrate FROM. The CRD is
// byte-identical pre-0.3 vs 0.3 (ADR-013 Context 1: the split touched only workloads,
// never spec.cluster.shards/replicasPerShard), so the only way to produce the legacy
// {name}-cluster single STS is to run the PRE-SPLIT operator (git ref 85e1a93^, the last
// single-STS commit) against a normal cluster CR. This test therefore deploys TWO operator
// images in sequence:
//
//  1. pre-split image  — bootstrap the legacy single-STS cluster + seed data;
//  2. migration image  — this branch (the git-hash-tagged image the suite already built),
//     redeployed over the same CR with no spec change, to drive the migration.
//
// REQUIRED external capability (escape hatch — see the milestone report): the pre-split
// image must be BUILT and PUSHED out-of-band from git ref 85e1a93^ (a different working
// tree than this branch) and its reference supplied via LEGACY_OPERATOR_IMAGE=<repo>:<tag>.
// Building a different git ref cannot happen from inside a Go test; this harness only
// *deploys* an already-published image (make deploy IMG=...). Absent the env var the spec
// self-skips with an actionable message rather than silently mis-running.
//
// Heavy + opt-in (the two-image redeploy is disruptive): Label("extended","migration").
// It is excluded by the default `!extended` filter; run it explicitly with
// `make test-e2e LABEL_FILTER='migration'` (see the Makefile E2E label convention).
var _ = Describe("Cluster Legacy→Per-Shard In-Place Migration (ADR-013)", Label("extended", "migration", "cluster"), Ordered, func() {

	expectedNodes := clusterTotalNodes(clusterReplicasPerShard)

	// WS2 isolation guardrails (ADR-013). This tier DEPLOYS and UPGRADES operators
	// itself (two-image sequence). It therefore MUST own a dedicated cluster; it must
	// never be pointed at a cluster carrying unrelated work (the "ms-smoke incident").
	// These guards run once, before any migration deploy, and fail the suite FAST.
	// They are target-agnostic (Kind / k3s / VM / prod) — not hard-wired to Kind.
	BeforeAll(func() { enforceDedicatedClusterGuards() })

	// --- default (opt-out) migration: upgrade drives to Complete, data + topology intact ---
	Context("default migration drives to Complete", Ordered, func() {
		var crName string
		var dataset map[string]string

		BeforeAll(func() {
			crName = fmt.Sprintf("mig-default-%d", time.Now().Unix())
			AddReportEntry("cr:" + crName)

			bootstrapLegacyCluster(crName, expectedNodes)

			By("seeding a known dataset spanning all shards through a legacy seed node")
			dataset = writeDatasetSpanningShards(legacyPod(crName), 50)
			Expect(dataset).To(HaveLen(clusterShards * 50))
		})

		AfterAll(func() {
			restoreMigrationOperator() // leave the suite on the migration image regardless of outcome
			if debugOnFailure && suiteOrSpecFailed() {
				By("skipping cleanup to allow debugging")
				return
			}
			By("cleaning up cluster CR")
			_, _ = utils.Run(exec.Command("kubectl", "delete", "littlered", crName,
				"-n", testNamespace, "--ignore-not-found", "--timeout=2m"))
		})

		It("migrates in place to the per-shard layout, preserving data and topology", func() {
			By("upgrading to the migration-capable operator image (same CR, no spec change)")
			// This is the migration TRIGGER (ADR-013 Consequences): the moment the 0.3
			// migration operator sees the legacy {name}-cluster STS it enters migration mode.
			upgradeToMigrationOperator()

			By("the shared Service must be shard-agnostic (fronts legacy AND new pods)")
			// Deterministic half of the coexistence claim (ADR-013 §8 / DESIGN §3): the shared
			// headless Service {name}-cluster selects component=cluster (shard-agnostic), so it
			// fronts legacy {name}-cluster-N AND new {name}-shard-K-M pods during the window.
			assertSharedServiceCoexistence(crName)

			By("sampling the data plane THROUGHOUT the migration window (records transient blips; does not fail on them)")
			// LR-025 replicate-then-failover has NO slot move: new nodes are invisible slot-less
			// replicas until their shard's single atomic CLUSTER FAILOVER, so the only client-
			// visible event per shard is ONE -MOVED at that failover (a client following the
			// redirect re-reads cleanly). We therefore expect FAR fewer blips than the superseded
			// reshard path (no cluster-require-full-coverage CLUSTERDOWN window, no per-slot -ASK
			// churn), but still RECORD rather than fail on any transient — the data-safety teeth
			// are the downstream verifyDataset + cluster_state:ok + recovery assertions. The
			// sampler runs the FULL window so it overlaps every per-shard failover.
			sampler := startCoexistenceSampler(crName, dataset)

			By("waiting for the operator to drive migration all the way to Complete")
			waitMigrationComplete(crName)
			sampler.stopAndReport()

			By("(a) all seeded keys must survive the migration, byte-for-byte")
			verifyDataset(clusterMasterPod(crName, clusterShards-1), dataset)

			By("(b) cluster_state must be ok with all 16384 slots assigned")
			Eventually(func(g Gomega) {
				out, err := redisExec(testNamespace, clusterMasterPod(crName, 0), "CLUSTER", "INFO")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring("cluster_state:ok"))
				g.Expect(out).To(ContainSubstring("cluster_slots_assigned:16384"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("(c) the legacy {name}-cluster STS must be gone and N per-shard STSs must exist")
			expectPerShardLayout(crName)

			By("(d) lrctl-verify colocation must pass (operator status matches Redis, shards colocated)")
			// verifyClusterTopologySync is the suite's colocation/verify assertion (same check
			// lrctl verify performs: status↔ground-truth agreement + the ADR-007/LR-020
			// shard-colocation invariant that each Redis shard lives in ONE StatefulSet).
			verifyClusterTopologySync(testNamespace, crName, expectedNodes)
		})
	})

	// --- hold escape hatch: annotation parks migration (non-mutating) until removed ---
	Context("hold annotation parks migration until removed", Ordered, func() {
		var crName string
		var dataset map[string]string

		BeforeAll(func() {
			crName = fmt.Sprintf("mig-hold-%d", time.Now().Unix())
			AddReportEntry("cr:" + crName)

			bootstrapLegacyCluster(crName, expectedNodes)

			By("seeding a known dataset spanning all shards")
			dataset = writeDatasetSpanningShards(legacyPod(crName), 20)
			Expect(dataset).To(HaveLen(clusterShards * 20))
		})

		AfterAll(func() {
			restoreMigrationOperator()
			if debugOnFailure && suiteOrSpecFailed() {
				By("skipping cleanup to allow debugging")
				return
			}
			By("cleaning up cluster CR")
			_, _ = utils.Run(exec.Command("kubectl", "delete", "littlered", crName,
				"-n", testNamespace, "--ignore-not-found", "--timeout=2m"))
		})

		It("does not mutate while held, then proceeds when the annotation is cleared", func() {
			By("setting the hold annotation BEFORE the upgrade (ADR-013 §3)")
			_, err := utils.Run(exec.Command("kubectl", "annotate", "littlered", crName,
				"-n", testNamespace,
				"redis.chuck-chuck-chuck.net/migrate-legacy-sts=hold", "--overwrite"))
			Expect(err).NotTo(HaveOccurred())

			By("upgrading to the migration-capable operator image")
			upgradeToMigrationOperator()

			By("the operator must report the held state (Ready=False, reason MigrationHeld)")
			Eventually(func(g Gomega) {
				reason, err := getConditionField(crName, "Ready", "reason")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(reason).To(Equal("MigrationHeld"))
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("status.status must be Migrating while held")
			Eventually(func(g Gomega) {
				out, err := utils.Run(exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.status}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(out)).To(Equal("Migrating"))
			}, 1*time.Minute, 5*time.Second).Should(Succeed())

			By("while held the operator MUST NOT mutate: legacy STS untouched, no per-shard STS created")
			// The hold branch stamps only status (no ensureMigrationResources), so no {name}-shard-K
			// StatefulSet must appear and the legacy {name}-cluster STS must remain. Consistently
			// over a window catches an operator that would mutate a beat later.
			Consistently(func(g Gomega) {
				g.Expect(stsExists(clusterStatefulSetName(crName))).To(BeTrue(),
					"legacy StatefulSet was removed while migration is held")
				g.Expect(stsExists(fmt.Sprintf("%s-shard-0", crName))).To(BeFalse(),
					"a per-shard StatefulSet was created while migration is held")
			}, 45*time.Second, 5*time.Second).Should(Succeed())

			By("removing the hold annotation — migration must now proceed to Complete")
			_, err = utils.Run(exec.Command("kubectl", "annotate", "littlered", crName,
				"-n", testNamespace,
				"redis.chuck-chuck-chuck.net/migrate-legacy-sts-")) // trailing '-' deletes the annotation
			Expect(err).NotTo(HaveOccurred())

			waitMigrationComplete(crName)

			By("data + topology intact after the resumed migration")
			verifyDataset(clusterMasterPod(crName, clusterShards-1), dataset)
			expectPerShardLayout(crName)
			verifyClusterTopologySync(testNamespace, crName, expectedNodes)
		})
	})

	// --- LR-025 restart-during-migration chaos tier: the headline regression guard ---
	//
	// This is the whole point of the replicate-then-failover redesign. While a migration
	// is in flight we crash the redis CONTAINER of a new per-shard master {name}-shard-K-0
	// (a kill-9 of PID 1 — EmptyDir + nodes.conf survive, IP preserved; NOT a pod delete,
	// which would wipe EmptyDir and take a different path). We crash it in BOTH windows:
	//   (A) pre-failover — while K-0 is a slot-less synced replica of the legacy master; and
	//   (B) post-failover — while K-0 OWNS its slot range (a master with the demoted legacy
	//       master as its live replica) — the exact state that deadlocked under the old path.
	// Assertions (must ALWAYS hold, even though the precise instant is opportunistic):
	//   - the crashed pod does NOT wedge in a CrashLoopBackOff deadlock (it restarts and its
	//     redis comes back reachable and rejoins the cluster);
	//   - no seeded key is lost (byte-for-byte survivor check);
	//   - migration still reaches Complete, cluster_state:ok, colocation passes.
	//
	// WHY THIS WOULD FAIL AGAINST THE OLD RESHARD MECHANISM (the red-meaningful part):
	// the superseded Draining path MOVED slots onto a bare {name}-shard-K-0 that owned slots
	// on EmptyDir with NO replica for the whole drain. A container restart there trips the
	// startup STEP-3 guard (LR-003): the restarted slot-owner yields waiting for a replica to
	// CLUSTER FAILOVER TAKEOVER, finds none, times out → parks forever → CrashLoopBackOff; and
	// the just-migrated keys, already MIGRATE-deleted from the source, are gone. Replicate-then-
	// failover keeps K-0 slot-less until an atomic, already-redundant handoff, so at every
	// instant the shard has >=2 live copies and a restarted K-0 ALWAYS has a synced replica
	// (the demoted legacy master, then its own new replicas) for STEP-3's TAKEOVER breaker to
	// promote — no deadlock, no loss. Run against redis:7.4.0 (E2E_REDIS_IMAGE) for fidelity to
	// the version that first exposed the hole on s1.
	//
	// SECOND REGRESSION GUARDED HERE (MIGRATION_CHAOS_SELF_REPLICATE_DEADLOCK): crash (B) can let
	// Redis natively fail shard K's range over to the NEW REPLICA pod {name}-shard-K-1 instead of
	// back to K-0, so a new-side pod ends up OWNING its range mid-migration. The Replicate planner
	// used to compute "replicate the current range owner" for that pod and emit REPLICATE <self>
	// (ERR Can't replicate myself) forever — waitMigrationComplete below would time out. It is now
	// fixed in the pure planner (a node that already owns its range is settled, never a replicate
	// target) and guarded deterministically by internal/redis unit tests (TestPlanReplicateNever-
	// TargetsSelf + the "replicate CHAOS" table case). This e2e reproduces it only opportunistically
	// (the native-failover-to-K-1 window is timing/env-sensitive: seen on scm-s2, not on s1).
	Context("restart during migration must not deadlock or lose data (LR-025)", Ordered, func() {
		var crName string
		var dataset map[string]string
		const victimShard = 0 // shard 0 fails over first (lowest K), so K-0 owns slots earliest

		BeforeAll(func() {
			crName = fmt.Sprintf("mig-chaos-%d", time.Now().Unix())
			AddReportEntry("cr:" + crName)

			bootstrapLegacyCluster(crName, expectedNodes)

			By("seeding a known dataset spanning all shards through a legacy seed node")
			dataset = writeDatasetSpanningShards(legacyPod(crName), 50)
			Expect(dataset).To(HaveLen(clusterShards * 50))
		})

		AfterAll(func() {
			restoreMigrationOperator()
			if debugOnFailure && suiteOrSpecFailed() {
				By("skipping cleanup to allow debugging")
				return
			}
			By("cleaning up cluster CR")
			_, _ = utils.Run(exec.Command("kubectl", "delete", "littlered", crName,
				"-n", testNamespace, "--ignore-not-found", "--timeout=2m"))
		})

		It("survives a {name}-shard-K-0 container crash mid-migration (no deadlock, no data loss)", func() {
			victim := clusterMasterPod(crName, victimShard) // {name}-shard-0-0

			By("upgrading to the migration-capable operator image (the migration TRIGGER)")
			upgradeToMigrationOperator()

			// --- Crash (A): pre-failover, while K-0 is a slot-less synced replica --------------
			// By strict-precedence the plan finishes ALL replication before ANY failover, so once
			// K-0 is a running cluster member it is (still) a slot-less replica — a wide, reliable
			// window. Under the OLD mechanism a restart here was survivable too (K-0 had no slots
			// yet), so this crash mainly proves the sync restarts cleanly; crash (B) is the teeth.
			By(fmt.Sprintf("waiting for the new master %s to join as a slot-less replica (Replicate window)", victim))
			waitClusterMemberRunning(crName, victim)
			roleA := migVictimClusterRole(crName, victim)
			AddReportEntry(fmt.Sprintf("crashA-role=%s legacySTS=%v", roleA, stsExists(clusterStatefulSetName(crName))))
			crashContainerAssertRecovers(crName, victim, "A (pre-failover)")

			// --- Crash (B): post-failover, while K-0 OWNS its slot range -----------------------
			// The exact old-bug state: a restarted new master owning slots. It survives ONLY
			// because the demoted legacy master (its live replica) lets STEP-3's TAKEOVER breaker
			// promote a replica so K-0 can rejoin — the replicate-then-failover guarantee.
			By(fmt.Sprintf("waiting for %s to OWN its slot range (post-failover, the old-bug state)", victim))
			waitVictimOwnsRange(crName, victim, victimShard)
			AddReportEntry(fmt.Sprintf("crashB legacySTS=%v", stsExists(clusterStatefulSetName(crName))))
			crashContainerAssertRecovers(crName, victim, "B (post-failover, owns slots)")

			By("migration must still be driven all the way to Complete after the crashes")
			waitMigrationComplete(crName)

			By("(no data loss) every seeded key must survive the crashes + migration, byte-for-byte")
			verifyDataset(clusterMasterPod(crName, clusterShards-1), dataset)

			By("(healthy) cluster_state ok with all 16384 slots assigned")
			Eventually(func(g Gomega) {
				out, err := redisExec(testNamespace, clusterMasterPod(crName, clusterShards-1), "CLUSTER", "INFO")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring("cluster_state:ok"))
				g.Expect(out).To(ContainSubstring("cluster_slots_assigned:16384"))
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("(topology) per-shard layout + lrctl-verify colocation intact")
			expectPerShardLayout(crName)
			verifyClusterTopologySync(testNamespace, crName, expectedNodes)
		})
	})
})

// =============================================================================
// Migration-specific helpers
// =============================================================================

// enforceDedicatedClusterGuards is the WS2 refuse-to-run safety net. The migration tier
// deploys/upgrades operators on its own (pre-split image → migration image over the SAME CR),
// so it MUST own a dedicated cluster. Two guards, both BEFORE any migration deploy, both
// target-agnostic (they gate Kind/k3s/VM/prod alike — nothing here is Kind-specific).
//
// They deliberately behave DIFFERENTLY, because they detect different things:
//
//  1. SKIP_OPERATOR_DEPLOY must NOT be set — and this one SKIPS. That flag means "reuse an
//     already-installed operator", the exact opposite of what this tier needs: this tier IS the
//     operator lifecycle (pre-split operator → migration operator → restore). But the caller
//     ASKED for that flag, so the tier is *unrunnable here*, not *failing* — which is what Skip
//     means. It also makes this precondition consistent with the tier's other environmental one:
//     an unset LEGACY_OPERATOR_IMAGE already skips (preSplitOperatorImage). Failing on one
//     environmental precondition and skipping on the other was incoherent, and a red on a
//     legitimate configuration teaches readers to scroll past reds.
//
//  2. No OTHER littlered operator may already be installed anywhere in the target cluster.
//     We scan ALL namespaces for a littlered controller Deployment (matched by the operator's
//     own chart labels: app.kubernetes.io/name=littlered AND control-plane=controller-manager)
//     and refuse if any exists that is NOT this suite's own operator (operatorNamespace/littlered,
//     deployed by BeforeSuite). A foreign operator ⇒ this is a shared cluster ⇒ refuse.
//
// Placed in a Describe-level BeforeAll so it runs once, before either Context's BeforeAll (and
// thus before bootstrapLegacyCluster's first deploy). The suite's own operator is deployed by
// BeforeSuite ahead of this and is explicitly excluded from guard 2.
func enforceDedicatedClusterGuards() {
	By("WS2 guard: SKIP_OPERATOR_DEPLOY is set, so this tier cannot own operator deployment")
	if v := os.Getenv("SKIP_OPERATOR_DEPLOY"); v == "true" || v == "1" || v == "yes" {
		Skip("SKIP_OPERATOR_DEPLOY=" + v + " — the ADR-013 migration e2e cannot run here: this tier " +
			"MUST own operator deployment (it deploys the pre-split operator, upgrades to the " +
			"migration operator, and restores it), and that flag asks for the opposite. Unset " +
			"SKIP_OPERATOR_DEPLOY and run against a dedicated cluster to exercise this tier.")
	}

	// Guard 2 stays a hard FAIL, deliberately. Unlike guard 1 this is not something the caller
	// asked for — it detects a condition they probably do not know about, and proceeding would
	// trample a foreign operator on a shared cluster. Skipping would hide a hazard; failing names it.
	By("WS2 guard: refusing to run if a foreign littlered operator is already installed in the cluster")
	out, err := utils.Run(exec.Command("kubectl", "get", "deployments", "--all-namespaces",
		"-l", "app.kubernetes.io/name=littlered,control-plane=controller-manager",
		"-o", "jsonpath={range .items[*]}{.metadata.namespace}/{.metadata.name}{\"\\n\"}{end}"))
	Expect(err).NotTo(HaveOccurred(), "failed to scan the cluster for existing littlered operators")

	ownOperator := operatorNamespace + "/littlered" // this suite's own operator (BeforeSuite)
	var foreign []string
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		nn := strings.TrimSpace(line)
		if nn == "" || nn == ownOperator {
			continue
		}
		foreign = append(foreign, nn)
	}
	Expect(foreign).To(BeEmpty(),
		"refusing to run: a littlered operator is already installed (%s); this e2e requires a "+
			"dedicated cluster (it deploys/upgrades operators cluster-wide and would collide with "+
			"unrelated work). Point it at a throwaway cluster.", strings.Join(foreign, ", "))
}

// legacyPod returns the pre-0.3 single-STS cluster seed pod name ({crName}-cluster-0).
func legacyPod(crName string) string {
	return fmt.Sprintf("%s-cluster-0", crName)
}

// clusterStatefulSetName returns the legacy single StatefulSet name ({crName}-cluster).
// Mirrors internal/controller resources.clusterStatefulSetName (not importable from the
// e2e package); the two are pinned to the same shape by the layout assertions below.
func clusterStatefulSetName(crName string) string {
	return crName + "-cluster"
}

// preSplitOperatorImage returns the pre-split (git 85e1a93^) operator image reference, or
// self-skips the spec if it was not supplied. Building that ref is out-of-band (a different
// git working tree); this harness only deploys an already-published image.
func preSplitOperatorImage() string {
	ref := os.Getenv("LEGACY_OPERATOR_IMAGE")
	if ref == "" {
		Skip("LEGACY_OPERATOR_IMAGE not set — the ADR-013 migration e2e needs the PRE-SPLIT " +
			"operator image (built + pushed out-of-band from git ref 85e1a93^) to bootstrap a real " +
			"legacy single-STS cluster. Set LEGACY_OPERATOR_IMAGE=<repo>:<tag> to run this tier.")
	}
	return ref
}

// migrationOperatorImage returns this branch's migration-capable operator image reference.
// `make run-test-e2e` exports OPERATOR_IMAGE (= the git-hash-tagged image the suite built
// and pushed in BeforeSuite), so the "upgrade" step redeploys exactly that image.
func migrationOperatorImage() string {
	ref := os.Getenv("OPERATOR_IMAGE")
	if ref == "" {
		Skip("OPERATOR_IMAGE not set — cannot determine the migration operator image to upgrade to " +
			"(normally exported by `make run-test-e2e`).")
	}
	return ref
}

// deployOperatorImage (re)deploys the operator Helm release pinned to a specific image
// reference (repo:tag) via `make deploy IMG=...`, then waits for that image to be rolled
// out and Ready. This is the two-image capability the migration harness needs; it mirrors
// deployOperator() in e2e_suite_test.go but targets an explicit image.
func deployOperatorImage(imageRef string) {
	By("deploying operator image " + imageRef)
	cmd := exec.Command("make", "deploy", "IMG="+imageRef)
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "failed to deploy operator image %s", imageRef)
	waitForOperatorImage(imageRef)
}

// waitForOperatorImage waits until the operator Deployment has rolled out the given image
// and is Ready (updatedReplicas==1, availableReplicas==1, observedGeneration current) AND the
// previous operator pod is fully gone. A plain availableReplicas check (waitForOperator) can
// pass on the OLD pod mid-rollout, which would race the upgrade.
//
// WS2 live-run finding: checking the Deployment's DESIRED image (.spec.template...image) is not
// enough — helm upgrade flips the spec instantly, and the Deployment's status counters are
// transiently still the OLD ReplicaSet's (replicas/updated/available all 1/1/1 for the old pod)
// before the controller reacts, so the very first poll passed while the OLD operator was still
// the running leader. During the two-image handoff (migration <-> pre-split operator) that old
// operator, still holding the leader lease through the surge rollout, reconciled the freshly
// created CR into the WRONG topology (a per-shard layout under the "pre-split" step, or vice
// versa). The robust gate is the actual RUNNING pods: require that EVERY operator pod runs the
// target image (a terminating old-image pod is still listed until fully gone) and at least one
// exists. Once the old pod is gone its process cannot reconcile, closing the race.
func waitForOperatorImage(imageRef string) {
	Eventually(func(g Gomega) {
		image, err := utils.Run(exec.Command("kubectl", "get", "deployment", "littlered",
			"-n", operatorNamespace, "-o", "jsonpath={.spec.template.spec.containers[*].image}"))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(image).To(ContainSubstring(imageRef), "operator Deployment still on the previous image")

		gen, err := utils.Run(exec.Command("kubectl", "get", "deployment", "littlered",
			"-n", operatorNamespace, "-o", "jsonpath={.metadata.generation}"))
		g.Expect(err).NotTo(HaveOccurred())
		observed, err := utils.Run(exec.Command("kubectl", "get", "deployment", "littlered",
			"-n", operatorNamespace, "-o", "jsonpath={.status.observedGeneration}"))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(observed)).To(Equal(strings.TrimSpace(gen)))

		updated, err := utils.Run(exec.Command("kubectl", "get", "deployment", "littlered",
			"-n", operatorNamespace, "-o", "jsonpath={.status.updatedReplicas}"))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(updated)).To(Equal("1"))

		avail, err := utils.Run(exec.Command("kubectl", "get", "deployment", "littlered",
			"-n", operatorNamespace, "-o", "jsonpath={.status.availableReplicas}"))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(avail)).To(Equal("1"))

		// The decisive gate: every RUNNING operator pod must be on the target image. This is
		// what actually rules out the old operator still reconciling during the lease handover.
		podImages, err := utils.Run(exec.Command("kubectl", "get", "pods",
			"-n", operatorNamespace, "-l", "control-plane=controller-manager",
			"-o", "jsonpath={range .items[*]}{.spec.containers[0].image}{\"\\n\"}{end}"))
		g.Expect(err).NotTo(HaveOccurred())
		running := strings.Fields(strings.TrimSpace(podImages))
		g.Expect(running).NotTo(BeEmpty(), "no operator pods running yet")
		for _, img := range running {
			g.Expect(img).To(ContainSubstring(imageRef),
				"an operator pod still runs a previous image (%s) — handoff not complete", img)
		}
	}, 3*time.Minute, 5*time.Second).Should(Succeed(), "operator did not roll out image %s", imageRef)
}

// upgradeToMigrationOperator redeploys this branch's migration-capable operator image.
func upgradeToMigrationOperator() { deployOperatorImage(migrationOperatorImage()) }

// restoreMigrationOperator ensures the suite is left on the migration operator image, so a
// subsequent tier does not inherit the pre-split operator. Idempotent (helm upgrade no-op if
// already there). Mirrors the reshard tier's `scaleOperator(1)` cleanup discipline.
func restoreMigrationOperator() {
	if os.Getenv("OPERATOR_IMAGE") == "" {
		return
	}
	deployOperatorImage(os.Getenv("OPERATOR_IMAGE"))
}

// bootstrapLegacyCluster deploys the PRE-SPLIT operator, creates a shape-preserving cluster
// CR, waits for it Running, and asserts the LEGACY single-STS layout materialized.
func bootstrapLegacyCluster(crName string, expectedNodes int) {
	By("deploying the pre-split operator to seed a real legacy single-STS cluster")
	deployOperatorImage(preSplitOperatorImage())

	By(fmt.Sprintf("creating a %d-shard / %d-replica cluster CR (%d nodes)%s",
		clusterShards, clusterReplicasPerShard, expectedNodes, redisImageDescription()))
	cr := clusterCR(crName, clusterReplicasPerShard, "", redisImageSpecFields()+`  resources:
    requests:
      cpu: "100m"
      memory: "128Mi"
    limits:
      cpu: "100m"
      memory: "128Mi"
`)
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(cr)
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())

	By("waiting for the legacy cluster to be Running")
	Eventually(func(g Gomega) {
		g.Expect(getPhase(crName)).To(Equal("Running"))
	}, 6*time.Minute, 5*time.Second).Should(Succeed())

	By("asserting the LEGACY layout exists ({name}-cluster single STS, pods {name}-cluster-N)")
	expectLegacyLayout(crName, expectedNodes)
}

// redisImageSpecFields returns a `spec.image` YAML block for E2E_REDIS_IMAGE, or "" when the
// env var is unset (in which case the operator default — Redis 8.4.2 — applies). spec.image
// flows to BOTH the pre-split bootstrap pods AND the migration per-shard pods (the migration
// operator redeploys over the SAME CR with no spec change), so source and destination nodes
// run the intended version.
//
// LR-025 note: replicate-then-failover is VERSION-AGNOSTIC — it uses only MEET/REPLICATE/
// coordinated FAILOVER/FORGET, with no slot move, so there is no ASM-vs-dance capability
// branch to exercise (unlike the superseded reshard path). E2E_REDIS_IMAGE is retained for
// REGRESSION FIDELITY: pin it to redis:7.4.0 to reproduce the exact version that deadlocked
// and lost data under the old mechanism on s1 (the environment that drove this redesign). The
// operator-default 8.4.2 run is a cheap cross-version confirmation.
//
//	Regression run: E2E_REDIS_IMAGE=redis:7.4.0
//	Default run:    E2E_REDIS_IMAGE unset (redis:8.4.2)
func redisImageSpecFields() string {
	ref := os.Getenv("E2E_REDIS_IMAGE")
	if ref == "" {
		return ""
	}
	reg, path, tag := parseImageRef(ref)
	return fmt.Sprintf("  image:\n    registry: %s\n    path: %s\n    tag: %q\n    pullPolicy: IfNotPresent\n",
		reg, path, tag)
}

// redisImageDescription is a short suffix for the CR-creation By() line noting the redis image.
func redisImageDescription() string {
	if ref := os.Getenv("E2E_REDIS_IMAGE"); ref != "" {
		return " with redis image " + ref
	}
	return " with the operator-default redis image"
}

// parseImageRef splits a Docker-style image reference into (registry, path, tag) using the same
// defaults as api/v1alpha1.ImageSpec (docker.io / library/redis / 8.4.2). It handles the two
// forms this suite uses — a bare "redis:TAG" (→ docker.io/library/redis:TAG) and an explicit
// "host[:port]/path:TAG" — so E2E_REDIS_IMAGE can name either an official image tag or a full ref.
func parseImageRef(ref string) (registry, path, tag string) {
	registry, path, tag = "docker.io", "library/redis", "8.4.2"

	name := ref
	// A ':' that comes AFTER the last '/' delimits the tag (a ':' before it is a registry port).
	if i := strings.LastIndex(ref, ":"); i > strings.LastIndex(ref, "/") {
		name = ref[:i]
		tag = ref[i+1:]
	}

	if i := strings.Index(name, "/"); i >= 0 {
		first := name[:i]
		// A leading segment that looks like a hostname (contains '.' or ':') or is "localhost"
		// is the registry; otherwise the whole thing is the image path (e.g. "library/redis").
		if strings.ContainsAny(first, ".:") || first == "localhost" {
			registry = first
			path = name[i+1:]
		} else {
			path = name
		}
	} else {
		// Bare official image name (e.g. "redis") → docker.io library namespace.
		path = "library/" + name
	}
	return registry, path, tag
}

// expectLegacyLayout asserts the pre-0.3 shape: one {crName}-cluster StatefulSet with all
// nodes Ready, legacy pods {crName}-cluster-N present, and NO per-shard {crName}-shard-K STS.
func expectLegacyLayout(crName string, expectedNodes int) {
	Eventually(func(g Gomega) {
		g.Expect(stsExists(clusterStatefulSetName(crName))).To(BeTrue(),
			"legacy single StatefulSet %s not found", clusterStatefulSetName(crName))

		ready, err := utils.Run(exec.Command("kubectl", "get", "statefulset",
			clusterStatefulSetName(crName), "-n", testNamespace,
			"-o", "jsonpath={.status.readyReplicas}"))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(ready)).To(Equal(fmt.Sprintf("%d", expectedNodes)))

		// No per-shard STS must exist under the pre-split operator.
		for k := 0; k < clusterShards; k++ {
			g.Expect(stsExists(fmt.Sprintf("%s-shard-%d", crName, k))).To(BeFalse(),
				"unexpected per-shard StatefulSet under the pre-split operator")
		}
	}, 2*time.Minute, 5*time.Second).Should(Succeed())

	By("confirming the legacy pods {name}-cluster-N are present and cluster_state is ok")
	Eventually(func(g Gomega) {
		out, err := redisExec(testNamespace, legacyPod(crName), "CLUSTER", "INFO")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(out).To(ContainSubstring("cluster_state:ok"))
		g.Expect(out).To(ContainSubstring("cluster_slots_assigned:16384"))
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

// expectPerShardLayout asserts the 0.3 end state: the legacy {crName}-cluster STS is GONE,
// N per-shard {crName}-shard-K StatefulSets exist each with 1+replicasPerShard Ready pods,
// and status.cluster.migration has been cleared.
func expectPerShardLayout(crName string) {
	Eventually(func(g Gomega) {
		g.Expect(stsExists(clusterStatefulSetName(crName))).To(BeFalse(),
			"legacy StatefulSet %s should be deleted at Decommission", clusterStatefulSetName(crName))

		for k := 0; k < clusterShards; k++ {
			name := fmt.Sprintf("%s-shard-%d", crName, k)
			g.Expect(stsExists(name)).To(BeTrue(), "per-shard StatefulSet %s missing", name)
			ready, err := utils.Run(exec.Command("kubectl", "get", "statefulset", name,
				"-n", testNamespace, "-o", "jsonpath={.status.readyReplicas}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(ready)).To(Equal(fmt.Sprintf("%d", 1+clusterReplicasPerShard)))
		}

		// Monitoring surface cleared on Complete (driver sets migration=nil).
		mig, err := utils.Run(exec.Command("kubectl", "get", "littlered", crName,
			"-n", testNamespace, "-o", "jsonpath={.status.cluster.migration.phase}"))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(mig)).To(BeEmpty(), "status.cluster.migration should be cleared after Complete")
	}, 3*time.Minute, 5*time.Second).Should(Succeed())
}

// waitMigrationComplete waits until the operator has driven migration to Complete: phase
// back to Running, legacy STS gone, and the migration monitoring status cleared. Generous
// timeout consistent with the reshard/wipe tiers (the plan is strict-precedence and paced
// one coordinated failover per shard per reconcile, and each new pod full-syncs first).
func waitMigrationComplete(crName string) {
	Eventually(func(g Gomega) {
		g.Expect(getPhase(crName)).To(Equal("Running"))
		g.Expect(stsExists(clusterStatefulSetName(crName))).To(BeFalse(),
			"legacy StatefulSet still present — migration not yet Complete")
	}, 15*time.Minute, 10*time.Second).Should(Succeed(),
		"operator did not drive the legacy cluster to the per-shard layout")
}

// =============================================================================
// LR-025 chaos-tier helpers (restart-during-migration)
// =============================================================================

// migLegacySeedPod returns a stable topology-query pod for the chaos windows: a legacy
// pod that stays a cluster member until Decommission. CLUSTER NODES read from it gives a
// full mesh view (legacy + new nodes) throughout the migration.
func migLegacySeedPod(crName string) string { return legacyPod(crName) }

// migVictimClusterRole returns the victim's role ("master"/"replica") as seen in the mesh's
// CLUSTER NODES (queried from a legacy seed), or "" if the victim is not yet a member. Keyed
// by the victim's pod IP (cluster-announce-ip == POD_IP, strict IP identity).
func migVictimClusterRole(crName, victim string) string {
	role, _, present := migClusterNodeInfo(crName, victim)
	if !present {
		return ""
	}
	return role
}

// migClusterNodeInfo parses CLUSTER NODES (from a legacy seed) for the line whose address is
// the victim's pod IP, returning its role, whether it owns any slots, and whether it is
// present at all. A node owns slots iff its line carries a trailing slot field (fields >= 9).
func migClusterNodeInfo(crName, victim string) (role string, ownsSlots, present bool) {
	ip := podIP(victim)
	if ip == "" {
		return "", false, false
	}
	out, err := redisExec(testNamespace, migLegacySeedPod(crName), "CLUSTER", "NODES")
	if err != nil {
		return "", false, false
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 8 {
			continue
		}
		// fields[1] is ip:port@cbusport — match the announced pod IP.
		addr := fields[1]
		if i := strings.IndexAny(addr, ":@"); i >= 0 {
			addr = addr[:i]
		}
		if addr != ip {
			continue
		}
		role = "master"
		if strings.Contains(fields[2], "slave") {
			role = "replica"
		}
		return role, len(fields) >= 9 && strings.TrimSpace(fields[8]) != "", true
	}
	return "", false, false
}

// waitClusterMemberRunning waits until the victim's redis is up (PING) AND it is a member of
// the legacy-rooted mesh — i.e. it has been MET and (by strict precedence) is a slot-less
// replica in the Replicate window. Bounded generously; the window opens early in migration.
func waitClusterMemberRunning(crName, victim string) {
	Eventually(func(g Gomega) {
		pong, err := redisExec(testNamespace, victim, "PING")
		g.Expect(err).NotTo(HaveOccurred(), "victim redis not reachable yet")
		g.Expect(strings.TrimSpace(pong)).To(Equal("PONG"))
		_, _, present := migClusterNodeInfo(crName, victim)
		g.Expect(present).To(BeTrue(), "victim not yet a cluster member (not MET)")
	}, 8*time.Minute, 3*time.Second).Should(Succeed(),
		"new master %s never came up as a cluster member during migration", victim)
}

// waitVictimOwnsRange waits until the victim ({name}-shard-K-0) OWNS its shard's slot range —
// i.e. its coordinated failover has completed and it is now a master holding slots. This is
// the post-failover state whose restart-safety is the LR-025 regression guard.
func waitVictimOwnsRange(crName, victim string, shard int) {
	rng := redisclient.GenerateSlotRanges(clusterShards)[shard]
	want := redisclient.FormatSlotRange(rng.Start, rng.End)
	Eventually(func(g Gomega) {
		out, err := redisExec(testNamespace, migLegacySeedPod(crName), "CLUSTER", "NODES")
		g.Expect(err).NotTo(HaveOccurred())
		ip := podIP(victim)
		g.Expect(ip).NotTo(BeEmpty())
		found := false
		for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 9 {
				continue
			}
			addr := fields[1]
			if i := strings.IndexAny(addr, ":@"); i >= 0 {
				addr = addr[:i]
			}
			if addr != ip {
				continue
			}
			slots := strings.Join(fields[8:], ",")
			g.Expect(strings.Contains(flags(fields), "master")).To(BeTrue(),
				"victim seen but not a master yet")
			g.Expect(slots).To(ContainSubstring(want),
				"victim is master but does not yet own range %s (has %q)", want, slots)
			found = true
			break
		}
		g.Expect(found).To(BeTrue(), "victim %s not yet owning range %s", victim, want)
	}, 8*time.Minute, 3*time.Second).Should(Succeed())
}

// flags returns the CLUSTER NODES flags field (index 2), or "" if the line is too short.
func flags(fields []string) string {
	if len(fields) < 3 {
		return ""
	}
	return fields[2]
}

// migRestartCount returns the redis container's restart count for a pod (0 if unknown).
func migRestartCount(pod string) int {
	out, _ := utils.Run(exec.Command("kubectl", "get", "pod", pod, "-n", testNamespace,
		"-o", `jsonpath={.status.containerStatuses[?(@.name=="redis")].restartCount}`))
	var n int
	_, _ = fmt.Sscan(strings.TrimSpace(out), &n)
	return n
}

// crashContainerAssertRecovers kill-9's the victim's redis CONTAINER (PID 1, via the privileged
// hostPID helper — EmptyDir/nodes.conf survive, IP preserved) and asserts it recovers without a
// CrashLoopBackOff deadlock: the pod is NOT replaced (UID unchanged), the container restarts
// (restart count increments), and redis comes back reachable and rejoins the mesh. Under the
// superseded reshard mechanism the post-failover crash would NEVER recover here (STEP-3 parks a
// replica-less slot-owner → perpetual restart), so "redis reachable again within the window" is
// exactly the assertion that distinguishes the fix from the old deadlock.
func crashContainerAssertRecovers(crName, victim, label string) {
	By("crash " + label + ": kill-9 the redis container of " + victim)
	beforeUID := podUID(testNamespace, victim)
	beforeRestarts := migRestartCount(victim)
	Expect(beforeUID).NotTo(BeEmpty(), "could not read victim pod UID before crash")

	killPodProcess(testNamespace, victim)

	By("the pod must NOT be replaced (container restart, not a reschedule that wipes EmptyDir)")
	Consistently(func(g Gomega) {
		g.Expect(podUID(testNamespace, victim)).To(Equal(beforeUID),
			"victim pod UID changed — this was a pod replacement, not an in-container crash")
	}, 12*time.Second, 3*time.Second).Should(Succeed())

	By("the redis container must restart (restart count increments)")
	Eventually(func(g Gomega) {
		g.Expect(migRestartCount(victim)).To(BeNumerically(">", beforeRestarts),
			"redis container did not restart after kill-9")
	}, 90*time.Second, 3*time.Second).Should(Succeed())

	By("the victim must RECOVER — redis reachable again + rejoined the cluster (NO CrashLoopBackOff deadlock)")
	// The teeth: under the old replica-less deadlock this never becomes reachable (STEP-3 parks →
	// liveness kills → restart → park → CrashLoopBackOff). Generous window covers the STEP-3
	// yield/TAKEOVER path (~30s) plus reschedule of any helper artifacts.
	Eventually(func(g Gomega) {
		pong, err := redisExec(testNamespace, victim, "PING")
		g.Expect(err).NotTo(HaveOccurred(), "victim redis still not reachable — possible CrashLoopBackOff deadlock")
		g.Expect(strings.TrimSpace(pong)).To(Equal("PONG"))
		waiting, _ := utils.Run(exec.Command("kubectl", "get", "pod", victim, "-n", testNamespace,
			"-o", `jsonpath={.status.containerStatuses[?(@.name=="redis")].state.waiting.reason}`))
		g.Expect(strings.TrimSpace(waiting)).NotTo(Equal("CrashLoopBackOff"),
			"victim redis container is in CrashLoopBackOff — the restart-during-migration deadlock")
	}, 5*time.Minute, 5*time.Second).Should(Succeed(),
		"victim %s did not recover after crash %s", victim, label)
}

// stsExists reports whether a StatefulSet exists in the test namespace.
func stsExists(name string) bool {
	_, err := utils.Run(exec.Command("kubectl", "get", "statefulset", name,
		"-n", testNamespace, "-o", "name"))
	return err == nil
}

// writeDatasetSpanningShards writes perShard keys into EACH shard's slot range (via a hash
// tag routing into GenerateSlotRanges(shards)[K]) through a -c client on seedPod, and returns
// the key→value map for a later integrity check. Guarantees the dataset genuinely spans all
// shards (not just probabilistically). Reuses findHashTagInSlotRange/keySlot from the reshard
// tier (same package).
func writeDatasetSpanningShards(seedPod string, perShard int) map[string]string {
	data := make(map[string]string, clusterShards*perShard)
	for _, rng := range redisclient.GenerateSlotRanges(clusterShards) {
		tag := findHashTagInSlotRange(seedPod, rng.Start, rng.End)
		for j := 1; j <= perShard; j++ {
			key := fmt.Sprintf("%smig:%d", tag, j)
			val := fmt.Sprintf("v-%s-%d", tag, j)
			_, err := redisExec(testNamespace, seedPod, "-c", "SET", key, val)
			Expect(err).NotTo(HaveOccurred(), "failed to seed key %s", key)
			data[key] = val
		}
	}
	return data
}

// verifyDataset asserts every seeded key still resolves to its exact value, read via a -c
// client through readPod (redirection resolves whichever new master now owns the slot).
func verifyDataset(readPod string, data map[string]string) {
	Eventually(func(g Gomega) {
		for key, want := range data {
			out, err := redisExec(testNamespace, readPod, "-c", "GET", key)
			g.Expect(err).NotTo(HaveOccurred(), "GET %s failed", key)
			g.Expect(strings.TrimSpace(out)).To(Equal(want), "value mismatch for key %s", key)
		}
	}, 3*time.Minute, 10*time.Second).Should(Succeed(), "seeded dataset was not preserved across migration")
}

// assertSharedServiceCoexistence asserts the structural coexistence property (DESIGN §3 /
// §8): the shared headless Service {name}-cluster selects component=cluster (shard-agnostic),
// so it fronts BOTH legacy {name}-cluster-N pods AND new {name}-shard-K-M pods during the
// migration window. This is the deterministic half of the coexistence claim.
func assertSharedServiceCoexistence(crName string) {
	selector, err := utils.Run(exec.Command("kubectl", "get", "service",
		clusterStatefulSetName(crName), "-n", testNamespace,
		"-o", "jsonpath={.spec.selector.app\\.kubernetes\\.io/component}"))
	Expect(err).NotTo(HaveOccurred(), "shared headless Service %s not found", clusterStatefulSetName(crName))
	Expect(strings.TrimSpace(selector)).To(Equal("cluster"),
		"shared Service selector is not shard-agnostic — it would not front both legacy and new pods")
}

// coexistenceSample accumulates what a data-plane client observed while migration ran. It is
// pure instrumentation (ADR-013 WS2 correction): the migration's data-safety teeth live
// DOWNSTREAM (waitMigrationComplete + verifyDataset + the cluster_state:ok assertion), so this
// tolerates transient errors during the transfer window and only RECORDS them, per category.
type coexistenceSample struct {
	samples     int // total probe attempts
	okReads     int // clean reads returning the exact seeded value
	clusterDown int // -CLUSTERDOWN (should not occur under LR-025 replicate-then-failover; measured to confirm)
	redirect    int // -ASK / -MOVED / -TRYAGAIN that surfaced as an error to the client
	connErr     int // dial / i-o-timeout / connection-refused (pod churning)
	podGone     int // kubectl: the seed pod no longer exists (Decommission end-of-window)
	wrongValue  int // a clean read returned the WRONG value — genuine corruption, NOT a blip
}

func (s coexistenceSample) transientBlips() int { return s.clusterDown + s.redirect + s.connErr }
func (s coexistenceSample) blipObserved() bool  { return s.transientBlips() > 0 }

// coexistenceSampler runs a background data-plane client for the whole migration window.
type coexistenceSampler struct {
	stop   chan struct{}
	result chan coexistenceSample
}

// coexistenceProbeKeys picks one representative seeded key per shard for a light, spanning probe.
func coexistenceProbeKeys(crName string, data map[string]string) []string {
	var probeKeys []string
	for _, rng := range redisclient.GenerateSlotRanges(clusterShards) {
		tag := findHashTagInSlotRange(legacyPod(crName), rng.Start, rng.End)
		if _, ok := data[fmt.Sprintf("%smig:1", tag)]; ok {
			probeKeys = append(probeKeys, fmt.Sprintf("%smig:1", tag))
		}
	}
	return probeKeys
}

// startCoexistenceSampler launches a background -c client that reads a spanning set of seeded keys
// through a legacy node (up until Decommission) once per second, classifying every reply, until
// stopped. It captures the migration's transient behavior — the whole point of WS2's "does this
// path blip?" measurement — WITHOUT failing on a blip. The caller stops it via stopAndReport once
// the migration is Complete. Reads are best-effort and never call Gomega inside the goroutine.
func startCoexistenceSampler(crName string, data map[string]string) *coexistenceSampler {
	probeKeys := coexistenceProbeKeys(crName, data)
	cs := &coexistenceSampler{stop: make(chan struct{}), result: make(chan coexistenceSample, 1)}
	go func() {
		defer GinkgoRecover()
		var s coexistenceSample
		for {
			select {
			case <-cs.stop:
				cs.result <- s
				return
			default:
			}
			for _, key := range probeKeys {
				s.samples++
				out, err := redisExec(testNamespace, legacyPod(crName), "-c", "GET", key)
				trimmed := strings.TrimSpace(out)
				switch {
				// Classify on the reply text first: redis-cli may exit 0 OR non-zero on an
				// error reply depending on version, so keying off err alone is unreliable.
				case strings.Contains(out, "CLUSTERDOWN"):
					s.clusterDown++
				case strings.Contains(out, "MOVED"), strings.Contains(out, "ASK"),
					strings.Contains(out, "TRYAGAIN"):
					s.redirect++
				case strings.Contains(out, "NotFound"), strings.Contains(out, "not found"):
					// The seed pod was decommissioned — end of the migration window, not a blip.
					s.podGone++
				case err != nil:
					s.connErr++
				case trimmed == data[key]:
					s.okReads++
				default:
					// A clean read that returned the wrong value: genuine corruption.
					s.wrongValue++
				}
			}
			time.Sleep(1 * time.Second)
		}
	}()
	return cs
}

// stopAndReport stops the background sampler, LOGS the per-category tally (so the report can state,
// per path, "blip observed: yes/no (N)"), and keeps ONE tooth: a clean read must never have
// returned a wrong value (that is corruption, not a tolerated blip). All other transient errors
// are recorded, not failed — the authoritative data-intact / recovery gates run downstream.
func (cs *coexistenceSampler) stopAndReport() coexistenceSample {
	close(cs.stop)
	s := <-cs.result

	_, _ = fmt.Fprintf(GinkgoWriter,
		"[migration coexistence] blip observed: %v — transient=%d (CLUSTERDOWN=%d redirect=%d conn=%d) "+
			"| ok=%d podGone=%d wrongValue=%d over %d samples\n",
		s.blipObserved(), s.transientBlips(), s.clusterDown, s.redirect, s.connErr,
		s.okReads, s.podGone, s.wrongValue, s.samples)
	AddReportEntry(fmt.Sprintf("coexistence-blip observed=%v CLUSTERDOWN=%d redirect=%d conn=%d ok=%d samples=%d",
		s.blipObserved(), s.clusterDown, s.redirect, s.connErr, s.okReads, s.samples))

	Expect(s.wrongValue).To(Equal(0),
		"a data-plane read returned the WRONG value during migration (%d times) — corruption, not a transient blip", s.wrongValue)
	return s
}
