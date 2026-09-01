package cmd

import (
	"encoding/json"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	clifailover "github.com/littlered-operator/littlered-operator/internal/cli/failover"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// printJSON marshals v as indented JSON and writes it to stdout.
func printJSON(v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("json marshal: %w", err)
	}
	fmt.Println(string(b))
	return nil
}

// =============================================================================
// Parsers
// =============================================================================

// parseAlternatingKV parses redis-cli output where lines alternate key/value,
// as produced by commands like SENTINEL MASTER mymaster.
func parseAlternatingKV(output string) map[string]string {
	result := make(map[string]string)
	lines := strings.Split(strings.ReplaceAll(strings.TrimSpace(output), "\r", ""), "\n")
	for i := 0; i+1 < len(lines); i += 2 {
		key := strings.TrimSpace(lines[i])
		val := strings.TrimSpace(lines[i+1])
		if key != "" {
			result[key] = val
		}
	}
	return result
}

// parseInfoKV parses redis INFO / CLUSTER INFO output (key:value\r\n lines,
// section headers starting with # are ignored).
func parseInfoKV(output string) map[string]string {
	result := make(map[string]string)
	for line := range strings.SplitSeq(output, "\n") {
		line = strings.TrimRight(strings.TrimSpace(line), "\r")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			result[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}
	return result
}

// =============================================================================
// status JSON types
// =============================================================================

type statusJSON struct {
	Name               string                               `json:"name"`
	Namespace          string                               `json:"namespace"`
	Mode               string                               `json:"mode"`
	Phase              littleredv1alpha1.LittleRedPhase     `json:"phase"`
	BootstrapRequired  bool                                 `json:"bootstrapRequired"`
	ObservedGeneration int64                                `json:"observedGeneration"`
	Redis              littleredv1alpha1.RedisStatus        `json:"redis"`
	Master             *littleredv1alpha1.MasterStatus      `json:"master,omitempty"`
	Sentinels          *littleredv1alpha1.SentinelStatus    `json:"sentinels,omitempty"`
	Replicas           *littleredv1alpha1.ReplicaStatus     `json:"replicas,omitempty"`
	Cluster            *littleredv1alpha1.ClusterStatusInfo `json:"cluster,omitempty"`
	Failover           *littleredv1alpha1.FailoverStatus    `json:"failover,omitempty"`
	Conditions         []metav1.Condition                   `json:"conditions,omitempty"`

	// Operation and AcknowledgedOperations are ADR-020's two status surfaces, mirrored
	// under the CRD's own key names so this output and `kubectl get -o json` are the
	// same shape. Both are omitempty, so an instance with no operation renders exactly
	// what it rendered before.
	//
	// AcknowledgedOperations is a completion RECORD of keyed digests. It is emitted
	// because --json is a faithful projection of the CR and a script may legitimately
	// ask "did the rename finish, and when" — but nothing in lrctl derives a verdict
	// from it (ADR-020 D3), and it is deliberately absent from the human output, where
	// an HMAC identifies nothing a reader can look up and invites being misread as "the
	// previous value" that ADR-018 refuses to remember.
	Operation              *littleredv1alpha1.OperationStatus `json:"operation,omitempty"`
	AcknowledgedOperations []littleredv1alpha1.OperationAck   `json:"acknowledgedOperations,omitempty"`
}

func lrToStatusJSON(lr *littleredv1alpha1.LittleRed) statusJSON {
	return statusJSON{
		Name:               lr.Name,
		Namespace:          lr.Namespace,
		Mode:               lr.Spec.Mode,
		Phase:              lr.Status.Phase,
		BootstrapRequired:  lr.Status.BootstrapRequired,
		ObservedGeneration: lr.Status.ObservedGeneration,
		Redis:              lr.Status.Redis,
		Master:             lr.Status.Master,
		Sentinels:          lr.Status.Sentinels,
		Replicas:           lr.Status.Replicas,
		Cluster:            lr.Status.Cluster,
		Failover:           lr.Status.Failover,
		Conditions:         lr.Status.Conditions,

		Operation:              lr.Status.Operation,
		AcknowledgedOperations: lr.Status.AcknowledgedOperations,
	}
}

// =============================================================================
// inspect JSON types
// =============================================================================

// sentinelPodJSON holds per-sentinel-pod data for JSON output.
// raw is unexported and used only for text rendering.
type sentinelPodJSON struct {
	Pod        string            `json:"pod"`
	IP         string            `json:"ip"`
	MasterInfo map[string]string `json:"masterInfo,omitempty"`
	// MonitoredMasters is EVERY master name this Sentinel carries. MasterInfo above
	// answers a single-name question and therefore cannot reveal a leftover name;
	// this is the surface that can (LR-048).
	MonitoredMasters []monitoredMasterJSON `json:"monitoredMasters,omitempty"`
	Error            string                `json:"error,omitempty"`
	raw              string
}

// clusterNodeInspectJSON is a JSON-tagged representation of a cluster node,
// mapped from redisclient.ClusterNodeInfo which has no JSON tags.
type clusterNodeInspectJSON struct {
	NodeID    string   `json:"nodeID"`
	Addr      string   `json:"addr"`
	Hostname  string   `json:"hostname,omitempty"`
	Flags     []string `json:"flags"`
	MasterID  string   `json:"masterID,omitempty"`
	LinkState string   `json:"linkState"`
	Slots     []string `json:"slots,omitempty"`
}

// redisPodJSON holds per-redis-pod data for JSON output.
// raw is unexported and used only for text rendering.
type redisPodJSON struct {
	Pod          string                   `json:"pod"`
	IP           string                   `json:"ip"`
	Replication  map[string]string        `json:"replication,omitempty"`
	ClusterNodes []clusterNodeInspectJSON `json:"clusterNodes,omitempty"`
	ClusterInfo  map[string]string        `json:"clusterInfo,omitempty"`
	// Assignment holds the operator-stamped assignment annotations plus the
	// role label (failover mode only): assigned-role, assigned-master-ip,
	// assignment-epoch, role-label.
	Assignment map[string]string `json:"assignment,omitempty"`
	Error      string            `json:"error,omitempty"`
	raw        string
}

type inspectJSON struct {
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
	Mode      string            `json:"mode"`
	Sentinels []sentinelPodJSON `json:"sentinels,omitempty"`
	Redis     []redisPodJSON    `json:"redis"`
}

func parseClusterNodesJSON(output string) []clusterNodeInspectJSON {
	raw := redisclient.ParseClusterNodes(output)
	result := make([]clusterNodeInspectJSON, 0, len(raw))
	for _, n := range raw {
		masterID := n.MasterID
		if masterID == "-" {
			masterID = ""
		}
		result = append(result, clusterNodeInspectJSON{
			NodeID:    n.NodeID,
			Addr:      n.Addr,
			Hostname:  n.Hostname,
			Flags:     n.Flags,
			MasterID:  masterID,
			LinkState: n.LinkState,
			Slots:     n.Slots,
		})
	}
	return result
}

// =============================================================================
// verify JSON types
// =============================================================================

type sentinelNodeVerifyJSON struct {
	PodName    string `json:"podName"`
	IP         string `json:"ip"`
	Monitoring bool   `json:"monitoring"`
	MasterIP   string `json:"masterIP,omitempty"`
	// FailoverState is Sentinel's own `failover-state`, absent unless a failover is
	// running. It was `failoverStatus`, read from a key neither project emits, so
	// it was ALWAYS empty and (being omitempty) never appeared in any output —
	// which is why renaming the JSON key breaks no consumer (LR-052).
	FailoverState string `json:"failoverState,omitempty"`
	Reachable     bool   `json:"reachable"`
	// MonitoredMasters is EVERY master name this Sentinel carries, not only the
	// one we asked about. Empty means the list could not be read, never that the
	// Sentinel monitors nothing (LR-041).
	MonitoredMasters []monitoredMasterJSON `json:"monitoredMasters,omitempty"`
}

// monitoredMasterJSON is one monitored master name, with the class it falls into
// against the CR's configured name: desired | stale | foreign.
type monitoredMasterJSON struct {
	Name  string `json:"name"`
	IP    string `json:"ip,omitempty"`
	Flags string `json:"flags,omitempty"`
	Class string `json:"class"`
}

// masterNameScopeJSON answers "does every Sentinel monitor ONLY the configured
// name" — the question a single-name probe structurally cannot ask (LR-048).
type masterNameScopeJSON struct {
	Desired    string   `json:"desired"`
	Converged  bool     `json:"converged"`
	Stale      []string `json:"stale,omitempty"`
	Foreign    []string `json:"foreign,omitempty"`
	Unreported []string `json:"unreported,omitempty"`
}

type redisNodeVerifyJSON struct {
	PodName    string `json:"podName"`
	IP         string `json:"ip"`
	Role       string `json:"role,omitempty"`
	MasterHost string `json:"masterHost,omitempty"`
	LinkStatus string `json:"linkStatus,omitempty"`
	Offset     int64  `json:"offset,omitempty"`
	Reachable  bool   `json:"reachable"`
}

type sentinelVerifyJSON struct {
	Name              string                   `json:"name"`
	Namespace         string                   `json:"namespace"`
	Mode              string                   `json:"mode"`
	RealMasterIP      string                   `json:"realMasterIP,omitempty"`
	RealMasterPodName string                   `json:"realMasterPodName,omitempty"`
	FailoverActive    bool                     `json:"failoverActive"`
	Sentinels         []sentinelNodeVerifyJSON `json:"sentinels"`
	Redis             []redisNodeVerifyJSON    `json:"redis"`
	HealActions       []string                 `json:"healActions"`
	Healthy           bool                     `json:"healthy"`

	// MasterName is the Sentinel master name in use — the instance's gossip identity.
	MasterName string `json:"masterName"`
	// CrossInstance is evidence that another Sentinel deployment shares that name.
	// Its absence means "nothing visible from this vantage", never "isolated".
	CrossInstance *crossInstanceJSON `json:"crossInstance,omitempty"`
	// MasterNameScope is what every reachable Sentinel actually monitors. It is
	// absent for an --unmanaged target, where no CR names the wanted master name
	// and classifying against a guess would accuse a correctly-named instance.
	MasterNameScope *masterNameScopeJSON `json:"masterNameScope,omitempty"`

	// Operation is the declared heavy operation in flight (ADR-020), absent when
	// there is none and for --unmanaged targets, which have no CR to read it from.
	Operation *operationJSON `json:"operation,omitempty"`
}

type sentinelCountJSON struct {
	PodName  string `json:"podName"`
	Reported int    `json:"reported"`
	Expected int    `json:"expected"`
}

type crossInstanceJSON struct {
	ForeignMasterIPs  []string            `json:"foreignMasterIPs,omitempty"`
	ForeignReplicaIPs []string            `json:"foreignReplicaIPs,omitempty"`
	PeerSurplus       []sentinelCountJSON `json:"peerSurplus,omitempty"`
	ReplicaSurplus    []sentinelCountJSON `json:"replicaSurplus,omitempty"`
}

type clusterNodeVerifyJSON struct {
	PodName      string   `json:"podName"`
	PodIP        string   `json:"podIP"`
	NodeID       string   `json:"nodeID"`
	Role         string   `json:"role,omitempty"`
	MasterNodeID string   `json:"masterNodeID,omitempty"`
	Slots        []string `json:"slots,omitempty"`
	LinkStatus   string   `json:"linkStatus,omitempty"`
	Reachable    bool     `json:"reachable"`
}

type clusterVerifyJSON struct {
	Name         string                  `json:"name"`
	Namespace    string                  `json:"namespace"`
	Mode         string                  `json:"mode"`
	ClusterState string                  `json:"clusterState"`
	TotalSlots   int                     `json:"totalSlots"`
	GhostNodes   []string                `json:"ghostNodes"`
	Partitions   [][]string              `json:"partitions,omitempty"`
	Nodes        []clusterNodeVerifyJSON `json:"nodes"`
	Healthy      bool                    `json:"healthy"`

	// Operation — see sentinelVerifyJSON.Operation.
	Operation *operationJSON `json:"operation,omitempty"`
}

// buildSentinelVerifyJSON renders the sentinel verify result. wantedName is the
// master name the CR asks for, or "" when there is no CR to read it from
// (--unmanaged) — in which case the monitored-name scope is reported as unknown
// rather than judged against sentinelMasterName's fallback guess.
func buildSentinelVerifyJSON(
	name, namespace string, redisMap map[string]string,
	state *redisclient.ReplicationState, sentinelMasterName, wantedName string,
	expectedSentinels, expectedReplicas int,
) sentinelVerifyJSON {
	actions := state.GetHealActions(sentinelMasterName)
	if actions == nil {
		actions = []string{}
	}

	result := sentinelVerifyJSON{
		Name:           name,
		Namespace:      namespace,
		Mode:           modeSentinel,
		RealMasterIP:   state.RealMasterIP,
		FailoverActive: state.FailoverActive,
		HealActions:    actions,
		MasterName:     sentinelMasterName,
		Sentinels:      []sentinelNodeVerifyJSON{},
		Redis:          []redisNodeVerifyJSON{},
	}

	if ev := state.DetectCrossInstance(expectedSentinels, expectedReplicas); ev.Any() {
		ci := &crossInstanceJSON{
			ForeignMasterIPs:  ev.ForeignMasterIPs,
			ForeignReplicaIPs: ev.ForeignReplicaIPs,
		}
		for _, c := range ev.PeerSurplus {
			ci.PeerSurplus = append(ci.PeerSurplus,
				sentinelCountJSON{PodName: c.PodName, Reported: c.Reported, Expected: c.Expected})
		}
		for _, c := range ev.ReplicaSurplus {
			ci.ReplicaSurplus = append(ci.ReplicaSurplus,
				sentinelCountJSON{PodName: c.PodName, Reported: c.Reported, Expected: c.Expected})
		}
		result.CrossInstance = ci
	}

	if state.RealMasterIP != "" {
		result.RealMasterPodName = redisMap[state.RealMasterIP]
	}

	scope := state.SurveyMonitoredNames(wantedName)
	if wantedName != "" {
		result.MasterNameScope = &masterNameScopeJSON{
			Desired:    wantedName,
			Converged:  scope.Converged(),
			Stale:      scope.Stale,
			Foreign:    scope.Foreign,
			Unreported: scope.Unreported,
		}
	}
	classOf := make(map[string]string, len(scope.Findings))
	for _, f := range scope.Findings {
		classOf[f.SentinelPod+"\x00"+f.Name] = f.Class
	}

	for _, sn := range state.SentinelNodes {
		entry := sentinelNodeVerifyJSON{
			PodName:       sn.PodName,
			IP:            sn.IP,
			Monitoring:    sn.Monitoring,
			MasterIP:      sn.MasterIP,
			FailoverState: sn.MasterFailoverState,
			Reachable:     sn.Reachable,
		}
		for _, m := range sn.MonitoredMasters {
			entry.MonitoredMasters = append(entry.MonitoredMasters, monitoredMasterJSON{
				Name: m.Name, IP: m.IP, Flags: m.Flags,
				Class: classOf[sn.PodName+"\x00"+m.Name],
			})
		}
		result.Sentinels = append(result.Sentinels, entry)
	}

	for _, rn := range state.RedisNodes {
		result.Redis = append(result.Redis, redisNodeVerifyJSON{
			PodName:    rn.PodName,
			IP:         rn.IP,
			Role:       rn.Role,
			MasterHost: rn.MasterHost,
			LinkStatus: rn.LinkStatus,
			Offset:     rn.Offset,
			Reachable:  rn.Reachable,
		})
	}

	// A Sentinel monitoring a name other than the CR's is a defect whatever else is
	// true, so it is part of the health verdict and not only of the text output —
	// the JSON consumer must not be able to read "healthy" off a two-name instance.
	result.Healthy = state.RealMasterIP != "" && len(result.HealActions) == 0 &&
		!state.FailoverActive && scope.Converged()
	return result
}

// failoverPodVerifyJSON merges the K8s-side view (assignment annotations,
// role label, readiness) with the gathered replication state for one data pod.
type failoverPodVerifyJSON struct {
	PodName          string `json:"podName"`
	IP               string `json:"ip"`
	Phase            string `json:"phase,omitempty"`
	Ready            bool   `json:"ready"`
	Restarted        bool   `json:"restarted"`
	Terminating      bool   `json:"terminating,omitempty"`
	RoleLabel        string `json:"roleLabel,omitempty"`
	HasAssignment    bool   `json:"hasAssignment"`
	AssignedRole     string `json:"assignedRole,omitempty"`
	AssignedMasterIP string `json:"assignedMasterIP,omitempty"`
	AssignmentEpoch  int64  `json:"assignmentEpoch,omitempty"`
	Parked           bool   `json:"parked,omitempty"`
	Role             string `json:"role,omitempty"`
	MasterHost       string `json:"masterHost,omitempty"`
	LinkStatus       string `json:"linkStatus,omitempty"`
	Offset           int64  `json:"offset,omitempty"`
	Keys             int64  `json:"keys,omitempty"`
	Replid           string `json:"replid,omitempty"`
	Replid2          string `json:"replid2,omitempty"`
	Reachable        bool   `json:"reachable"`
}

type failoverVerifyJSON struct {
	Name                  string                  `json:"name"`
	Namespace             string                  `json:"namespace"`
	Mode                  string                  `json:"mode"`
	IntendedMasterPod     string                  `json:"intendedMasterPod,omitempty"`
	IntendedMasterIP      string                  `json:"intendedMasterIP,omitempty"`
	MasterAssignmentEpoch int64                   `json:"masterAssignmentEpoch,omitempty"`
	MaxAssignmentEpoch    int64                   `json:"maxAssignmentEpoch,omitempty"`
	AuthorityMasterPod    string                  `json:"authorityMasterPod,omitempty"`
	AuthorityMasterIP     string                  `json:"authorityMasterIP,omitempty"`
	Pods                  []failoverPodVerifyJSON `json:"pods"`
	Findings              []clifailover.Finding   `json:"findings"`
	Healthy               bool                    `json:"healthy"`
	Degraded              bool                    `json:"degraded"`

	// Operation — see sentinelVerifyJSON.Operation.
	Operation *operationJSON `json:"operation,omitempty"`
}

func buildFailoverVerifyJSON(
	name, namespace string, views []clifailover.PodView,
	state *redisclient.ReplicationState, analysis clifailover.Analysis,
) failoverVerifyJSON {
	findings := analysis.Findings
	if findings == nil {
		findings = []clifailover.Finding{}
	}
	result := failoverVerifyJSON{
		Name:                  name,
		Namespace:             namespace,
		Mode:                  modeFailover,
		IntendedMasterPod:     analysis.Intent.MasterName,
		IntendedMasterIP:      analysis.Intent.MasterIP,
		MasterAssignmentEpoch: analysis.Intent.MasterEpoch,
		MaxAssignmentEpoch:    analysis.Intent.MaxEpoch,
		AuthorityMasterPod:    analysis.AuthorityPod,
		AuthorityMasterIP:     analysis.AuthorityIP,
		Pods:                  []failoverPodVerifyJSON{},
		Findings:              findings,
		Healthy:               analysis.Healthy(),
		Degraded:              analysis.Degraded(),
	}

	parked := make(map[string]bool, len(analysis.Parked))
	for _, p := range analysis.Parked {
		parked[p] = true
	}

	for _, v := range views {
		entry := failoverPodVerifyJSON{
			PodName:          v.Name,
			IP:               v.IP,
			Phase:            v.Phase,
			Ready:            v.Ready,
			Restarted:        v.Restarted,
			Terminating:      v.Terminating,
			RoleLabel:        v.RoleLabel,
			HasAssignment:    v.HasAssignment,
			AssignedRole:     v.AssignedRole,
			AssignedMasterIP: v.AssignedMasterIP,
			AssignmentEpoch:  v.Epoch,
			Parked:           parked[v.Name],
		}
		if rn := state.RedisNodes[v.IP]; rn != nil {
			entry.Role = rn.Role
			entry.MasterHost = rn.MasterHost
			entry.LinkStatus = rn.LinkStatus
			entry.Offset = rn.Offset
			entry.Keys = rn.Keys
			entry.Replid = rn.Replid
			entry.Replid2 = rn.Replid2
			entry.Reachable = rn.Reachable
		}
		result.Pods = append(result.Pods, entry)
	}
	return result
}

func buildClusterVerifyJSON(name, namespace string, gt *redisclient.ClusterGroundTruth) clusterVerifyJSON {
	ghostNodes := gt.GhostNodes
	if ghostNodes == nil {
		ghostNodes = []string{}
	}

	result := clusterVerifyJSON{
		Name:         name,
		Namespace:    namespace,
		Mode:         modeCluster,
		ClusterState: gt.ClusterState,
		TotalSlots:   gt.TotalSlots,
		GhostNodes:   ghostNodes,
		Partitions:   gt.Partitions,
		Nodes:        []clusterNodeVerifyJSON{},
	}

	for _, n := range gt.Nodes {
		masterNodeID := n.MasterNodeID
		if masterNodeID == "-" {
			masterNodeID = ""
		}
		result.Nodes = append(result.Nodes, clusterNodeVerifyJSON{
			PodName:      n.PodName,
			PodIP:        n.PodIP,
			NodeID:       n.NodeID,
			Role:         n.Role,
			MasterNodeID: masterNodeID,
			Slots:        n.Slots,
			LinkStatus:   n.LinkStatus,
			Reachable:    n.Reachable,
		})
	}

	expectedNodes := int32(len(gt.Nodes))
	expectedShards := int32(gt.CountMasters())
	result.Healthy = gt.IsHealthy(expectedNodes, expectedShards)
	return result
}

// verifyJSONResult is the little that the three mode-specific verify results have in
// common: they all carry a `healthy` flag a script trusts, and a declared operation is
// mode-neutral because status.operation is.
//
// It exists so the operation is attached in ONE place, beside the exit-code decision it
// must agree with. A JSON consumer must not be able to read `healthy: true` off an
// instance whose `verify` exited non-zero — the same property buildSentinelVerifyJSON
// already guarantees for a two-name instance.
type verifyJSONResult interface {
	applyOperation(op *operationJSON)
}

func applyOperationTo(healthy *bool, dst **operationJSON, op *operationJSON) {
	*dst = op
	if op != nil && op.NeedsAction {
		*healthy = false
	}
}

func (j *sentinelVerifyJSON) applyOperation(op *operationJSON) {
	applyOperationTo(&j.Healthy, &j.Operation, op)
}

func (j *clusterVerifyJSON) applyOperation(op *operationJSON) {
	applyOperationTo(&j.Healthy, &j.Operation, op)
}

func (j *failoverVerifyJSON) applyOperation(op *operationJSON) {
	applyOperationTo(&j.Healthy, &j.Operation, op)
}
