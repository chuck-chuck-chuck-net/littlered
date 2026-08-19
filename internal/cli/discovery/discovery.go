package discovery

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	"github.com/littlered-operator/littlered-operator/internal/cli/types"
)

const (
	containerRedis    = "redis"
	containerSentinel = "sentinel"
)

// GetContext builds a ClusterContext either from a CR or via heuristics
func GetContext(ctx context.Context, k8sClient client.Client, namespace, name, mode string, unmanaged bool) (*types.ClusterContext, error) {
	if !unmanaged {
		return getFromCR(ctx, k8sClient, namespace, name)
	}
	return discoverUnmanaged(ctx, k8sClient, namespace, name, mode)
}

func getFromCR(ctx context.Context, k8sClient client.Client, namespace, name string) (*types.ClusterContext, error) {
	lr := &littleredv1alpha1.LittleRed{}
	err := k8sClient.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, lr)
	if err != nil {
		return nil, fmt.Errorf("failed to get LittleRed CR: %w", err)
	}

	podList := &corev1.PodList{}
	err = k8sClient.List(ctx, podList, client.InNamespace(namespace), client.MatchingLabels{
		"app.kubernetes.io/instance": name,
	})
	if err != nil {
		return nil, err
	}

	cCtx := &types.ClusterContext{
		Name:               name,
		Namespace:          namespace,
		Mode:               lr.Spec.Mode,
		SentinelMasterName: lr.SentinelMasterName(),
		RedisContainer:     containerRedis,
		SentinelContainer:  containerSentinel,
	}

	for _, pod := range podList.Items {
		comp := pod.Labels["app.kubernetes.io/component"]
		if comp == containerRedis || comp == "cluster" {
			cCtx.RedisPods = append(cCtx.RedisPods, pod)
		}
		if comp == containerSentinel {
			cCtx.SentinelPods = append(cCtx.SentinelPods, pod)
		}
	}

	return cCtx, nil
}

func discoverUnmanaged(ctx context.Context, k8sClient client.Client, namespace, name, mode string) (*types.ClusterContext, error) {
	cCtx := &types.ClusterContext{
		Name:      name,
		Namespace: namespace,
		Mode:      mode,
	}

	podList := &corev1.PodList{}
	err := k8sClient.List(ctx, podList, client.InNamespace(namespace))
	if err != nil {
		return nil, err
	}

	// Define heuristics as regex patterns
	// 1. CloudPirates: <name>-redis-<digit>
	cpRegex := regexp.MustCompile(fmt.Sprintf("^%s-redis-[0-9]+$", regexp.QuoteMeta(name)))
	// 2. Bitnami: <name>-redis-node-<digit>, <name>-redis-cluster-<digit>, or <name>-master-<digit> (standalone)
	bitnamiRegex := regexp.MustCompile(fmt.Sprintf("^%s-(redis-(node|cluster)|master)-[0-9]+$", regexp.QuoteMeta(name)))
	// 3. Generic Cluster: <name>-cluster-[0-9]+ (pre-0.3.0 LittleRed and other operators)
	clusterRegex := regexp.MustCompile(fmt.Sprintf("^%s-cluster-[0-9]+$", regexp.QuoteMeta(name)))
	// 3b. LittleRed per-shard Cluster (0.3.0+): <name>-shard-<shard>-<ordinal>
	shardRegex := regexp.MustCompile(fmt.Sprintf("^%s-shard-[0-9]+-[0-9]+$", regexp.QuoteMeta(name)))
	// 4. Spotahome Redis: rfr-<name>-<digit>
	shRedisRegex := regexp.MustCompile(fmt.Sprintf("^rfr-%s-[0-9]+$", regexp.QuoteMeta(name)))
	// 5. Spotahome Sentinel: rfs-<name>-...
	shSentinelRegex := regexp.MustCompile(fmt.Sprintf("^rfs-%s-[a-z0-9]+(-[a-z0-9]+)?$", regexp.QuoteMeta(name)))

	foundRedis := false
	foundSentinel := false

	for _, pod := range podList.Items {
		isRedis := cpRegex.MatchString(pod.Name) || bitnamiRegex.MatchString(pod.Name) ||
			clusterRegex.MatchString(pod.Name) || shardRegex.MatchString(pod.Name) ||
			shRedisRegex.MatchString(pod.Name)

		isSentinel := shSentinelRegex.MatchString(pod.Name)

		if isRedis {
			cCtx.RedisPods = append(cCtx.RedisPods, pod)
			if !foundRedis {
				cCtx.RedisContainer = detectContainer(pod, []string{containerRedis, "valkey", "cluster"}, containerRedis)
				foundRedis = true
			}

			// In sidecar mode (CP/Bitnami), the sentinel is in the same pod
			if !isSentinel {
				for _, container := range pod.Spec.Containers {
					if strings.Contains(strings.ToLower(container.Name), containerSentinel) {
						cCtx.SentinelPods = append(cCtx.SentinelPods, pod)
						if !foundSentinel {
							cCtx.SentinelContainer = container.Name
							foundSentinel = true
						}
						break
					}
				}
			}
		}

		if isSentinel {
			cCtx.SentinelPods = append(cCtx.SentinelPods, pod)
			if !foundSentinel {
				cCtx.SentinelContainer = detectContainer(pod, []string{containerSentinel}, containerSentinel)
				foundSentinel = true
			}
		}
	}

	// Final defaults if nothing detected
	if cCtx.RedisContainer == "" {
		cCtx.RedisContainer = containerRedis
	}
	if cCtx.SentinelContainer == "" {
		cCtx.SentinelContainer = containerSentinel
	}

	if len(cCtx.RedisPods) == 0 && len(cCtx.SentinelPods) == 0 {
		return nil, fmt.Errorf("could not find any pods matching CloudPirates, Bitnami, Cluster, or Spotahome patterns for %q in namespace %q", name, namespace)
	}

	return cCtx, nil
}

// detectContainer tries to find a container matching keywords, otherwise returns the first container or fallback
func detectContainer(pod corev1.Pod, keywords []string, fallback string) string {
	if len(pod.Spec.Containers) == 0 {
		return fallback
	}
	if len(pod.Spec.Containers) == 1 {
		return pod.Spec.Containers[0].Name
	}

	for _, container := range pod.Spec.Containers {
		for _, kw := range keywords {
			if strings.Contains(strings.ToLower(container.Name), kw) {
				return container.Name
			}
		}
	}

	return pod.Spec.Containers[0].Name
}
