# LittleRed Helm Chart

Helm chart for deploying the LittleRed operator to Kubernetes.

## Prerequisites

- Kubernetes 1.28+
- Helm 3.13+

## Installation

```bash
helm install littlered oci://ghcr.io/chuck-chuck-chuck-net/charts/littlered \
  --version <version> \
  -n littlered-system \
  --create-namespace
```

Find available versions on the [releases page](https://github.com/chuck-chuck-chuck-net/littlered/releases).

## Uninstallation

```bash
helm uninstall littlered -n littlered-system
```

Note: CRDs are not deleted automatically. To remove them:

```bash
kubectl delete crd littlereds.chuck-chuck-chuck.net
```

## Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `image.repository` | Operator image repository | `ghcr.io/chuck-chuck-chuck-net/littlered` |
| `image.tag` | Operator image tag | `0.1.0` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `replicas` | Number of operator replicas | `1` |
| `resources.limits.cpu` | CPU limit | `500m` |
| `resources.limits.memory` | Memory limit | `128Mi` |
| `resources.requests.cpu` | CPU request | `10m` |
| `resources.requests.memory` | Memory request | `64Mi` |
| `serviceAccount.create` | Create ServiceAccount | `true` |
| `serviceAccount.name` | ServiceAccount name (auto-generated if empty) | `""` |
| `nodeSelector` | Node selector for operator pods | `{}` |
| `tolerations` | Tolerations for operator pods | `[]` |
| `affinity` | Affinity rules for operator pods | `{}` |
| `topologySpreadConstraints` | Topology spread constraints for operator pods (only meaningful with `replicas > 1`) | `[]` |
| `scope.watchNamespaces` | Allow-list: watch only these namespaces (renders namespaced RBAC, sets `WATCH_NAMESPACE`). Mutually exclusive with `scope.ignoreNamespaces`. | `[]` |
| `scope.ignoreNamespaces` | Deny-list: watch all namespaces except these (keeps cluster-wide RBAC, sets `IGNORE_NAMESPACE`). | `[]` |
| `metrics.enabled` | Expose Prometheus metrics endpoint | `false` |
| `metrics.port` | Metrics port | `8080` |
| `metrics.serviceMonitor.enabled` | Create a Prometheus Operator ServiceMonitor | `false` |
| `networkPolicy.enabled` | Restrict metrics port access to labeled namespaces | `false` |

## Examples

### Custom image (e.g. private registry)

```bash
helm install littlered oci://ghcr.io/chuck-chuck-chuck-net/charts/littlered \
  --version <version> \
  -n littlered-system \
  --create-namespace \
  --set image.repository=myregistry/littlered \
  --set image.tag=v0.1.0
```

### With Prometheus monitoring

```bash
helm install littlered oci://ghcr.io/chuck-chuck-chuck-net/charts/littlered \
  --version <version> \
  -n littlered-system \
  --create-namespace \
  --set metrics.enabled=true \
  --set metrics.serviceMonitor.enabled=true
```

### Using a values file

```bash
helm install littlered oci://ghcr.io/chuck-chuck-chuck-net/charts/littlered \
  --version <version> \
  -n littlered-system \
  --create-namespace \
  -f my-values.yaml
```

## After Installation

Once the operator is running, create LittleRed resources:

```yaml
apiVersion: chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: store
spec:
  mode: standalone  # standalone | sentinel | cluster
```

See the [main README](../../README.md) for full LittleRed CR documentation.

## Upgrading

```bash
helm upgrade littlered oci://ghcr.io/chuck-chuck-chuck-net/charts/littlered \
  --version <new-version> \
  -n littlered-system
```

## RBAC

By default (cluster-scoped, or deny-list via `scope.ignoreNamespaces`) the chart creates:

- **ClusterRole**: Permissions to manage LittleRed CRs, Pods, Services, ConfigMaps, StatefulSets, Secrets, and ServiceMonitors
- **ClusterRoleBinding**: Binds the ClusterRole to the operator ServiceAccount
- **Role** (namespaced, operator namespace): Leader election permissions
- **RoleBinding** (namespaced, operator namespace): Binds the leader election Role

### Namespace scoping (least-privilege)

When `scope.watchNamespaces` is set (allow-list), the operator is scoped to those
namespaces and the cluster-wide grant is dropped: instead of the ClusterRole /
ClusterRoleBinding, the chart renders the **same reconcile permissions as a `Role` +
`RoleBinding` in each watched namespace**, bound to the operator ServiceAccount (which
stays in the operator's own namespace). The leader-election Role/RoleBinding remain in
the operator's namespace in all modes. The CRD is always installed cluster-wide (from
`crds/`). `scope.watchNamespaces` and `scope.ignoreNamespaces` are mutually exclusive —
setting both fails the render.
