package k8s

import (
	"bytes"
	"context"
	"fmt"

	littleredv1alpha1 "github.com/chuck-chuck-chuck-net/littlered/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NewClient returns a controller-runtime client and a core kubernetes client,
// along with the default namespace from kubeconfig, using the current context.
func NewClient(kubeconfigPath string) (client.Client, *kubernetes.Clientset, *rest.Config, string, error) {
	return NewClientForContext(kubeconfigPath, "")
}

// NewClientForContext is NewClient with an explicit kube-context override
// (empty ⇒ the kubeconfig's current context). Multi-site lrctl uses it to point
// at the hub context to read the CR and at each site's own context to gather —
// the operator's site kubeconfig Secrets embed the replication-net endpoint that
// is reachable only from hub pods, so a workstation must use its own contexts
// (docs/MULTI_SITE_DESIGN.md §8).
func NewClientForContext(kubeconfigPath, contextName string) (client.Client, *kubernetes.Clientset, *rest.Config, string, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfigPath != "" {
		loadingRules.ExplicitPath = kubeconfigPath
	}

	configOverrides := &clientcmd.ConfigOverrides{}
	if contextName != "" {
		configOverrides.CurrentContext = contextName
	}
	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)

	config, err := kubeConfig.ClientConfig()
	if err != nil {
		return nil, nil, nil, "", fmt.Errorf("could not load kubeconfig: %w", err)
	}

	namespace, _, err := kubeConfig.Namespace()
	if err != nil {
		namespace = "default"
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(littleredv1alpha1.AddToScheme(scheme))

	c, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return nil, nil, nil, "", err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, nil, "", err
	}

	return c, clientset, config, namespace, nil
}

// AvailableContexts returns the names of every context defined in the resolved
// kubeconfig. Used to resolve multi-site site names to workstation-routable
// contexts (and to error clearly when a site's context is missing).
func AvailableContexts(kubeconfigPath string) ([]string, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfigPath != "" {
		loadingRules.ExplicitPath = kubeconfigPath
	}
	raw, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules, &clientcmd.ConfigOverrides{}).RawConfig()
	if err != nil {
		return nil, fmt.Errorf("could not load kubeconfig: %w", err)
	}
	names := make([]string, 0, len(raw.Contexts))
	for name := range raw.Contexts {
		names = append(names, name)
	}
	return names, nil
}

// Exec executes a command in a pod and returns stdout and stderr.
func Exec(ctx context.Context, clientset *kubernetes.Clientset, config *rest.Config, namespace, podName, containerName string, command []string) (string, string, error) {
	req := clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec")

	option := &corev1.PodExecOptions{
		Command:   command,
		Container: containerName,
		Stdin:     false,
		Stdout:    true,
		Stderr:    true,
		TTY:       false,
	}

	req.VersionedParams(
		option,
		clientgoscheme.ParameterCodec,
	)

	exec, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		return "", "", fmt.Errorf("failed to create executor: %w", err)
	}

	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	return stdout.String(), stderr.String(), err
}
