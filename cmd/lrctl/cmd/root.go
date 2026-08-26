package cmd

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	"github.com/littlered-operator/littlered-operator/internal/cli/k8s"
	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	modeStandalone    = "standalone"
	modeCluster       = "cluster"
	modeSentinel      = "sentinel"
	modeFailover      = "failover"
	roleMaster        = "master"
	roleSlave         = "slave" // Redis wire term, as reported by INFO / SENTINEL replies
	redisCliBin       = "redis-cli"
	infoSubcommand    = "info"
	clusterSubcommand = "cluster"
	statusUnreachable = "unreachable"
	valueNone         = "<none>"
)

var (
	namespace     string
	kubeconfig    string
	unmanaged     bool
	kind          string
	allNamespaces bool
	jsonOutput    bool
)

var rootCmd = &cobra.Command{
	Use:   "lrctl",
	Short: "lrctl is a CLI tool for managing LittleRed and foreign Redis instances",
	Long: `lrctl provides syntactic sugar for interacting with LittleRed resources,
but also supports unmanaged Redis instances (standalone, sentinel, cluster) via heuristics.`,
	SilenceUsage:  true, // don't print usage on RunE errors
	SilenceErrors: true, // main.go prints the error; avoid double-printing
}

func Execute() error {
	return rootCmd.Execute()
}

// listLittleReds returns ObjectKeys for all LittleRed CRs.
// Pass namespace="" to list across all namespaces.
func listLittleReds(ctx context.Context, k8sClient client.Client, namespace string) ([]client.ObjectKey, error) {
	lrList := &littleredv1alpha1.LittleRedList{}
	opts := []client.ListOption{}
	if namespace != "" {
		opts = append(opts, client.InNamespace(namespace))
	}
	if err := k8sClient.List(ctx, lrList, opts...); err != nil {
		return nil, fmt.Errorf("failed to list LittleRed resources: %w", err)
	}
	keys := make([]client.ObjectKey, len(lrList.Items))
	for i, lr := range lrList.Items {
		keys[i] = client.ObjectKey{Name: lr.Name, Namespace: lr.Namespace}
	}
	return keys, nil
}

// resolveTargets returns the set of (namespace, name) pairs a command should operate on.
// It honours --all-namespaces / -A and the positional name argument.
func resolveTargets(
	ctx context.Context, k8sClient client.Client, args []string, targetNS string,
) ([]client.ObjectKey, error) {
	if allNamespaces && len(args) > 0 {
		return nil, fmt.Errorf("a resource name may not be specified when --all-namespaces (-A) is set")
	}
	if len(args) == 1 {
		return []client.ObjectKey{{Name: args[0], Namespace: targetNS}}, nil
	}
	// No name given: list all in the target namespace (or all namespaces).
	listNS := targetNS
	if allNamespaces {
		listNS = ""
	}
	return listLittleReds(ctx, k8sClient, listNS)
}

// completeLittleRedNames is used as ValidArgsFunction on commands that accept an
// optional LittleRed resource name. It queries the API for names in the currently
// selected namespace so that <tab> completes resource names rather than filenames.
func completeLittleRedNames(_ *cobra.Command, args []string, _ string) ([]string, cobra.ShellCompDirective) {
	if len(args) >= 1 {
		// Already have a name — suppress all further completions.
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	k8sClient, _, _, defaultNS, err := k8s.NewClient(kubeconfig)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	targetNS := namespace
	if targetNS == "" {
		targetNS = defaultNS
	}
	keys, err := listLittleReds(context.Background(), k8sClient, targetNS)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	names := make([]string, len(keys))
	for i, key := range keys {
		names[i] = key.Name
	}
	return names, cobra.ShellCompDirectiveNoFileComp
}

// completeNamespaces is used as the completion function for the --namespace / -n flag.
func completeNamespaces(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
	k8sClient, _, _, _, err := k8s.NewClient(kubeconfig)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	nsList := &corev1.NamespaceList{}
	if err := k8sClient.List(context.Background(), nsList); err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	names := make([]string, len(nsList.Items))
	for i, ns := range nsList.Items {
		names[i] = ns.Name
	}
	return names, cobra.ShellCompDirectiveNoFileComp
}

func init() {
	rootCmd.PersistentFlags().StringVarP(
		&namespace, "namespace", "n", "", "If present, the namespace scope for this CLI request")
	rootCmd.PersistentFlags().StringVar(
		&kubeconfig, "kubeconfig", "", "Path to the kubeconfig file to use for CLI requests")
	rootCmd.PersistentFlags().BoolVar(
		&unmanaged, "unmanaged", false, "If true, skip looking for a LittleRed CR and use heuristics to find pods")
	rootCmd.PersistentFlags().StringVar(
		&kind, "kind", "sentinel", "The instance kind (standalone|sentinel|cluster) when using --unmanaged")
	rootCmd.PersistentFlags().BoolVarP(
		&allNamespaces, "all-namespaces", "A", false, "If present, list resources across all namespaces")
	rootCmd.PersistentFlags().BoolVar(&jsonOutput, "json", false, "If true, output as JSON")

	// Register completions for persistent flags defined here.
	// Cobra propagates these to all subcommands that inherit the flags.
	if err := rootCmd.RegisterFlagCompletionFunc("namespace", completeNamespaces); err != nil {
		panic(err)
	}
	kindCompletion := func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{modeStandalone, modeSentinel, modeCluster}, cobra.ShellCompDirectiveNoFileComp
	}
	if err := rootCmd.RegisterFlagCompletionFunc("kind", kindCompletion); err != nil {
		panic(err)
	}
}
