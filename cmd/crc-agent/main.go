/*
Copyright 2026.

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

// Command crc-agent is the CRC-in-VM side helper for topology=crc ClusterInstances.
//
// The ClusterInstance controller (internal/controller/clusterinstance_crc.go) boots
// a KubeVirt VirtualMachine from an extracted CRC bundle's crc.qcow2 disk image. The
// controller never reaches into that VM itself. Once the VirtualMachineInstance
// reports an IP, the controller creates a run-to-completion Kubernetes Job that runs
// this binary (see internal/resources.BuildCRCAgentJob), and it waits for the Job to
// publish a kubeconfig.
//
// A freshly booted CRC bundle VM is "parked" on purpose: its kubelet is disabled,
// its pull secret is an empty "{}", and its kubelet client and serving certs are
// valid for only ~30 days from when the bundle was built. Bringing the VM up
// therefore requires post-boot fixups:
//
//  1. Wait for the CRC VM's SSH endpoint (cfg.SSHHost:22) to accept connections.
//  2. Run guest-side fixups over SSH (guest.go):
//     - Configure dnsmasq so *.crc.testing resolves inside the VM.
//     - Swap the SSH public key with a freshly generated ed25519 key.
//     - Start the kubelet.
//     - Regenerate the admin CA and client cert (CA bootstrap), patching the
//     admin-kubeconfig-client-ca configmap via oc-on-guest so the new cert
//     is trusted before any typed client connects.
//  3. Reconnect SSH with the new key and open an SSH port-forward to
//     api.crc.testing:6443 inside the VM (cluster client tunnel).
//  4. Run cluster-level fixups via typed Kubernetes/OpenShift clients
//     routed through the SSH tunnel (cluster.go):
//     - Approve pending kubelet CSRs.
//     - Inject the real pull secret.
//     - Update kubeadmin + developer passwords (bcrypt in-process, no podman).
//     - Install the stable serving cert for cfg.APIHostname (the
//     management-cluster passthrough Route host the ClusterInstance
//     controller already provisioned, see
//     internal/resources.BuildCRCAPIRoute) and patch the apiserver's
//     namedCertificates so the API server presents it for that SNI.
//     - Wait for key cluster operators to become Available.
//     - Read the OpenShift version from ClusterVersion.
//  5. Produce a routable admin kubeconfig (server rewritten to
//     https://<cfg.APIHostname>) and publish it together with the OCP
//     version as a VMI-specific Secret in the
//     ClusterInstance's namespace (keys "kubeconfig" and "ocpVersion").
//     This Secret is the only contract between crc-agent and the
//     ClusterInstance controller. ensureCRCBacking (clusterinstance_crc.go)
//     reads this Secret and never SSHes anywhere itself.
//
// crc-agent is deployed as a Kubernetes Job, one per ClusterInstance VMI. The
// controller creates a new, VMI-scoped Job after a VMI replacement. The Job runs to completion.
// Success means the raw kubeconfig Secret now exists. Failure exits non-zero,
// and the Job's BackoffLimit governs retries.
//
// The crc-agent container image needs only the following host tools:
//   - curl, tar (zstd), jq, sha256sum, sed, coreutils, qemu-img: for the
//     bundle-prep Job that reuses this same image.
//   - oc: required by the bundle-prep Job (publishes SSH key + metadata).
//   - openssh-clients: needed by the bundle-prep Job's shell script.
//
// The crc-cloud binary and Pulumi CLI have been removed from the image.
package main

import (
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"
	"time"

	gossh "golang.org/x/crypto/ssh"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	k8syaml "sigs.k8s.io/yaml"

	"github.com/caxu-rh/guestcluster-operator/internal/resources"
)

// config holds the crc-agent's runtime configuration. It comes only from
// environment variables and flags, so a plain container in a Kubernetes Job
// spec can set it (see internal/resources.BuildCRCAgentJob) without any
// additional CRDs of its own.
type config struct {
	Namespace      string // ClusterInstance/Secret namespace
	InstanceName   string // ClusterInstance name; also the CRC VM name (resources.VMName)
	SSHHost        string // CRC VM's reachable IP/host (populated from VMI status by the controller)
	ExpectedVMIUID string // UID of the VMI this Job is allowed to configure
	SSHPort        int
	SSHUser        string
	SSHKeyPath     string // path to the CRC bundle's SSH private key, mounted into the container

	// APIHostname is the externally-routable hostname of the management
	// cluster's passthrough Route fronting the guest API server (see
	// internal/resources.BuildCRCAPIRoute), populated by the
	// ClusterInstance controller via CRC_API_HOSTNAME. Used to validate the
	// mounted identity and rewrite the published kubeconfig's server URL.
	APIHostname string

	PullSecretPath string // path to the real pull secret file, mounted into the container
	Identity       resources.CRCIdentity
	IdentityPath   string

	SSHReadyTimeout  time.Duration // how long to wait for the VM's sshd to accept connections
	SSHRetryInterval time.Duration // how often to retry the SSH-reachability check
}

func configFromEnv() config {
	c := config{
		Namespace:      os.Getenv("INSTANCE_NAMESPACE"),
		InstanceName:   os.Getenv("INSTANCE_NAME"),
		SSHHost:        os.Getenv("CRC_SSH_HOST"),
		ExpectedVMIUID: os.Getenv("CRC_VMI_UID"),
		SSHUser:        envDefault("CRC_SSH_USER", "core"),
		SSHKeyPath:     envDefault("CRC_SSH_KEY_PATH", resources.CRCAgentSSHKeyPath()),
		APIHostname:    os.Getenv(resources.CRCAPIHostnameEnvVar),
		PullSecretPath: envDefault("PULL_SECRET_PATH", resources.CRCAgentPullSecretPath()),
		IdentityPath:   envDefault("CRC_IDENTITY_PATH", "/etc/crc-agent/identity"),
	}
	c.SSHPort = 22
	c.SSHReadyTimeout = 5 * time.Minute
	c.SSHRetryInterval = 10 * time.Second
	return c
}

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// clusterInfo is what a successful fetchClusterInfo call returns: the raw admin
// kubeconfig bytes (rewritten to the externally-routable API hostname's URL)
// and the OpenShift version string reported by the guest cluster.
type clusterInfo struct {
	Kubeconfig []byte
	OCPVersion string
}

func main() {
	opts := zap.Options{Development: true}
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	log := logf.Log.WithName("crc-agent")

	cfg := configFromEnv()
	flag.StringVar(&cfg.InstanceName, "instance-name", cfg.InstanceName, "ClusterInstance name this agent serves")
	flag.StringVar(&cfg.Namespace, "namespace", cfg.Namespace, "Namespace of the ClusterInstance/Secret")
	flag.StringVar(&cfg.SSHHost, "ssh-host", cfg.SSHHost, "SSH-reachable host/IP of the CRC VM")
	flag.StringVar(&cfg.ExpectedVMIUID, "vmi-uid", cfg.ExpectedVMIUID, "UID of the CRC VMI")
	flag.Parse()

	if cfg.InstanceName == "" || cfg.Namespace == "" || cfg.SSHHost == "" || cfg.ExpectedVMIUID == "" {
		const missingConfigMsg = "INSTANCE_NAME, INSTANCE_NAMESPACE, CRC_SSH_HOST and CRC_VMI_UID " +
			"(or their --flag equivalents) are required"
		log.Error(fmt.Errorf("missing required configuration"), missingConfigMsg)
		os.Exit(1)
	}

	bundleSigner, err := loadSigner(cfg.SSHKeyPath)
	if err != nil {
		log.Error(err, "mounted bundle SSH key failed pre-flight validation")
		os.Exit(1)
	}
	identity, err := loadCRCIdentity(cfg.IdentityPath, cfg.APIHostname)
	if err != nil {
		log.Error(err, "mounted CRC identity failed pre-flight validation")
		os.Exit(1)
	}
	cfg.Identity = identity

	restCfg, err := kubeRESTConfig()
	if err != nil {
		log.Error(err, "unable to build Kubernetes client")
		os.Exit(1)
	}
	kc, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		log.Error(err, "unable to build Kubernetes client")
		os.Exit(1)
	}
	vmiClient, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		log.Error(err, "unable to build KubeVirt client")
		os.Exit(1)
	}

	ctx := ctrl.SetupSignalHandler()
	watchedCtx, stopVMIWatch, err := monitorCRCVMILifecycle(ctx, vmiClient, cfg, log)
	if err != nil {
		log.Error(err, "unable to monitor CRC VirtualMachineInstance")
		os.Exit(1)
	}
	defer stopVMIWatch()

	log.Info("starting crc-agent", "instance", cfg.InstanceName, "namespace", cfg.Namespace,
		"sshHost", cfg.SSHHost, "vmiUid", cfg.ExpectedVMIUID)

	info, err := fetchClusterInfo(watchedCtx, log, cfg, bundleSigner)
	if err != nil {
		if errors.Is(context.Cause(watchedCtx), errCRCVMINoLongerCurrent) {
			log.Info("CRC VirtualMachineInstance is no longer current; ending agent run")
			return
		}
		log.Error(err, "failed to bring up CRC cluster")
		os.Exit(1)
	}

	if err := ensureExpectedCRCVMIRunning(ctx, vmiClient.Resource(crcVMIGVR).Namespace(cfg.Namespace), cfg); err != nil {
		if errors.Is(err, errCRCVMINoLongerCurrent) {
			log.Info("CRC VirtualMachineInstance is no longer current; not publishing handoff")
			return
		}
		log.Error(err, "unable to verify CRC VirtualMachineInstance before publishing handoff")
		os.Exit(1)
	}
	if err := publishRawKubeconfig(watchedCtx, kc, cfg, info); err != nil {
		log.Error(err, "failed to publish raw kubeconfig secret")
		os.Exit(1)
	}

	log.Info("published raw kubeconfig, crc-agent run complete", "ocpVersion", info.OCPVersion)
}

func kubeRESTConfig() (*rest.Config, error) {
	restCfg, err := rest.InClusterConfig()
	if err != nil {
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			return nil, fmt.Errorf("not running in-cluster and KUBECONFIG is not set: %w", err)
		}
		restCfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("building config from KUBECONFIG: %w", err)
		}
	}
	return restCfg, nil
}

// fetchClusterInfo is the main orchestration function. It performs these steps:
//  1. Waits for the CRC VM's SSH endpoint to come up.
//  2. Runs guest-side fixups (dnsmasq, kubelet, SSH key swap, CA bootstrap).
//  3. Reconnects with the new SSH key.
//  4. Opens the SSH tunnel and runs cluster-level fixups.
//  5. Reads the bundle kubeconfig, rewrites the server to the externally
//     routable API hostname's URL.
//  6. Returns the kubeconfig + OCP version.
func fetchClusterInfo(ctx context.Context, log logrLike, cfg config, bundleSigner gossh.Signer) (*clusterInfo, error) {
	// 1. Wait for SSH
	log.Info("waiting for CRC VM SSH endpoint", "addr", fmt.Sprintf("%s:%d", cfg.SSHHost, cfg.SSHPort))
	if err := WaitForConnectivity(ctx, cfg.SSHHost, cfg.SSHPort, cfg.SSHRetryInterval); err != nil {
		return nil, fmt.Errorf("waiting for CRC VM SSH endpoint: %w", err)
	}

	// 2. Connect with bundle key and run guest fixups
	log.Info("connecting to CRC VM with bundle SSH key")
	runner, err := NewRunner(cfg.SSHHost, cfg.SSHPort, cfg.SSHUser, bundleSigner)
	if err != nil {
		return nil, fmt.Errorf("SSH connect (bundle key): %w", err)
	}
	stopClosingRunner := closeRunnerOnCancellation(ctx, runner)

	log.Info("running guest-side fixups")
	guestRes, err := RunGuestFixups(runner, cfg, log)
	stopClosingRunner()
	if err != nil {
		_ = runner.Close()
		return nil, fmt.Errorf("guest fixups: %w", err)
	}
	_ = runner.Close()

	// 3. Reconnect with the freshly swapped SSH key
	log.Info("reconnecting with new SSH key")
	runner2, err := NewRunner(cfg.SSHHost, cfg.SSHPort, cfg.SSHUser, guestRes.SSHSigner)
	if err != nil {
		return nil, fmt.Errorf("SSH reconnect (new key): %w", err)
	}
	stopClosingRunner2 := closeRunnerOnCancellation(ctx, runner2)
	defer stopClosingRunner2()
	defer func() { _ = runner2.Close() }()

	// 4. Build typed guest clients (routed through the SSH tunnel).
	// The CA here must be guestRes.ServerCAPEM, the bundle's own
	// server-serving-cert CA. It must not be the mounted client CA: that CA
	// only signs the client cert (trusted through admin-kubeconfig-client-ca)
	// and never signed the API server's own TLS certificate for the
	// api.crc.testing SNI. Using that CA here produces "certificate signed
	// by unknown authority". See guestResult.ServerCAPEM's doc comment.
	log.Info("building typed guest clients via SSH tunnel")
	guestClients, err := NewGuestClients(runner2, guestRes.ServerCAPEM, guestRes.ClientCertPEM, guestRes.ClientKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("building guest clients: %w", err)
	}

	// Read the pull secret
	pullSecretJSON, err := os.ReadFile(cfg.PullSecretPath)
	if err != nil {
		return nil, fmt.Errorf("reading pull secret: %w", err)
	}

	if cfg.APIHostname == "" {
		return nil, fmt.Errorf("CRC_API_HOSTNAME is not set (expected to be populated by the ClusterInstance controller)")
	}

	fixupCfg := clusterFixupConfig{
		PullSecretJSON: pullSecretJSON,
		APIHostname:    cfg.APIHostname,
		Identity:       cfg.Identity,
	}

	log.Info("running cluster-level fixups")
	fixupRes, err := RunClusterFixups(ctx, guestClients, fixupCfg, log)
	if err != nil {
		return nil, fmt.Errorf("cluster fixups: %w", err)
	}

	// 5. Produce a routable kubeconfig. Read the kubeconfig from the guest,
	// rewrite its server URL to the management cluster's passthrough Route,
	// and embed the external API serving cert as the trusted CA. Because
	// the cert is self-signed, it also doubles as its own trust root.
	log.Info("reading kubeconfig from guest")
	// This call retries because applyExternalAPIPatches, called above through
	// RunClusterFixups, can trigger a kube-apiserver-operator revision
	// rollout. That rollout can transiently invalidate the guest's on-disk
	// kubeconfig while it is being re-rendered, even though cluster
	// operators already reported Available. See readGuestKubeconfig.
	bundleKubeconfigRaw, err := readGuestKubeconfig(runner2, 10*time.Minute, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("reading guest kubeconfig: %w", err)
	}
	externalKubeconfig, err := rewriteKubeconfigServer(
		[]byte(bundleKubeconfigRaw),
		fmt.Sprintf("https://%s:443", cfg.APIHostname),
		fixupRes.ExternalAPICACertPEM,
	)
	if err != nil {
		return nil, fmt.Errorf("rewriting kubeconfig server: %w", err)
	}

	log.Info("cluster is up", "ocpVersion", fixupRes.OCPVersion)
	return &clusterInfo{Kubeconfig: externalKubeconfig, OCPVersion: fixupRes.OCPVersion}, nil
}

// rewriteKubeconfigServer replaces the server URL and certificate-authority-data
// of the "admin" cluster entry in a YAML kubeconfig, making the kubeconfig
// externally routable. The bundle default server
// (https://api.crc.testing:6443) resolves only inside the VM. Its baked-in
// CA does not cover the self-signed external-API serving certificate that
// the cluster fixups apply (cluster.go's applyExternalAPIPatches). For this
// reason, caCertPEM (that same self-signed cert, which is its own trust
// root) replaces the original CA data, instead of disabling TLS
// verification outright.
func rewriteKubeconfigServer(kubeconfigYAML []byte, newServer string, caCertPEM []byte) ([]byte, error) {
	var kc map[string]interface{}
	if err := k8syaml.Unmarshal(kubeconfigYAML, &kc); err != nil {
		return nil, fmt.Errorf("parsing kubeconfig: %w", err)
	}

	clusters, ok := kc["clusters"].([]interface{})
	if !ok || len(clusters) == 0 {
		return nil, fmt.Errorf("kubeconfig has no clusters entries")
	}
	for _, c := range clusters {
		entry, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		clusterField, ok := entry["cluster"].(map[string]interface{})
		if !ok {
			continue
		}
		clusterField["server"] = newServer
		clusterField["certificate-authority-data"] = base64.StdEncoding.EncodeToString(caCertPEM)
		delete(clusterField, "certificate-authority")
	}

	out, err := k8syaml.Marshal(kc)
	if err != nil {
		return nil, fmt.Errorf("re-marshalling kubeconfig: %w", err)
	}
	return out, nil
}

// loadSigner reads a private key file and parses it into a gossh.Signer.
// Used as a pre-flight sanity check in main() and to provide the initial
// bundle SSH credential.
func loadSigner(path string) (gossh.Signer, error) {
	keyBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading SSH private key %q: %w", path, err)
	}
	signer, err := gossh.ParsePrivateKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("parsing SSH private key %q: %w", path, err)
	}
	return signer, nil
}

func loadCRCIdentity(path, hostname string) (resources.CRCIdentity, error) {
	keys := []string{
		resources.CRCIdentityClientCAKey,
		resources.CRCIdentityClientCertKey,
		resources.CRCIdentityClientPrivateKey,
		resources.CRCIdentityServingCertKey,
		resources.CRCIdentityServingPrivateKey,
	}
	data := make(map[string][]byte, len(keys))
	for _, key := range keys {
		value, err := os.ReadFile(path + "/" + key)
		if err != nil {
			return resources.CRCIdentity{}, fmt.Errorf("reading identity key %q: %w", key, err)
		}
		data[key] = value
	}
	return resources.CRCIdentityFromSecretData(data, hostname)
}

// publishRawKubeconfig creates or updates the raw kubeconfig Secret that the
// ClusterInstance controller's ensureCRCBacking reads (see
// internal/resources.RawKubeconfigSecretNameForVMI/KubeconfigSecretKey/OCPVersionSecretKey).
func publishRawKubeconfig(ctx context.Context, kc *kubernetes.Clientset, cfg config, info *clusterInfo) error {
	name := resources.RawKubeconfigSecretNameForVMI(cfg.InstanceName, cfg.ExpectedVMIUID)
	secretsClient := kc.CoreV1().Secrets(cfg.Namespace)

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cfg.Namespace,
			Labels: map[string]string{
				resources.LabelManagedBy: "crc-agent",
				resources.LabelInstance:  cfg.InstanceName,
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			resources.KubeconfigSecretKey: info.Kubeconfig,
			resources.OCPVersionSecretKey: []byte(info.OCPVersion),
			resources.VMIUIDSecretKey:     []byte(cfg.ExpectedVMIUID),
		},
	}

	_, err := secretsClient.Create(ctx, secret, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		existing, getErr := secretsClient.Get(ctx, name, metav1.GetOptions{})
		if getErr != nil {
			return getErr
		}
		if reflect.DeepEqual(existing.Data, secret.Data) && existing.Type == secret.Type {
			return nil
		}
		existing.Data, existing.Type = secret.Data, secret.Type
		_, err = secretsClient.Update(ctx, existing, metav1.UpdateOptions{})
	}
	return err
}

// logrLike is the minimal logging interface that fetchClusterInfo,
// waitForSSH, and similar functions need. logr.Logger satisfies this
// interface, so this file does not need to import github.com/go-logr/logr
// directly, beyond controller-runtime's own re-export.
type logrLike interface {
	Info(msg string, keysAndValues ...any)
}
