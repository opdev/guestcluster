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

// Package resources contains builder helpers. These helpers construct the
// real, typed Kubernetes objects (KubeVirt VirtualMachine/DataVolume,
// HyperShift HostedCluster/NodePool) that back a ClusterInstance. Keeping
// the builders isolated from the reconcilers makes the desired-state shape
// easy to unit test and easy to audit against upstream API docs.
package resources

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
)

// The operator sets LabelManagedBy to this value on every object it creates.
// The label scopes watches and list calls, and identifies owned resources
// for cleanup.
const LabelManagedBy = "opdev.io/managed-by"

// LabelInstance names the owning ClusterInstance on every backing object.
const LabelInstance = "opdev.io/cluster-instance"

// LabelPool names the owning ClusterPool on every ClusterInstance the pool
// controller creates. This lets code list a pool's instances with a label
// selector (client.MatchingLabels), instead of an unfiltered List plus a
// manual filter.
const LabelPool = "opdev.io/pool"

// ManagerName is the value written into LabelManagedBy.
const ManagerName = "guestcluster-operator"

// CommonLabels returns the standard label set for every object created on
// behalf of a ClusterInstance. Callers can find these objects with a label
// selector (for example during recycle or teardown), independent of owner
// references.
func CommonLabels(instance *brokerv1alpha1.ClusterInstance) map[string]string {
	return map[string]string{
		LabelManagedBy: ManagerName,
		LabelInstance:  instance.Name,
	}
}

// PoolLabels returns the label set that the ClusterPool controller applies to
// every ClusterInstance it creates. It combines LabelManagedBy with
// LabelPool, so callers can list instances that belong to a given pool with
// a single label selector.
func PoolLabels(poolName string) map[string]string {
	return map[string]string{
		LabelManagedBy: ManagerName,
		LabelPool:      poolName,
	}
}

// VMName is the deterministic name of the KubeVirt VirtualMachine backing a
// topology=crc ClusterInstance.
func VMName(instanceName string) string {
	return instanceName
}

// DataVolumeName is the deterministic name of the CDI DataVolume that
// provides the root disk for a topology=crc ClusterInstance's VM. This name
// is deliberately distinct from VMName. Recycle logic can delete and
// recreate the DataVolume to wipe guest state, while leaving the VM object
// in place. This avoids VM object churn and the owner-reference/
// controller-ref bookkeeping that recreating the VM would otherwise
// require.
func DataVolumeName(instanceName string) string {
	return instanceName + "-rootdisk"
}

// DefaultHostedClusterNamespace is the conventional namespace for HyperShift
// HostedCluster/NodePool objects. Its HCP pods land in "clusters-<name>".
const DefaultHostedClusterNamespace = "clusters"

// HostedClusterName is the deterministic name of the HostedCluster backing a
// topology=hcp-* ClusterInstance. Using the ClusterInstance's own name keeps
// the mapping trivially invertible.
func HostedClusterName(instanceName string) string {
	return instanceName
}

// NodePoolName is the deterministic name of the (default) NodePool backing a
// topology=hcp-* ClusterInstance's workers.
func NodePoolName(instanceName string) string {
	return instanceName + "-workers"
}

// AdminKubeconfigSecretName mirrors HyperShift's own naming convention for
// the Secret that holds the guest cluster's admin kubeconfig. The operator
// prefers the reference published on HostedCluster.Status.KubeConfig when
// present.
func AdminKubeconfigSecretName(hostedClusterName string) string {
	return hostedClusterName + "-admin-kubeconfig"
}

// KubeconfigSecretName is the name of the Secret where this operator writes
// or copies the guest cluster's admin kubeconfig, in the ClusterInstance's
// own namespace. This lets all topologies expose kubeconfigs the same way,
// regardless of where the source-of-truth Secret physically lives.
func KubeconfigSecretName(instanceName string) string {
	return instanceName + "-kubeconfig"
}

// KubeconfigSecretKey is the Secret data key that holds the raw kubeconfig
// bytes. This applies to the canonical per-instance/per-lease Secrets this
// operator writes, and to the raw handoff Secret the crc-agent produces
// (see RawKubeconfigSecretName). The key matches HyperShift's own
// convention, so code can copy admin-kubeconfig Secrets byte-for-byte
// without re-keying.
const KubeconfigSecretKey = "kubeconfig"

// OCPVersionSecretKey is the Secret data key where the crc-agent writes the
// observed in-guest OpenShift version, alongside the raw kubeconfig, for
// topology=crc instances (see RawKubeconfigSecretName). The
// management-cluster ClusterInstance controller uses the published API route
// only for a readiness check. It relies on the crc-agent, which reaches the
// guest API over the hypervisor-local VM network, to report this value.
const OCPVersionSecretKey = "ocpVersion"

// VMIUIDSecretKey is the Secret data key where crc-agent records the UID of
// the VMI it configured. The controller uses it to reject a handoff from a
// VMI that was deleted or replaced while the agent was still running.
const VMIUIDSecretKey = "vmiUID"

// RawKubeconfigSecretName defines the contract between the crc-agent and
// the ClusterInstance controller for topology=crc instances. The crc-agent
// runs with network access to the nested CRC VM. It drives in-guest
// `crc start`/`crc status`/`oc` operations over SSH, extracts the guest
// admin kubeconfig and the observed OCP version, and publishes both into
// this Secret (under keys KubeconfigSecretKey and OCPVersionSecretKey) in
// the ClusterInstance's namespace. The ClusterInstance controller only
// reads this Secret; it never SSHes into the guest itself. See
// cmd/crc-agent for the (stubbed) producer side.
func RawKubeconfigSecretName(instanceName string) string {
	return instanceName + "-crc-raw-kubeconfig"
}

// RawKubeconfigSecretNameForVMI fences a handoff to one VMI. A stale agent
// can only write its own Secret and cannot replace the current VMI handoff.
func RawKubeconfigSecretNameForVMI(instanceName, vmiUID string) string {
	return RawKubeconfigSecretName(instanceName) + "-" + vmiUID
}

// LeaseKubeconfigSecretName is the name of the Secret where the
// ClusterLease controller copies a bound ClusterInstance's kubeconfig, in
// the ClusterLease's own namespace. CI consumers read this Secret instead
// of the instance's Secret. They never need to know which concrete instance
// backs their lease, and the mapping can change across recycle/rebind
// cycles.
func LeaseKubeconfigSecretName(leaseName string) string {
	return leaseName + "-kubeconfig"
}

// CRCAgentJobName is the deterministic name of the per-VMI Kubernetes Job
// that runs the crc-agent container for a topology=crc ClusterInstance.
// This is a run-to-completion Job, not a long-running Deployment. The
// crc-agent's work is a one-shot task per VM boot: SSH into the freshly
// booted CRC VM, run the native post-boot fixups, and publish
// RawKubeconfigSecretName. The VMI hash keeps the name within the Kubernetes
// DNS label limit when an instance name is at its maximum length.
func CRCAgentJobName(instanceName, vmiUID string) string {
	const suffix = "-crc-agent-"
	const hashLength = 12
	hash := sha256.Sum256([]byte(vmiUID))
	uidHash := hex.EncodeToString(hash[:])[:hashLength]
	maxPrefixLength := 63 - len(suffix) - hashLength
	if len(instanceName) > maxPrefixLength {
		instanceName = instanceName[:maxPrefixLength]
	}
	return instanceName + suffix + uidHash
}

// CRCAPIServiceName is the deterministic name of the ClusterIP Service that
// selects the KubeVirt virt-launcher pod backing a topology=crc
// ClusterInstance's VM (via the vm.kubevirt.io/name label). The Service
// exposes the guest API server port (6443) inside the management cluster,
// so a Route can front it. See BuildCRCAPIService.
func CRCAPIServiceName(instanceName string) string {
	return instanceName + "-crc-api"
}

// CRCAPIRouteName is the deterministic name of the passthrough Route that
// exposes a topology=crc ClusterInstance's guest API server externally, via
// CRCAPIServiceName. See BuildCRCAPIRoute.
func CRCAPIRouteName(instanceName string) string {
	return instanceName + "-crc-api"
}

// APIServerHostname returns the deterministic, externally-routable admin
// hostname this operator uses to front a guest cluster's API server. Both
// the crc path (see ensureCRCAPIRoute) and the hcp path (see
// ensureHyperShiftBacking) share it, so the naming convention lives in one
// place.
func APIServerHostname(instanceName, mgmtIngressDomain string) string {
	return fmt.Sprintf("api-%s.%s", instanceName, mgmtIngressDomain)
}

// CRCAPIHostnameEnvVar is the environment variable the crc-agent Job reads
// to learn the externally-routable hostname. This is the Route host the
// ClusterInstance controller chose (for example
// api-<instance>.apps.<mgmt-domain>). The crc-agent mints its
// external-facing serving certificate for this hostname, and rewrites the
// published kubeconfig's server URL to it. See BuildCRCAgentJob.
const CRCAPIHostnameEnvVar = "CRC_API_HOSTNAME"

// PullSecretDataKey is the conventional data key under which a Kubernetes
// pull-secret Secret (type kubernetes.io/dockerconfigjson) stores its JSON
// payload. The operator uses this key both to validate a
// ClusterTemplate.PullSecretRef Secret at reconcile time, and to select the
// key mounted into the crc-agent Job.
const PullSecretDataKey = ".dockerconfigjson"

// ClusterPullSecretNamespace and ClusterPullSecretName identify the global
// pull secret every OpenShift cluster carries (the one `oc get secret
// pull-secret -n openshift-config` reads). When a ClusterTemplate leaves
// PullSecretRef unset, the operator defaults to a copy of this Secret. This
// avoids requiring an admin to separately provide a credential the
// management cluster already has.
const (
	ClusterPullSecretNamespace = "openshift-config"
	ClusterPullSecretName      = "pull-secret"
)

// DefaultPullSecretName is the deterministic name of the per-instance Secret
// the operator creates as a copy of ClusterPullSecretName, when
// ClusterTemplate.PullSecretRef is left unset. The operator places it in
// whatever namespace the topology needs: the instance's own namespace for
// crc, the HostedCluster's namespace for hcp-*. See resolvePullSecret in the
// ClusterInstance controller.
func DefaultPullSecretName(instanceName string) string {
	return instanceName + "-pull-secret"
}

// HCPWorkerSSHKeyDataKey is the conventional data key that a
// ClusterTemplate.HCPWorkerSSHKeyRef Secret must carry its SSH public key
// under. This matches HyperShift's own HostedCluster.spec.sshKey Secret
// convention exactly (see hypershift-operator/controllers/hostedcluster,
// which rejects a referenced Secret that is missing this key).
const HCPWorkerSSHKeyDataKey = "id_rsa.pub"

// HCPWorkerSSHKeyName is the deterministic name of the per-instance copy of
// ClusterTemplate.HCPWorkerSSHKeyRef. The operator creates this copy in the
// HostedCluster's own namespace. HostedCluster.spec.sshKey must reference a
// Secret in that same namespace, because LocalObjectReference resolves
// relative to the referencing object. As a result, the operator cannot use
// the Secret referenced by HCPWorkerSSHKeyRef directly.
func HCPWorkerSSHKeyName(instanceName string) string {
	return instanceName + "-hcp-ssh-key"
}

// BundleSSHKeyDataKey lists the conventional data keys under which a
// ClusterTemplate.BundleSSHKeyRef Secret stores the CRC bundle's SSH private
// key (the bundle's "id_ecdsa_crc" file). The resolver checks the keys in
// order, and uses the first one present in the Secret's Data.
var BundleSSHKeyDataKeys = []string{"id_ecdsa", "ssh-privatekey", "id_rsa"}

// OperatorNamespaceEnvVar is the environment variable the manager Deployment
// sets (via the downward API, see config/manager/manager.yaml) to its own
// namespace. The operator creates cluster-scoped-adjacent resources that
// still need a concrete namespace here: the CRCBundle golden PVC, its
// derived SSH-key Secret, and the bundle-prep Job.
const OperatorNamespaceEnvVar = "OPERATOR_NAMESPACE"

// OperatorNamespace falls back to defaultOperatorNamespace if
// OperatorNamespaceEnvVar is unset and the in-cluster service account
// namespace file is not present (for example, when running via `make run`
// outside a Pod without exporting the env var).
const defaultOperatorNamespace = "default"

// serviceAccountNamespaceFile is the standard in-cluster path every Pod's
// mounted ServiceAccount token includes, containing the Pod's own namespace.
const serviceAccountNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// OperatorNamespace resolves the namespace the operator runs in. Callers use
// this namespace to place shared, cluster-scoped-adjacent objects: the
// CRCBundle golden PVC, its SSH-key Secret, and the bundle-prep Job.
// OperatorNamespace prefers OperatorNamespaceEnvVar, falls back to reading
// the in-cluster ServiceAccount namespace file, and finally falls back to
// defaultOperatorNamespace.
func OperatorNamespace() string {
	if ns := os.Getenv(OperatorNamespaceEnvVar); ns != "" {
		return ns
	}
	if data, err := os.ReadFile(serviceAccountNamespaceFile); err == nil {
		if ns := string(data); ns != "" {
			return ns
		}
	}
	return defaultOperatorNamespace
}

// CRCBundleName is the deterministic, version+arch-keyed name for a
// CRCBundle object. Because the name derives only from version and arch,
// two ClusterPools requesting the same crcVersion always resolve to the
// same CRCBundle object. Whichever pool reconciles first creates it. Every
// subsequent pool, from any namespace since CRCBundle is cluster-scoped,
// finds and reuses it.
func CRCBundleName(version, arch string) string {
	return "crc-" + version + "-" + arch
}

// DefaultCRCArch is used when a ClusterTemplate/CRCBundle leaves Arch empty.
const DefaultCRCArch = "amd64"

// GoldenPVCName is the deterministic name of the PersistentVolumeClaim (in
// OperatorNamespace) that a CRCBundle's bundle-prep Job extracts crc.qcow2
// into. ClusterInstance DataVolumes clone from this PVC once the bundle is
// Ready.
func GoldenPVCName(version, arch string) string {
	return CRCBundleName(version, arch) + "-golden"
}

// BundleSSHKeySecretName is the deterministic name of the Secret (in
// OperatorNamespace) where a CRCBundle's bundle-prep Job writes the
// bundle's id_ecdsa_crc SSH private key, under data key "id_ecdsa", once
// extraction succeeds.
func BundleSSHKeySecretName(version, arch string) string {
	return CRCBundleName(version, arch) + "-ssh-key"
}

// BundleMetadataConfigMapName is the deterministic name of the ConfigMap
// (in OperatorNamespace) where a CRCBundle's bundle-prep Job writes derived
// metadata: the bundle's own reported OCP version and the verified sha256
// of the downloaded .crcbundle artifact. The CRCBundleReconciler reads this
// metadata back into CRCBundle.status once the Job succeeds.
func BundleMetadataConfigMapName(version, arch string) string {
	return CRCBundleName(version, arch) + "-metadata"
}

// BundlePrepJobName is the deterministic name of the run-to-completion
// Kubernetes Job (in OperatorNamespace) that downloads, verifies, and
// extracts a CRCBundle's official .crcbundle artifact.
func BundlePrepJobName(version, arch string) string {
	return CRCBundleName(version, arch) + "-prep"
}

// BundleMirrorURL returns the deterministic official OpenShift mirror URL for
// a CRC bundle version/arch, matching crc-org/crc's own
// constants.DefaultBundleURLBase pattern.
func BundleMirrorURL(version, arch string) string {
	name := "crc_libvirt_" + version + "_" + arch + ".crcbundle"
	return "https://mirror.openshift.com/pub/openshift-v4/clients/crc/bundles/openshift/" + version + "/" + name
}

// BundleMirrorSHA256URL returns the deterministic official checksum file URL
// published alongside a CRC bundle version/arch on the OpenShift mirror.
func BundleMirrorSHA256URL(version string) string {
	return "https://mirror.openshift.com/pub/openshift-v4/clients/crc/bundles/openshift/" + version + "/sha256sum.txt"
}
