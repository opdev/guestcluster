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

package resources

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	brokerv1alpha1 "github.com/opdev/guestcluster/api/v1alpha1"
)

// CRCAgentImageEnvVar is the environment variable the operator reads (on
// its own manager Deployment) to determine which container image to run
// the per-instance crc-agent Job with. This mirrors the common "related
// image" pattern (compare with OLM's RELATED_IMAGE_* convention), so an
// admin can pin or override the crc-agent image without a code change.
const CRCAgentImageEnvVar = "CRC_AGENT_IMAGE"

// DefaultCRCAgentImage is used when CRCAgentImageEnvVar is unset. Callers
// are expected to override this in production via the manager Deployment's
// environment (see config/manager/manager.yaml).
const DefaultCRCAgentImage = "opdev.io/guestcluster-operator-crc-agent:latest"

const CRCAgentClusterRoleEnvVar = "CRC_AGENT_CLUSTER_ROLE"
const DefaultCRCAgentClusterRole = "crc-agent-instance-role"

// CRCAgentClusterRole returns the installed name, including any Kustomize prefix.
func CRCAgentClusterRole() string {
	if name := os.Getenv(CRCAgentClusterRoleEnvVar); name != "" {
		return name
	}
	return DefaultCRCAgentClusterRole
}

// CRCAgentAccountName is stable for an instance and stays within the DNS label limit.
// The hash distinguishes names that share a truncated prefix.
func CRCAgentAccountName(instanceName string) string {
	return crcAgentResourceName(instanceName, "-crc-agent-")
}

func CRCAgentBindingName(instanceName string) string {
	return crcAgentResourceName(instanceName, "-crc-agent-binding-")
}

func crcAgentResourceName(instanceName, suffix string) string {
	hash := sha256.Sum256([]byte(instanceName))
	digest := hex.EncodeToString(hash[:])[:12]
	prefix := instanceName
	if len(prefix) > 63-len(suffix)-len(digest) {
		prefix = prefix[:63-len(suffix)-len(digest)]
	}
	// A truncated DNS subdomain can end at a dot. The appended hyphen
	// would then start the next DNS label, which the API rejects.
	prefix = strings.TrimRight(prefix, "-.")
	return prefix + suffix + digest
}

const (
	crcAgentPullSecretMountPath = "/etc/crc-agent/pull-secret"
	crcAgentSSHKeyMountPath     = "/etc/crc-agent/ssh"
	crcAgentSSHKeyFileName      = "id_ecdsa"
)

// CRCAgentSSHKeyPath is the fixed in-container path where the bundle SSH
// private key is mounted, regardless of the source Secret's data key name
// (see BuildCRCAgentJob's use of a Secret volume `items` remap).
func CRCAgentSSHKeyPath() string {
	return crcAgentSSHKeyMountPath + "/" + crcAgentSSHKeyFileName
}

// CRCAgentPullSecretPath is the fixed in-container path where the
// pull-secret Secret's PullSecretDataKey entry is mounted.
func CRCAgentPullSecretPath() string {
	return crcAgentPullSecretMountPath + "/" + PullSecretDataKey
}

// BuildCRCAgentJob constructs the per-ClusterInstance, run-to-completion
// Kubernetes Job that drives the topology=crc post-boot provisioning flow.
// The Job SSHes, as user "core" using the given SSH key Secret, into the
// CRC VM at vmIP and runs the post-boot fixups natively (see
// cmd/crc-agent). It then publishes the resulting kubeconfig into
// RawKubeconfigSecretName.
//
// sshKeySecretName is the name (in instance.Namespace) of the Secret
// holding the bundle's SSH private key. bundleKeyDataKey is the data key
// within that Secret that holds it. The caller resolves both,
// either from the template's BundleSSHKeyRef (manual/fallback path, with
// the data key resolved against BundleSSHKeyDataKeys by the caller's
// precheck), or from an instance-owned copy of the Ready CRCBundle's
// Status.SSHKeySecretRef (turnkey path, which always uses data key
// "id_ecdsa" per the bundle-prep script).
// image is the crc-agent container image to run (see CRCAgentImageEnvVar).
// apiHostname is the externally routable hostname for which the
// ClusterInstance controller already provisioned a passthrough Route (see
// BuildCRCAPIRoute); the crc-agent uses it to select the mounted guest API
// server certificate and to rewrite the published
// kubeconfig's server URL. pullSecretName is the name (in
// instance.Namespace) of the Secret holding the pull-secret to inject into
// the guest cluster. The caller resolves it, either from the template's
// explicit PullSecretRef, or from the namespace's default pull secret (see
// ClusterInstanceReconciler.resolvePullSecret).
func BuildCRCAgentJob(instance *brokerv1alpha1.ClusterInstance, vmIP, vmiUID, sshKeySecretName, bundleKeyDataKey, identitySecretName, image, apiHostname, pullSecretName string) *batchv1.Job {
	labels := CommonLabels(instance)

	backoffLimit := int32(2)
	activeDeadline := int64(45 * 60) // 45 minutes: generous upper bound on a single post-boot fixup run.

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:            CRCAgentJobName(instance.Name, vmiUID),
			OwnerReferences: InstanceOwnerReferences(instance),
			Namespace:       instance.Namespace,
			Labels:          labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:          &backoffLimit,
			ActiveDeadlineSeconds: &activeDeadline,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyNever,
					ServiceAccountName: CRCAgentAccountName(instance.Name),
					Containers: []corev1.Container{
						{
							Name:  "crc-agent",
							Image: image,
							Env: []corev1.EnvVar{
								{Name: "INSTANCE_NAME", Value: instance.Name},
								{Name: "INSTANCE_UID", Value: string(instance.UID)},
								{Name: "INSTANCE_NAMESPACE", Value: instance.Namespace},
								{Name: "CRC_SSH_HOST", Value: vmIP},
								{Name: "CRC_VMI_UID", Value: vmiUID},
								{Name: "CRC_SSH_KEY_PATH", Value: CRCAgentSSHKeyPath()},
								{Name: "CRC_IDENTITY_PATH", Value: "/etc/crc-agent/identity"},
								{Name: "PULL_SECRET_PATH", Value: CRCAgentPullSecretPath()},
								{Name: CRCAPIHostnameEnvVar, Value: apiHostname},
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "pull-secret", MountPath: crcAgentPullSecretMountPath, ReadOnly: true},
								{Name: "bundle-ssh-key", MountPath: crcAgentSSHKeyMountPath, ReadOnly: true},
								{Name: "identity", MountPath: "/etc/crc-agent/identity", ReadOnly: true},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "pull-secret",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName: pullSecretName,
								},
							},
						},
						{
							Name: "bundle-ssh-key",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName: sshKeySecretName,
									Items: []corev1.KeyToPath{
										{Key: bundleKeyDataKey, Path: crcAgentSSHKeyFileName},
									},
								},
							},
						},
						{
							Name:         "identity",
							VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: identitySecretName}},
						},
					},
				},
			},
		},
	}
}
