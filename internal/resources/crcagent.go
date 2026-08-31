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
	"os"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
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

// CRCAgentServiceAccountEnvVar is the environment variable the manager
// Deployment sets (see config/manager/manager.yaml, populated at kustomize
// build time via a `replacements` entry that copies the ServiceAccount's
// own final metadata.name) to the actual name of the ServiceAccount the
// crc-agent Job runs as. This indirection exists because config/default
// applies a namePrefix to every RBAC object it includes, so the
// ServiceAccount's deployed name is not known until kustomize build time
// and must not be hardcoded twice (once in YAML, once in Go).
const CRCAgentServiceAccountEnvVar = "CRC_AGENT_SERVICE_ACCOUNT"

// DefaultCRCAgentServiceAccount is used when CRCAgentServiceAccountEnvVar is
// unset, matching config/rbac/crc_agent_service_account.yaml's unprefixed
// name (for example when running via `make run` outside a Pod).
const DefaultCRCAgentServiceAccount = "crc-agent"

// CRCAgentServiceAccount resolves the name of the ServiceAccount the
// crc-agent Job runs as. It must exist, with RBAC to get, create, and
// update Secrets, in every namespace where ClusterInstances are created.
// See config/rbac/crc_agent_*.yaml, which provisions it in the operator's
// own namespace. This is consistent with this project's single-namespace
// deployment model: ClusterTemplate.PullSecretRef/BundleSSHKeyRef are
// likewise documented as living "in the operator's namespace".
// CRCAgentServiceAccount prefers CRCAgentServiceAccountEnvVar, falling back
// to DefaultCRCAgentServiceAccount when unset.
func CRCAgentServiceAccount() string {
	if sa := os.Getenv(CRCAgentServiceAccountEnvVar); sa != "" {
		return sa
	}
	return DefaultCRCAgentServiceAccount
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
// precheck), or from a Ready CRCBundle's Status.SSHKeySecretRef (turnkey
// path, which always uses data key "id_ecdsa" per the bundle-prep script).
// image is the crc-agent container image to run (see CRCAgentImageEnvVar).
// apiHostname is the externally routable hostname for which the
// ClusterInstance controller already provisioned a passthrough Route (see
// BuildCRCAPIRoute); the crc-agent uses it to mint the guest API server's
// external-facing serving certificate and to rewrite the published
// kubeconfig's server URL. pullSecretName is the name (in
// instance.Namespace) of the Secret holding the pull-secret to inject into
// the guest cluster. The caller resolves it, either from the template's
// explicit PullSecretRef, or from a materialized copy of the management
// cluster's own default pull secret (see
// ClusterInstanceReconciler.resolvePullSecret).
func BuildCRCAgentJob(instance *brokerv1alpha1.ClusterInstance, vmIP, sshKeySecretName, bundleKeyDataKey, image, apiHostname, pullSecretName string) *batchv1.Job {
	labels := CommonLabels(instance)

	backoffLimit := int32(2)
	activeDeadline := int64(45 * 60) // 45 minutes: generous upper bound on a single post-boot fixup run.

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CRCAgentJobName(instance.Name),
			Namespace: instance.Namespace,
			Labels:    labels,
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
					ServiceAccountName: CRCAgentServiceAccount(),
					Containers: []corev1.Container{
						{
							Name:  "crc-agent",
							Image: image,
							Env: []corev1.EnvVar{
								{Name: "INSTANCE_NAME", Value: instance.Name},
								{Name: "INSTANCE_NAMESPACE", Value: instance.Namespace},
								{Name: "CRC_SSH_HOST", Value: vmIP},
								{Name: "CRC_SSH_KEY_PATH", Value: CRCAgentSSHKeyPath()},
								{Name: "PULL_SECRET_PATH", Value: CRCAgentPullSecretPath()},
								{Name: CRCAPIHostnameEnvVar, Value: apiHostname},
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "pull-secret", MountPath: crcAgentPullSecretMountPath, ReadOnly: true},
								{Name: "bundle-ssh-key", MountPath: crcAgentSSHKeyMountPath, ReadOnly: true},
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
					},
				},
			},
		},
	}
}
