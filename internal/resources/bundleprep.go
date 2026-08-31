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
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
)

// BundlePrepServiceAccount is the fixed name of the ServiceAccount the
// bundle-prep Job runs as, in OperatorNamespace. It needs RBAC to create
// and update Secrets and ConfigMaps in that same namespace only; it never
// touches ClusterInstance namespaces. See config/rbac/bundle_prep_*.yaml.
const BundlePrepServiceAccount = "bundle-prep"

const (
	bundlePrepGoldenMountPath  = "/golden"
	bundlePrepScratchMountPath = "/work"

	// defaultScratchVolumeSize must comfortably hold, at peak, BOTH the
	// compressed .crcbundle download (typically well under 10Gi) AND the
	// extracted (decompressed) crc.qcow2 file (observed ~20-25Gi for a
	// 4.16.x bundle) at the same time. BuildBundlePrepJob's script extracts
	// crc.qcow2 into scratch space, not directly to golden, so that
	// `qemu-img convert` can read it as a real seekable file. The script
	// converts it to a raw disk.img in the golden PVC, and only then
	// removes the scratch copies. An earlier streamed tar-to-stdout
	// approach produced a golden disk.img that was still qcow2-formatted
	// internally (misnamed as raw), which KubeVirt/SeaBIOS could not boot
	// ("Boot failed: not a bootable disk"). The qcow2-to-raw conversion via
	// qemu-img is what makes the golden PVC a true raw disk image, matching
	// CDI's own importer convention.
	defaultScratchVolumeSize = "40Gi"

	// defaultGoldenVolumeSize mirrors CRCBundleSpec's own kubebuilder
	// default. BuildGoldenPVC uses it only as a Go-level fallback if a
	// CRCBundle somehow reaches the builder with an empty GoldenVolumeSize,
	// for example when a test constructs it directly rather than through
	// the API server's defaulting.
	defaultGoldenVolumeSize = "35Gi"
)

// ScratchPVCName is the deterministic name of the transient
// PersistentVolumeClaim (in OperatorNamespace) the bundle-prep Job uses as
// scratch space while downloading and verifying the compressed .crcbundle
// artifact. Unlike the golden PVC, callers can safely delete this PVC once
// the CRCBundle reaches Ready (see CRCBundleReconciler), reclaiming most of
// the transient storage footprint.
func ScratchPVCName(version, arch string) string {
	return CRCBundleName(version, arch) + "-scratch"
}

// BuildGoldenPVC constructs the PersistentVolumeClaim (in
// OperatorNamespace) that caches a CRCBundle's extracted crc.qcow2 disk
// image, converted to RAW format via `qemu-img convert`, at "/disk.img".
// This path matches CDI's own importer convention for filesystem-mode
// PVCs, so ClusterInstance DataVolumes can clone this PVC via a
// DataVolumeSourcePVC and have KubeVirt boot it exactly as if CDI had
// imported it directly. ReadWriteOnce is sufficient, because CDI's
// host-assisted clone mounts the source PVC once per clone, sequentially.
func BuildGoldenPVC(bundle *brokerv1alpha1.CRCBundle) *corev1.PersistentVolumeClaim {
	size := bundle.Spec.GoldenVolumeSize
	if size == "" {
		size = defaultGoldenVolumeSize
	}
	arch := bundleArch(bundle)

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GoldenPVCName(bundle.Spec.Version, arch),
			Namespace: OperatorNamespace(),
			Labels:    crcBundleLabels(bundle),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(size),
				},
			},
		},
	}
	if bundle.Spec.StorageClassName != "" {
		pvc.Spec.StorageClassName = &bundle.Spec.StorageClassName
	}
	return pvc
}

// BuildScratchPVC constructs the transient PersistentVolumeClaim (in
// OperatorNamespace) that the bundle-prep Job uses as scratch space. See
// ScratchPVCName's doc comment: callers can safely delete this PVC once the
// CRCBundle is Ready.
func BuildScratchPVC(bundle *brokerv1alpha1.CRCBundle) *corev1.PersistentVolumeClaim {
	arch := bundleArch(bundle)
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ScratchPVCName(bundle.Spec.Version, arch),
			Namespace: OperatorNamespace(),
			Labels:    crcBundleLabels(bundle),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(defaultScratchVolumeSize),
				},
			},
		},
	}
	if bundle.Spec.StorageClassName != "" {
		pvc.Spec.StorageClassName = &bundle.Spec.StorageClassName
	}
	return pvc
}

// bundlePrepScript is the shell script BuildBundlePrepJob's container runs.
// It downloads the official .crcbundle artifact and its published
// sha256sum.txt, and verifies the checksum. It then streams the crc.qcow2
// member directly out of the still-compressed archive into the golden PVC,
// without a full extraction pass (see defaultScratchVolumeSize's doc
// comment). It finally publishes the bundle's SSH private key and reported
// OpenShift version for the CRCBundleReconciler to read back into status.
//
// The script requires curl, tar (with zstd support), jq, and oc on PATH.
// This project reuses the same crc-agent image here, by design, rather
// than shipping a separate bundle-prep image, because the crc-agent image
// already needs curl/tar/jq/oc-adjacent tooling for its own native
// post-boot fixups.
const bundlePrepScript = `set -euo pipefail
echo "downloading bundle: ${BUNDLE_URL}"
curl -f -L --progress-bar "${BUNDLE_URL}" -o "${SCRATCH_DIR}/bundle.crcbundle"
echo "download complete: $(du -h "${SCRATCH_DIR}/bundle.crcbundle" | cut -f1)"
curl -fsSL "${SHA256_URL}" -o "${SCRATCH_DIR}/sha256sum.txt"

BUNDLE_FILENAME=$(basename "${BUNDLE_URL}")
EXPECTED=$(grep " ${BUNDLE_FILENAME}\$" "${SCRATCH_DIR}/sha256sum.txt" | awk '{print $1}')
if [ -z "${EXPECTED}" ]; then
  echo "no checksum entry found for ${BUNDLE_FILENAME} in sha256sum.txt" >&2
  exit 1
fi
echo "verifying checksum for ${BUNDLE_FILENAME} (this can take a while for large bundles)"
ACTUAL=$(sha256sum "${SCRATCH_DIR}/bundle.crcbundle" | awk '{print $1}')
if [ "${EXPECTED}" != "${ACTUAL}" ]; then
  echo "checksum mismatch for ${BUNDLE_FILENAME}: expected ${EXPECTED}, got ${ACTUAL}" >&2
  exit 1
fi
echo "checksum verified: ${ACTUAL}"

BUNDLE_DIR="${BUNDLE_FILENAME%.crcbundle}"
echo "extracting bundle contents to scratch space (this can take a while for large bundles)"
tar --zstd -xvf "${SCRATCH_DIR}/bundle.crcbundle" -C "${SCRATCH_DIR}" \
  "${BUNDLE_DIR}/crc.qcow2" "${BUNDLE_DIR}/id_ecdsa_crc" "${BUNDLE_DIR}/crc-bundle-info.json"
echo "extraction complete"
rm -f "${SCRATCH_DIR}/bundle.crcbundle"

echo "converting crc.qcow2 (qcow2 format) to ${GOLDEN_DIR}/disk.img (raw format) via qemu-img"
qemu-img convert -p -f qcow2 -O raw "${SCRATCH_DIR}/${BUNDLE_DIR}/crc.qcow2" "${GOLDEN_DIR}/disk.img"
rm -f "${SCRATCH_DIR}/${BUNDLE_DIR}/crc.qcow2"

OCP_VERSION=$(jq -r '.clusterInfo.openshiftVersion' "${SCRATCH_DIR}/${BUNDLE_DIR}/crc-bundle-info.json")
if [ -z "${OCP_VERSION}" ] || [ "${OCP_VERSION}" = "null" ]; then
  echo "could not determine openshiftVersion from bundle metadata" >&2
  exit 1
fi

echo "publishing SSH key secret ${SSH_SECRET_NAME} in ${NAMESPACE}"
cat > "${SCRATCH_DIR}/ssh-secret.yaml" <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: ${SSH_SECRET_NAME}
  namespace: ${NAMESPACE}
  ownerReferences:
  - apiVersion: guestcluster.opdev.io/v1alpha1
    kind: CRCBundle
    name: ${BUNDLE_NAME}
    uid: ${BUNDLE_UID}
    controller: true
type: Opaque
stringData:
  id_ecdsa: |
$(sed 's/^/    /' "${SCRATCH_DIR}/${BUNDLE_DIR}/id_ecdsa_crc")
EOF
oc apply -f "${SCRATCH_DIR}/ssh-secret.yaml"

echo "publishing metadata configmap ${METADATA_CONFIGMAP_NAME} in ${NAMESPACE}"
cat > "${SCRATCH_DIR}/metadata-cm.yaml" <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: ${METADATA_CONFIGMAP_NAME}
  namespace: ${NAMESPACE}
  ownerReferences:
  - apiVersion: guestcluster.opdev.io/v1alpha1
    kind: CRCBundle
    name: ${BUNDLE_NAME}
    uid: ${BUNDLE_UID}
    controller: true
data:
  ocpVersion: "${OCP_VERSION}"
  sha256: "${ACTUAL}"
EOF
oc apply -f "${SCRATCH_DIR}/metadata-cm.yaml"

echo "bundle prep complete: ocpVersion=${OCP_VERSION} sha256=${ACTUAL}"
`

// BuildBundlePrepJob constructs the run-to-completion Kubernetes Job (in
// OperatorNamespace) that downloads, verifies, and extracts a CRCBundle's
// official .crcbundle artifact. bundleURL and sha256URL are the
// caller-resolved (spec-override-or-derived) download URLs. image is the
// container image to run: the crc-agent image, reused by design (see
// CRCAgentImageEnvVar/DefaultCRCAgentImage).
func BuildBundlePrepJob(bundle *brokerv1alpha1.CRCBundle, bundleURL, sha256URL, image string) *batchv1.Job {
	arch := bundleArch(bundle)
	labels := crcBundleLabels(bundle)
	ns := OperatorNamespace()

	backoffLimit := int32(2)
	// This is a generous upper bound. A ~10-20GB download over a
	// slow/shared egress link, plus verification, can legitimately take a
	// while. This Job only ever runs once per version; results are cached
	// after that.
	activeDeadline := int64(2 * 60 * 60)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      BundlePrepJobName(bundle.Spec.Version, arch),
			Namespace: ns,
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
					ServiceAccountName: BundlePrepServiceAccount,
					Containers: []corev1.Container{
						{
							Name:    "bundle-prep",
							Image:   image,
							Command: []string{"/bin/sh", "-c"},
							Args:    []string{bundlePrepScript},
							Env: []corev1.EnvVar{
								{Name: "BUNDLE_URL", Value: bundleURL},
								{Name: "SHA256_URL", Value: sha256URL},
								{Name: "NAMESPACE", Value: ns},
								{Name: "BUNDLE_NAME", Value: bundle.Name},
								{Name: "BUNDLE_UID", Value: string(bundle.UID)},
								{Name: "SSH_SECRET_NAME", Value: BundleSSHKeySecretName(bundle.Spec.Version, arch)},
								{Name: "METADATA_CONFIGMAP_NAME", Value: BundleMetadataConfigMapName(bundle.Spec.Version, arch)},
								{Name: "GOLDEN_DIR", Value: bundlePrepGoldenMountPath},
								{Name: "SCRATCH_DIR", Value: bundlePrepScratchMountPath},
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "golden", MountPath: bundlePrepGoldenMountPath},
								{Name: "scratch", MountPath: bundlePrepScratchMountPath},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "golden",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: GoldenPVCName(bundle.Spec.Version, arch),
								},
							},
						},
						{
							Name: "scratch",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: ScratchPVCName(bundle.Spec.Version, arch),
								},
							},
						},
					},
				},
			},
		},
	}
}

// bundleArch returns bundle.Spec.Arch, defaulting to DefaultCRCArch when
// empty. This is a Go-level fallback that mirrors CRCBundleSpec's own
// kubebuilder default.
func bundleArch(bundle *brokerv1alpha1.CRCBundle) string {
	if bundle.Spec.Arch != "" {
		return bundle.Spec.Arch
	}
	return DefaultCRCArch
}

// crcBundleLabels returns the standard label set for every object created
// on behalf of a CRCBundle: its golden/scratch PVCs and its bundle-prep
// Job.
func crcBundleLabels(bundle *brokerv1alpha1.CRCBundle) map[string]string {
	return map[string]string{
		LabelManagedBy:        ManagerName,
		"opdev.io/crc-bundle": bundle.Name,
	}
}

// ResolveBundleURLs returns the download URLs for a CRCBundle, using
// spec-overridden URLs when present and falling back to the deterministic
// official mirror pattern otherwise.
func ResolveBundleURLs(bundle *brokerv1alpha1.CRCBundle) (bundleURL, sha256URL string) {
	arch := bundleArch(bundle)
	bundleURL = bundle.Spec.BundleURL
	if bundleURL == "" {
		bundleURL = BundleMirrorURL(bundle.Spec.Version, arch)
	}
	sha256URL = bundle.Spec.SHA256URL
	if sha256URL == "" {
		sha256URL = BundleMirrorSHA256URL(bundle.Spec.Version)
	}
	return bundleURL, sha256URL
}

// bundleNotReadyError is returned by callers (for example
// ClusterPoolReconciler, ClusterInstanceReconciler) when a referenced
// CRCBundle exists but has not yet reached Phase=Ready. This lets the
// caller distinguish "still preparing, requeue" from a hard error.
func BundleNotReadyMessage(bundle *brokerv1alpha1.CRCBundle) string {
	return fmt.Sprintf("CRCBundle %q for version %s/%s is not Ready yet (phase=%s)", bundle.Name, bundle.Spec.Version, bundleArch(bundle), bundle.Status.Phase)
}
