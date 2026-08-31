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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	intstr "k8s.io/apimachinery/pkg/util/intstr"
	kubevirtv1 "kubevirt.io/api/core/v1"
	cdiv1beta1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	routev1 "github.com/openshift/api/route/v1"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
)

// The DataVolume builders fall back to defaultCRCRootVolumeSize when
// ClusterTemplate.RootVolumeSize is unset. CRC bundles need headroom for
// the SNO control plane, etcd, and workloads.
const defaultCRCRootVolumeSize = "80Gi"

// BuildCRCDataVolume constructs the CDI DataVolume that provides the root
// disk for a topology=crc ClusterInstance's VirtualMachine. The disk image
// is imported over HTTP from Spec.Template.ReleaseImage, which for the CRC
// path MUST be the URL of the crc.qcow2 disk image already EXTRACTED from
// an official .crcbundle archive (see ClusterTemplate.ReleaseImage godoc).
// This image is a fully pre-installed single-node OpenShift cluster,
// parked with kubelet disabled and an empty pull-secret, until the
// crc-agent's post-boot fixups (run natively, see cmd/crc-agent) bring it
// up. Extracting the bundle and hosting crc.qcow2 at a stable HTTP URL is
// expected to happen upstream of this operator, and is out of scope here.
func BuildCRCDataVolume(instance *brokerv1alpha1.ClusterInstance) *cdiv1beta1.DataVolume {
	tmpl := instance.Spec.Template

	size := tmpl.RootVolumeSize
	if size == "" {
		size = defaultCRCRootVolumeSize
	}

	storage := &cdiv1beta1.StorageSpec{
		Resources: corev1.VolumeResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse(size),
			},
		},
	}
	if tmpl.StorageClassName != "" {
		sc := tmpl.StorageClassName
		storage.StorageClassName = &sc
	}

	return &cdiv1beta1.DataVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DataVolumeName(instance.Name),
			Namespace: instance.Namespace,
			Labels:    CommonLabels(instance),
		},
		Spec: cdiv1beta1.DataVolumeSpec{
			Source: &cdiv1beta1.DataVolumeSource{
				HTTP: &cdiv1beta1.DataVolumeSourceHTTP{
					URL: tmpl.ReleaseImage,
				},
			},
			Storage: storage,
		},
	}
}

// BuildCRCDataVolumeFromBundle constructs the CDI DataVolume that provides
// the root disk for a topology=crc ClusterInstance's VirtualMachine,
// sourced by CLONING the golden PersistentVolumeClaim of a Ready CRCBundle
// (see ClusterTemplate.CRCVersion) rather than importing the disk image
// over HTTP. This is the turnkey path: the bundle-prep Job (see
// BuildBundlePrepJob) has already downloaded, verified, and extracted
// crc.qcow2 into the bundle's golden PVC exactly once. Every instance that
// references that version gets its own copy via CDI's cross-namespace PVC
// clone support, with no per-instance download. Callers must confirm
// bundle.Status.Phase == CRCBundlePhaseReady, and that
// QCOW2PVCRef/QCOW2PVCNamespace are populated, before calling this
// function.
func BuildCRCDataVolumeFromBundle(instance *brokerv1alpha1.ClusterInstance, bundle *brokerv1alpha1.CRCBundle) *cdiv1beta1.DataVolume {
	tmpl := instance.Spec.Template

	size := tmpl.RootVolumeSize
	if size == "" {
		size = defaultCRCRootVolumeSize
	}

	storage := &cdiv1beta1.StorageSpec{
		Resources: corev1.VolumeResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse(size),
			},
		},
	}
	if tmpl.StorageClassName != "" {
		sc := tmpl.StorageClassName
		storage.StorageClassName = &sc
	} else if bundle.Spec.StorageClassName != "" {
		sc := bundle.Spec.StorageClassName
		storage.StorageClassName = &sc
	}

	return &cdiv1beta1.DataVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DataVolumeName(instance.Name),
			Namespace: instance.Namespace,
			Labels:    CommonLabels(instance),
		},
		Spec: cdiv1beta1.DataVolumeSpec{
			Source: &cdiv1beta1.DataVolumeSource{
				PVC: &cdiv1beta1.DataVolumeSourcePVC{
					Namespace: bundle.Status.QCOW2PVCNamespace,
					Name:      bundle.Status.QCOW2PVCRef.Name,
				},
			},
			Storage: storage,
		},
	}
}

// BuildCRCVirtualMachine constructs the KubeVirt VirtualMachine running the
// CRC/SNO bundle, booting from the DataVolume named dvName (see
// BuildCRCDataVolume / DataVolumeName). The VM is created with
// RunStrategyAlways, so it boots immediately on creation. The crc-agent
// (cmd/crc-agent) drives in-guest crc start/stop/recycle operations, and
// flips RunStrategy to Halted/Always across a stop/start of the whole VM
// when needed, for example to reclaim hypervisor resources for a
// Ready-but-idle pool member.
func BuildCRCVirtualMachine(instance *brokerv1alpha1.ClusterInstance, dvName string) *kubevirtv1.VirtualMachine {
	tmpl := instance.Spec.Template
	runStrategy := kubevirtv1.RunStrategyAlways

	cpu := *resource.NewQuantity(int64(tmpl.Cores), resource.DecimalSI)

	labels := CommonLabels(instance)

	return &kubevirtv1.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      VMName(instance.Name),
			Namespace: instance.Namespace,
			Labels:    labels,
		},
		Spec: kubevirtv1.VirtualMachineSpec{
			RunStrategy: &runStrategy,
			Template: &kubevirtv1.VirtualMachineInstanceTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: kubevirtv1.VirtualMachineInstanceSpec{
					NodeSelector: tmpl.VMNodeSelector,
					Domain: kubevirtv1.DomainSpec{
						Resources: kubevirtv1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse(tmpl.Memory),
								corev1.ResourceCPU:    cpu,
							},
						},
						Devices: kubevirtv1.Devices{
							Disks: []kubevirtv1.Disk{
								{
									Name: "rootdisk",
									DiskDevice: kubevirtv1.DiskDevice{
										Disk: &kubevirtv1.DiskTarget{Bus: "virtio"},
									},
								},
							},
						},
					},
					Volumes: []kubevirtv1.Volume{
						{
							Name: "rootdisk",
							VolumeSource: kubevirtv1.VolumeSource{
								DataVolume: &kubevirtv1.DataVolumeSource{Name: dvName},
							},
						},
					},
				},
			},
		},
	}
}

// crcAPIServicePort is the guest OpenShift API server's fixed port.
const crcAPIServicePort = 6443

// vmNameLabel is the label KubeVirt itself applies to every virt-launcher
// pod, and copies onto the VMI, naming the owning VirtualMachine. Unlike
// pod names, this label stays stable across VM restarts. That stability
// makes it the correct Service selector for reaching the current
// virt-launcher pod for a given ClusterInstance across recycles.
const vmNameLabel = "vm.kubevirt.io/name"

// BuildCRCAPIService constructs the ClusterIP Service that exposes a
// topology=crc ClusterInstance's guest API server (port 6443) inside the
// management cluster, by selecting the VM's virt-launcher pod directly via
// KubeVirt's own vm.kubevirt.io/name label. No NodePort/LoadBalancer is
// needed, because only the in-cluster router, via BuildCRCAPIRoute, needs
// to reach it.
//
// This function exists because the VMI's own pod-network IP is not
// routable outside the management cluster; it lives in the cluster's
// internal pod CIDR. External access must therefore go through the
// management cluster's own ingress (Route), rather than a raw IP or
// hostname pointed directly at the VMI.
func BuildCRCAPIService(instance *brokerv1alpha1.ClusterInstance) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CRCAPIServiceName(instance.Name),
			Namespace: instance.Namespace,
			Labels:    CommonLabels(instance),
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{vmNameLabel: VMName(instance.Name)},
			Ports: []corev1.ServicePort{
				{
					Name:       "api",
					Protocol:   corev1.ProtocolTCP,
					Port:       crcAPIServicePort,
					TargetPort: intstr.FromInt32(crcAPIServicePort),
				},
			},
		},
	}
}

// BuildCRCAPIRoute constructs the passthrough Route that fronts
// BuildCRCAPIService with an externally-routable hostname, for example
// api-<instance>.apps.<mgmt-ingress-domain>. Passthrough termination is
// required, not edge or reencrypt: the guest Kubernetes API requires mTLS
// client-certificate authentication and SPDY/websocket upgrades (exec,
// logs, port-forward). Both only work if the router forwards the raw TLS
// stream to the guest API server untouched.
func BuildCRCAPIRoute(instance *brokerv1alpha1.ClusterInstance, host, serviceName string) *routev1.Route {
	weight := int32(100)
	return &routev1.Route{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CRCAPIRouteName(instance.Name),
			Namespace: instance.Namespace,
			Labels:    CommonLabels(instance),
		},
		Spec: routev1.RouteSpec{
			Host: host,
			To: routev1.RouteTargetReference{
				Kind:   "Service",
				Name:   serviceName,
				Weight: &weight,
			},
			Port: &routev1.RoutePort{
				TargetPort: intstr.FromString("api"),
			},
			TLS: &routev1.TLSConfig{
				Termination:                   routev1.TLSTerminationPassthrough,
				InsecureEdgeTerminationPolicy: routev1.InsecureEdgeTerminationPolicyNone,
			},
		},
	}
}
