# API Reference

## Packages
- [guestcluster.opdev.io/v1alpha1](#guestclusteropdeviov1alpha1)


## guestcluster.opdev.io/v1alpha1

Package v1alpha1 contains API Schema definitions for the guestcluster v1alpha1 API group.

### Resource Types
- [CRCBundle](#crcbundle)
- [CRCBundleList](#crcbundlelist)
- [ClusterInstance](#clusterinstance)
- [ClusterInstanceList](#clusterinstancelist)
- [ClusterLease](#clusterlease)
- [ClusterLeaseList](#clusterleaselist)
- [ClusterPool](#clusterpool)
- [ClusterPoolList](#clusterpoollist)



#### CRCBackingStatus



CRCBackingStatus tracks the KubeVirt VM backing a topology=crc instance.



_Appears in:_
- [ClusterInstanceStatus](#clusterinstancestatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `vmName` _string_ | VMName is the name of the KubeVirt VirtualMachine running the CRC/SNO bundle. |  |  |
| `dataVolumeName` _string_ | DataVolumeName is the CDI DataVolume providing the VM's root disk. |  |  |
| `sshEndpoint` _string_ | SSHEndpoint is host:port used by the crc-agent to reach the CRC VM for<br />post-boot fixups and kubeconfig extraction. |  |  |


#### CRCBundle



CRCBundle is the Schema for the crcbundles API. It is cluster-scoped and
version-keyed (see internal/resources.CRCBundleName) so that a version is
downloaded and extracted at most once and shared by every ClusterPool /
ClusterInstance across all namespaces that reference it.



_Appears in:_
- [CRCBundleList](#crcbundlelist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `guestcluster.opdev.io/v1alpha1` | | |
| `kind` _string_ | `CRCBundle` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  | Optional: \{\} <br /> |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  | Optional: \{\} <br /> |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[CRCBundleSpec](#crcbundlespec)_ |  |  |  |
| `status` _[CRCBundleStatus](#crcbundlestatus)_ |  |  |  |


#### CRCBundleList



CRCBundleList contains a list of CRCBundle.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `guestcluster.opdev.io/v1alpha1` | | |
| `kind` _string_ | `CRCBundleList` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  | Optional: \{\} <br /> |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  | Optional: \{\} <br /> |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[CRCBundle](#crcbundle) array_ |  |  |  |


#### CRCBundlePhase

_Underlying type:_ _string_

CRCBundlePhase represents where a CRCBundle is in its download/verify/extract
lifecycle.



_Appears in:_
- [CRCBundleStatus](#crcbundlestatus)

| Field | Description |
| --- | --- |
| `Pending` | CRCBundlePhasePending means no preparation work has started yet.<br /> |
| `Preparing` | CRCBundlePhasePreparing means the bundle-prep Job is downloading, verifying,<br />and extracting the bundle into the golden PVC.<br /> |
| `Ready` | CRCBundlePhaseReady means the golden PVC and derived SSH-key Secret are<br />populated and ClusterInstances may clone from this bundle.<br /> |
| `Failed` | CRCBundlePhaseFailed means the bundle-prep Job exhausted its retries.<br /> |


#### CRCBundleSpec



CRCBundleSpec defines the desired state of a cached, version-keyed CRC bundle.

A CRCBundle is the turnkey alternative to manually extracting a .crcbundle and
hosting crc.qcow2/id_ecdsa_crc yourself. The admin specifies only a version (and,
on the ClusterPool/ClusterInstance referencing it, a pull secret for runtime
injection). CRCBundleReconciler downloads the official bundle from the OpenShift
mirror, verifies its checksum, extracts it, and caches the resulting crc.qcow2
disk image in a golden PersistentVolumeClaim (plus the bundle's SSH private key
in a derived Secret). Any number of ClusterPools/ClusterInstances, in any
namespace, can clone from this bundle via CDI's cross-namespace PVC clone support.



_Appears in:_
- [CRCBundle](#crcbundle)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `version` _string_ | Version is the CRC/OpenShift Local bundle version to fetch, e.g. "4.16.0". |  | Required: \{\} <br /> |
| `arch` _string_ | Arch is the CPU architecture of the bundle to fetch. | amd64 | Enum: [amd64 arm64] <br />Optional: \{\} <br /> |
| `bundleURL` _string_ | BundleURL optionally overrides the deterministic official mirror URL<br />(https://mirror.openshift.com/pub/openshift-v4/clients/crc/bundles/openshift/<version>/crc_libvirt_<version>_<arch>.crcbundle),<br />which is derived from Version/Arch when this is left empty. |  | Optional: \{\} <br /> |
| `sha256URL` _string_ | SHA256URL optionally overrides the deterministic mirror checksum URL (the<br />sha256sum.txt published alongside the bundle) used to verify the download.<br />Derived from Version/Arch when left empty. |  | Optional: \{\} <br /> |
| `storageClassName` _string_ | StorageClassName is the StorageClass used for the golden PVC that caches the<br />extracted crc.qcow2 disk image for this version. For fast per-instance<br />provisioning it should support CSI clone or snapshot; if it does not,<br />ClusterInstances fall back to a full re-import per instance. |  | Optional: \{\} <br /> |
| `goldenVolumeSize` _string_ | GoldenVolumeSize is the size of the golden PVC that stores the extracted<br />crc.qcow2. Defaults to 35Gi (CRC bundle disks are typically ~31Gi). | 35Gi | Optional: \{\} <br /> |


#### CRCBundleStatus



CRCBundleStatus defines the observed state of a CRCBundle.



_Appears in:_
- [CRCBundle](#crcbundle)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `phase` _[CRCBundlePhase](#crcbundlephase)_ | Phase summarizes where this bundle is in its download/extract lifecycle. |  | Optional: \{\} <br /> |
| `qcow2PVCRef` _[LocalObjectReference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#localobjectreference-v1-core)_ | QCOW2PVCRef references the golden PersistentVolumeClaim holding the extracted<br />crc.qcow2 disk image, once Phase=Ready. ClusterInstance DataVolumes clone<br />from it via a cross-namespace CDI PVC source. |  | Optional: \{\} <br /> |
| `qcow2PVCNamespace` _string_ | QCOW2PVCNamespace is the namespace QCOW2PVCRef lives in (the operator's own<br />namespace, where the bundle-prep Job runs). |  | Optional: \{\} <br /> |
| `sshKeySecretRef` _[LocalObjectReference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#localobjectreference-v1-core)_ | SSHKeySecretRef references the Secret (in QCOW2PVCNamespace) holding the<br />bundle's id_ecdsa_crc SSH private key under data key "id_ecdsa", derived<br />automatically during extraction. |  | Optional: \{\} <br /> |
| `ocpVersion` _string_ | OCPVersion is the OpenShift version reported by the bundle's own<br />crc-bundle-info.json metadata, recorded here once extraction succeeds. |  | Optional: \{\} <br /> |
| `sha256` _string_ | SHA256 is the verified checksum of the downloaded .crcbundle artifact. |  | Optional: \{\} <br /> |
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#condition-v1-meta) array_ | Conditions represent the latest available observations of the bundle's state. |  | Optional: \{\} <br /> |


#### ClusterInstance



ClusterInstance is the Schema for the clusterinstances API. It represents a single
concrete guest OpenShift cluster (CRC VM or HyperShift hosted cluster) managed by
this operator.



_Appears in:_
- [ClusterInstanceList](#clusterinstancelist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `guestcluster.opdev.io/v1alpha1` | | |
| `kind` _string_ | `ClusterInstance` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  | Optional: \{\} <br /> |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  | Optional: \{\} <br /> |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[ClusterInstanceSpec](#clusterinstancespec)_ |  |  |  |
| `status` _[ClusterInstanceStatus](#clusterinstancestatus)_ |  |  |  |


#### ClusterInstanceList



ClusterInstanceList contains a list of ClusterInstance.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `guestcluster.opdev.io/v1alpha1` | | |
| `kind` _string_ | `ClusterInstanceList` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  | Optional: \{\} <br /> |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  | Optional: \{\} <br /> |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[ClusterInstance](#clusterinstance) array_ |  |  |  |


#### ClusterInstancePhase

_Underlying type:_ _string_

ClusterInstancePhase is the lifecycle phase of a ClusterInstance.

This phase deliberately has no "Leased" value. Whether a ClusterLease
claims an instance is not part of the instance's own lifecycle. It is a
fact about demand, derived from ClusterLease.Status.InstanceRef, the
single source of truth for the binding (see clusterlease_types.go). This
design mirrors a Kubernetes Node, which has no "occupied" phase; a Node's
use is derived by listing Pods with a matching Spec.NodeName, never stored
on the Node itself. An instance's own lifecycle has exactly four states:
it is being created (Provisioning), it is usable (Ready), it failed
(Failed), or it is being torn down (Terminating), regardless of whether a
lease currently claims it. See Status.LeaseRef for the read-only, derived
projection of which lease currently claims a Ready instance, if any.

_Validation:_
- Enum: [Provisioning Ready Failed Terminating]

_Appears in:_
- [ClusterInstanceStatus](#clusterinstancestatus)

| Field | Description |
| --- | --- |
| `Provisioning` | PhaseProvisioning means the backing VM/HostedCluster is being created and has not<br />yet reported a usable kubeconfig.<br /> |
| `Ready` | PhaseReady means the instance has a valid kubeconfig and is usable. It may or may<br />not currently be claimed by a ClusterLease. See Status.LeaseRef.<br /> |
| `Failed` | PhaseFailed means provisioning failed and the instance requires operator/human<br />intervention or deletion.<br /> |
| `Terminating` | PhaseTerminating means the instance and its backing resources are being torn down.<br /> |


#### ClusterInstanceSpec



ClusterInstanceSpec defines the desired state of ClusterInstance.



_Appears in:_
- [ClusterInstance](#clusterinstance)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `type` _[ClusterTopology](#clustertopology)_ | Type is the topology of this guest cluster. |  | Enum: [crc hcp] <br />Required: \{\} <br /> |
| `poolRef` _[LocalObjectReference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#localobjectreference-v1-core)_ | PoolRef names the owning ClusterPool in the same namespace. Empty for<br />standalone instances created outside of pool management. |  | Optional: \{\} <br /> |
| `template` _[ClusterTemplate](#clustertemplate)_ | Template is a (possibly pool-inherited) copy of the provisioning parameters used<br />to create this instance. |  | Required: \{\} <br /> |


#### ClusterInstanceStatus



ClusterInstanceStatus defines the observed state of ClusterInstance.



_Appears in:_
- [ClusterInstance](#clusterinstance)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `phase` _[ClusterInstancePhase](#clusterinstancephase)_ | Phase is the current lifecycle phase. |  | Enum: [Provisioning Ready Failed Terminating] <br /> |
| `ocpVersion` _string_ | OCPVersion is the OpenShift version reported by the running guest<br />cluster (as opposed to Spec.Template.OCPVersion, which is the requested version).<br />A mismatch between requested and observed is surfaced via the VersionMismatch<br />condition and must be treated by CI as a hard fail unless explicitly waived. |  |  |
| `topology` _[ClusterTopology](#clustertopology)_ | Topology echoes Spec.Type once the instance is Ready, for convenient consumption<br />as an explicit CI output alongside OCPVersion. |  | Enum: [crc hcp] <br /> |
| `apiEndpoint` _string_ | APIEndpoint is the guest cluster's externally reachable API server URL. |  |  |
| `kubeconfigSecretRef` _[LocalObjectReference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#localobjectreference-v1-core)_ | KubeconfigSecretRef names the Secret (in this ClusterInstance's namespace)<br />containing the guest cluster's admin kubeconfig under key "kubeconfig". |  |  |
| `leaseRef` _[LocalObjectReference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#localobjectreference-v1-core)_ | LeaseRef names the ClusterLease currently claiming this instance, if any.<br />ClusterInstanceReconciler maintains this READ-ONLY, DERIVED projection for<br />observability only (e.g. the "Lease" column on `kubectl get clusterinstance`).<br />This field is never authoritative, and nothing, including the ClusterPool and<br />ClusterInstance controllers themselves, makes scheduling/lifecycle decisions<br />based on it. The single source of truth for the binding is<br />ClusterLease.Status.InstanceRef. ClusterInstanceReconciler keeps this field in<br />sync with that value by watching ClusterLeases, so it may lag by one reconcile. |  |  |
| `crc` _[CRCBackingStatus](#crcbackingstatus)_ | CRC holds backing-object references for topology=crc instances. |  | Optional: \{\} <br /> |
| `hyperShift` _[HyperShiftBackingStatus](#hypershiftbackingstatus)_ | HyperShift holds backing-object references for topology=hcp instances. |  | Optional: \{\} <br /> |
| `observedGeneration` _integer_ | ObservedGeneration is the generation last reconciled. |  |  |
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#condition-v1-meta) array_ | Conditions represent the latest available observations of the instance's state,<br />e.g. type=Ready, type=VersionMismatch. |  | Optional: \{\} <br /> |


#### ClusterLease



ClusterLease is the Schema for the clusterleases API. CI creates a ClusterLease to
acquire an available guest cluster of a given topology from a ClusterPool. Deleting
the ClusterLease releases the instance back to the pool for recycling.



_Appears in:_
- [ClusterLeaseList](#clusterleaselist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `guestcluster.opdev.io/v1alpha1` | | |
| `kind` _string_ | `ClusterLease` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  | Optional: \{\} <br /> |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  | Optional: \{\} <br /> |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[ClusterLeaseSpec](#clusterleasespec)_ |  |  |  |
| `status` _[ClusterLeaseStatus](#clusterleasestatus)_ |  |  |  |


#### ClusterLeaseList



ClusterLeaseList contains a list of ClusterLease.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `guestcluster.opdev.io/v1alpha1` | | |
| `kind` _string_ | `ClusterLeaseList` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  | Optional: \{\} <br /> |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  | Optional: \{\} <br /> |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[ClusterLease](#clusterlease) array_ |  |  |  |


#### ClusterLeasePhase

_Underlying type:_ _string_

ClusterLeasePhase is the lifecycle phase of a ClusterLease claim.

_Validation:_
- Enum: [Pending Bound Releasing Released Failed]

_Appears in:_
- [ClusterLeaseStatus](#clusterleasestatus)

| Field | Description |
| --- | --- |
| `Pending` | PhaseLeasePending means no matching Ready ClusterInstance has been bound yet.<br />The ClusterLease controller is either waiting for one to free up or has<br />triggered on-demand provisioning (subject to pool MaxSize).<br /> |
| `Bound` | PhaseLeaseBound means a ClusterInstance is exclusively bound to this lease and<br />its kubeconfig Secret is available.<br /> |
| `Releasing` | PhaseLeaseReleasing means the lease is being torn down and its instance is being<br />recycled.<br /> |
| `Released` | PhaseLeaseReleased is a terminal phase set just before the ClusterLease object<br />itself is deleted by the requester; kept for observability in edge cases.<br /> |
| `Failed` | PhaseLeaseFailed means binding or provisioning failed (e.g. pool at MaxSize with<br />no available instance, or provisioning error propagated from ClusterInstance).<br /> |


#### ClusterLeaseSpec



ClusterLeaseSpec defines the desired state of ClusterLease.



_Appears in:_
- [ClusterLease](#clusterlease)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `poolRef` _[LocalObjectReference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#localobjectreference-v1-core)_ | PoolRef names the ClusterPool to lease from, in this namespace. The<br />referenced ClusterPool's Spec.Type determines the requested guest<br />cluster topology (echoed back on Status.Topology once bound). This<br />field does not duplicate the topology, because a ClusterPool name is<br />unique within a namespace and already unambiguously implies its type. |  | Required: \{\} <br /> |
| `ttl` _[Duration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#duration-v1-meta)_ | TTL bounds how long the lease may remain Bound before the controller forcibly<br />releases and recycles the underlying instance, protecting the pool from CI jobs<br />that fail to release. A value of 0 (or omitted) means no TTL enforcement. |  | Optional: \{\} <br /> |
| `requestedBy` _string_ | RequestedBy is a free-form identifier of the CI job/run requesting this lease,<br />recorded for audit purposes only. |  | Optional: \{\} <br /> |


#### ClusterLeaseStatus



ClusterLeaseStatus defines the observed state of ClusterLease.



_Appears in:_
- [ClusterLease](#clusterlease)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `phase` _[ClusterLeasePhase](#clusterleasephase)_ | Phase is the current lifecycle phase of the lease. |  | Enum: [Pending Bound Releasing Released Failed] <br /> |
| `instanceRef` _[LocalObjectReference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#localobjectreference-v1-core)_ | InstanceRef names the ClusterInstance claimed by this lease, once Phase=Bound.<br />This field is THE SINGLE SOURCE OF TRUTH for the lease<->instance binding<br />(analogous to a Kubernetes Pod's Spec.NodeName). ClusterLeaseReconciler writes<br />it exactly once, atomically with Phase transitioning to Bound, and never<br />duplicates it onto the ClusterInstance side. An instance is considered<br />claimed/in-use the moment a non-terminal (Pending or Bound) ClusterLease's<br />InstanceRef names it. The instance is "assumed" immediately, even before it<br />necessarily exists yet in some accounting paths. This design ensures that<br />ClusterPoolReconciler's supply accounting and ClusterInstance's derived<br />LeaseRef projection can never observe a state where the binding is ambiguous<br />or split across two independently-written records. (Formerly named<br />BoundInstanceRef.) |  |  |
| `kubeconfigSecretRef` _[LocalObjectReference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#localobjectreference-v1-core)_ | KubeconfigSecretRef names the Secret (in this ClusterLease's namespace) containing<br />the bound guest cluster's admin kubeconfig under key "kubeconfig".<br />ClusterLeaseReconciler copies/mirrors this Secret from the bound ClusterInstance's<br />kubeconfig Secret, so that CI can consume a single, stable location regardless of<br />which instance backs the lease. |  |  |
| `ocpVersion` _string_ | OCPVersion is the OpenShift version of the bound guest cluster. This field is an<br />explicit CI output alongside Topology, expected to be recorded next to the<br />artifact OCP line. |  |  |
| `topology` _[ClusterTopology](#clustertopology)_ | Topology echoes the bound instance's topology (crc \| hcp). This field is an<br />explicit CI output. A mismatch between Spec.Type and this value should never<br />occur, and indicates a controller bug if observed. |  | Enum: [crc hcp] <br /> |
| `apiEndpoint` _string_ | APIEndpoint is the bound guest cluster's API server URL, mirrored for convenience. |  |  |
| `boundTime` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#time-v1-meta)_ | BoundTime is when this lease was bound to an instance. |  |  |
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#condition-v1-meta) array_ | Conditions represent the latest available observations of the lease's state. |  | Optional: \{\} <br /> |


#### ClusterPool



ClusterPool is the Schema for the clusterpools API. It declares a budgeted pool of
guest OpenShift clusters (CRC or HyperShift) that CI jobs can lease from via
ClusterLease objects.



_Appears in:_
- [ClusterPoolList](#clusterpoollist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `guestcluster.opdev.io/v1alpha1` | | |
| `kind` _string_ | `ClusterPool` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  | Optional: \{\} <br /> |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  | Optional: \{\} <br /> |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[ClusterPoolSpec](#clusterpoolspec)_ |  |  |  |
| `status` _[ClusterPoolStatus](#clusterpoolstatus)_ |  |  |  |


#### ClusterPoolList



ClusterPoolList contains a list of ClusterPool.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `guestcluster.opdev.io/v1alpha1` | | |
| `kind` _string_ | `ClusterPoolList` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  | Optional: \{\} <br /> |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  | Optional: \{\} <br /> |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[ClusterPool](#clusterpool) array_ |  |  |  |


#### ClusterPoolSpec



ClusterPoolSpec defines the desired state of ClusterPool.



_Appears in:_
- [ClusterPool](#clusterpool)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `type` _[ClusterTopology](#clustertopology)_ | Type is the topology of guest clusters this pool manages. |  | Enum: [crc hcp] <br />Required: \{\} <br /> |
| `maxSize` _integer_ | MaxSize is the hard budget cap: the pool will never have more than this many<br />ClusterInstances (Ready+Leased+Provisioning) at once. Enforced by the<br />ClusterPool controller and used by the ClusterLease controller to decide<br />whether a new instance may be provisioned on-demand. |  | Minimum: 1 <br />Required: \{\} <br /> |
| `minSize` _integer_ | MinSize is the minimum number of ClusterInstances (any non-terminal phase,<br />Provisioning, Ready, or Leased) the pool controller keeps in existence at all<br />times, independent of lease demand. Unlike WarmSpares, this is a stable,<br />total-count floor. An instance transitioning Ready->Leased (or back) does not<br />change how many instances count against it, so top-up/scale-down against<br />MinSize cannot race with ClusterLease binding. 0 (the default) means the pool<br />may shrink to zero instances when there is no demand and WarmSpares is also 0<br />(pure on-demand provisioning). Subject to MaxSize. | 0 | Optional: \{\} <br /> |
| `warmSpares` _integer_ | WarmSpares is the number of Ready, unleased ClusterInstances the pool<br />controller tries to keep provisioned ahead of demand, so that a ClusterLease<br />can bind instantly instead of waiting for a full provision. This floor is<br />measured against spare (available) capacity, so it rises with load. Under N<br />active leases the pool targets roughly N+WarmSpares total instances. Subject<br />to MaxSize. (Formerly named MinAvailable.) | 0 | Optional: \{\} <br /> |
| `template` _[ClusterTemplate](#clustertemplate)_ | Template describes how to provision new ClusterInstances for this pool. |  | Required: \{\} <br /> |


#### ClusterPoolStatus



ClusterPoolStatus defines the observed state of ClusterPool.



_Appears in:_
- [ClusterPool](#clusterpool)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `totalInstances` _integer_ | TotalInstances is the current count of ClusterInstances owned by this pool<br />(any phase except Failed/deleted). |  |  |
| `availableInstances` _integer_ | AvailableInstances is the count of Ready, unleased ClusterInstances. |  |  |
| `leasedInstances` _integer_ | LeasedInstances is the count of ClusterInstances currently bound to a ClusterLease. |  |  |
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#condition-v1-meta) array_ | Conditions represent the latest available observations of the pool's state. |  | Optional: \{\} <br /> |


#### ClusterTemplate



ClusterTemplate describes how new ClusterInstances for a pool should be provisioned.



_Appears in:_
- [ClusterInstanceSpec](#clusterinstancespec)
- [ClusterPoolSpec](#clusterpoolspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `crcVersion` _string_ | CRCVersion, when set (topology=crc only), is the turnkey path. The admin<br />specifies only a CRC/OpenShift Local version (e.g. "4.16.0"), and the operator<br />downloads, verifies, and extracts the official bundle for that version via a<br />cluster-scoped CRCBundle. The operator auto-creates this CRCBundle and reuses it<br />across pools/instances (see api/v1alpha1/crcbundle_types.go), then clones its<br />cached crc.qcow2 and SSH key for this instance. When set, ReleaseImage and<br />BundleSSHKeyRef below are ignored (they are derived from the CRCBundle instead).<br />Ignored for topology=hcp. |  | Optional: \{\} <br /> |
| `crcArch` _string_ | CRCArch is the CPU architecture of the CRC bundle to fetch when CRCVersion is<br />set. Defaults to "amd64" if left empty. Ignored when CRCVersion is unset, and<br />for topology=hcp. |  | Enum: [amd64 arm64] <br />Optional: \{\} <br /> |
| `releaseImage` _string_ | ReleaseImage is the OCP release payload image used to provision the guest<br />cluster. For topology=crc this is a FALLBACK to the turnkey CRCVersion path<br />above. When CRCVersion is unset, this MUST be an HTTP(S) URL to the<br />*extracted* crc.qcow2 disk image from an official CRC/OpenShift Local<br />.crcbundle (NOT the .crcbundle tar itself). For example, extract via<br />`curl <bundle-url> \| tar --zstd -xf -` and host the resulting crc.qcow2 at a<br />URL reachable by CDI's HTTP DataVolume importer. For topology=hcp this is<br />always the HyperShift/OCP release image pullspec (required, CRCVersion does<br />not apply). |  | Optional: \{\} <br /> |
| `ocpVersion` _string_ | OCPVersion is the expected/recorded OpenShift version for clusters provisioned from<br />this template (e.g. "4.16.10"). It is echoed back on ClusterInstance.status.ocpVersion<br />once the guest cluster reports its own version, and is used to detect drift. |  | Required: \{\} <br /> |
| `nodePoolReplicas` _integer_ | NodePoolReplicas is the number of worker replicas for topology=hcp. Ignored<br />for topology=crc. Defaults to 1 when unset. |  | Minimum: 1 <br />Optional: \{\} <br /> |
| `controllerAvailabilityPolicy` _[ControllerAvailabilityPolicy](#controlleravailabilitypolicy)_ | ControllerAvailabilityPolicy sets the HostedCluster control-plane<br />availability for topology=hcp: SingleReplica or HighlyAvailable. Defaults<br />to SingleReplica, since HighlyAvailable's 3-way etcd/kube-apiserver spread<br />requires anti-affinity across that many schedulable, untainted nodes on<br />the management cluster, which small/dev management clusters (e.g.<br />CRC-hosted) often don't have. HighlyAvailable is therefore opt-in.<br />This field maps directly to HostedCluster.spec.controllerAvailabilityPolicy,<br />which is immutable once the HostedCluster is created. Changing this field<br />on an existing ClusterTemplate has no effect on already-provisioned<br />instances. Ignored for topology=crc. | SingleReplica | Enum: [SingleReplica HighlyAvailable] <br />Optional: \{\} <br /> |
| `memory` _string_ | Memory is the amount of memory allocated per VM (CRC VM, or each HyperShift KubeVirt<br />worker VM). E.g. "16Gi" for CRC, "6Gi" for a HyperShift worker. |  | Required: \{\} <br /> |
| `cores` _integer_ | Cores is the number of vCPUs allocated per VM. |  | Minimum: 1 <br />Required: \{\} <br /> |
| `rootVolumeSize` _string_ | RootVolumeSize is the size of the root disk (e.g. "35Gi"). |  | Optional: \{\} <br /> |
| `pullSecretRef` _[LocalObjectReference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#localobjectreference-v1-core)_ | PullSecretRef references a Secret (in the operator's namespace) containing the<br />pull-secret used to pull the release payload / CRC bundle images. This field is<br />optional. When unset, the operator defaults to the management cluster's own<br />global pull secret (the "pull-secret" Secret in the "openshift-config"<br />namespace), which is present on every OpenShift cluster. Set this field<br />explicitly to use a different/narrower credential (e.g. one scoped to a<br />disconnected mirror). |  | Optional: \{\} <br /> |
| `idmsRef` _[LocalObjectReference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#localobjectreference-v1-core)_ | IDMSRef optionally references an ImageDigestMirrorSet-shaped ConfigMap applied at<br />provision time for disconnected/mirrored registries. Interpreted per-topology:<br />for hcp it seeds HostedCluster.spec.imageContentSources; for crc it is applied<br />as an ImageDigestMirrorSet inside the guest cluster post-boot. |  | Optional: \{\} <br /> |
| `bundleSSHKeyRef` _[LocalObjectReference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#localobjectreference-v1-core)_ | BundleSSHKeyRef references a Secret (in the operator's namespace) containing the<br />CRC bundle's SSH private key (the "id_ecdsa_crc" file shipped inside an official<br />.crcbundle, used to reach the booted VM as user "core"), under a data key named<br />"id_ecdsa" or "ssh-privatekey". This field is a FALLBACK. When CRCVersion is set,<br />the operator derives the SSH key automatically from the referenced CRCBundle<br />instead, and this field is ignored. When CRCVersion is unset, this field is<br />required for topology=crc: the crc-agent Job uses this key to SSH into the<br />freshly booted CRC VM and run the post-boot fixups natively (start kubelet,<br />approve kubelet CSRs, inject the real pull secret, set credentials, rewrite the<br />kubeconfig server to the externally-routable API Route hostname the<br />ClusterInstance controller provisions). Ignored for topology=hcp. |  | Optional: \{\} <br /> |
| `hcpWorkerSSHKeyRef` _[LocalObjectReference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#localobjectreference-v1-core)_ | HCPWorkerSSHKeyRef optionally references a Secret (in the operator's namespace)<br />containing an SSH public key, under a data key named "id_rsa.pub", to inject as<br />an authorized key for the "core" user on every hcp NodePool worker (via<br />HostedCluster.spec.sshKey, see HyperShift's own ignition machine-config<br />generation). This field is only a debugging convenience (e.g. to inspect a<br />worker that is stuck before ever registering as a Node) and has no effect on<br />cluster function. Most deployments should leave it unset. Ignored for<br />topology=crc (see BundleSSHKeyRef for that path's own, unrelated SSH mechanism). |  | Optional: \{\} <br /> |
| `vmNodeSelector` _object (keys:string, values:string)_ | VMNodeSelector constrains which hypervisor node(s) the guest VM(s) are scheduled to. |  | Optional: \{\} <br /> |
| `storageClassName` _string_ | StorageClassName is the StorageClass used for VM root/data volumes. |  | Optional: \{\} <br /> |


#### ClusterTopology

_Underlying type:_ _string_

ClusterTopology identifies the shape of the provisioned guest cluster.

_Validation:_
- Enum: [crc hcp]

_Appears in:_
- [ClusterInstanceSpec](#clusterinstancespec)
- [ClusterInstanceStatus](#clusterinstancestatus)
- [ClusterLeaseStatus](#clusterleasestatus)
- [ClusterPoolSpec](#clusterpoolspec)

| Field | Description |
| --- | --- |
| `crc` | TopologyCRC is a single-node OpenShift cluster (CodeReady Containers / OpenShift Local)<br />running as a nested KubeVirt VirtualMachine on the hypervisor cluster.<br /> |
| `hcp` | TopologyHCP is a HyperShift-hosted cluster. Worker NodePool replica count is<br />controlled by ClusterTemplate.NodePoolReplicas (default 1) and control-plane<br />availability by ClusterTemplate.ControllerAvailabilityPolicy (default<br />SingleReplica). Both are independent, non-topology-encoded choices.<br /> |


#### ControllerAvailabilityPolicy

_Underlying type:_ _string_

ControllerAvailabilityPolicy selects the HostedCluster control-plane
availability for topology=hcp. This type is locally defined (rather than
importing HyperShift's own hyperv1beta1.AvailabilityPolicy into this API
package), so callers compare against these constants instead of an
upstream enum's string form. internal/resources maps this type to
hyperv1beta1.AvailabilityPolicy when building the HostedCluster.

_Validation:_
- Enum: [SingleReplica HighlyAvailable]

_Appears in:_
- [ClusterTemplate](#clustertemplate)

| Field | Description |
| --- | --- |
| `SingleReplica` | AvailabilityPolicySingleReplica runs a single replica of each<br />control-plane component. The default: see ClusterTemplate's<br />ControllerAvailabilityPolicy doc comment for why.<br /> |
| `HighlyAvailable` | AvailabilityPolicyHighlyAvailable spreads control-plane components<br />across multiple replicas for resilience, at the cost of requiring more<br />schedulable management-cluster capacity.<br /> |


#### HyperShiftBackingStatus



HyperShiftBackingStatus tracks the HostedCluster/NodePool backing a topology=hcp
instance.



_Appears in:_
- [ClusterInstanceStatus](#clusterinstancestatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `hostedClusterName` _string_ | HostedClusterName is the name of the hypershift.openshift.io/v1beta1 HostedCluster. |  |  |
| `hostedClusterNamespace` _string_ | HostedClusterNamespace is the namespace holding the HostedCluster (conventionally<br />"clusters"). |  |  |
| `nodePoolNames` _string array_ | NodePoolNames lists the NodePool(s) backing this instance's workers. |  |  |


