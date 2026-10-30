package controller

import (
	"testing"

	brokerv1alpha1 "github.com/opdev/guestcluster/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestNamespaceClusterInstanceResourceLimitsUseLowestHardLimit(t *testing.T) {
	cpuKey := corev1.ResourceName(clusterInstanceCPUQuota)
	memoryKey := corev1.ResourceName(clusterInstanceMemoryQuota)
	storageKey := corev1.ResourceName(clusterInstanceStorageQuota)
	quotas := &corev1.ResourceQuotaList{Items: []corev1.ResourceQuota{
		{ObjectMeta: metav1.ObjectMeta{Name: "first", Namespace: "tenant"}, Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			cpuKey: resource.MustParse("8"), memoryKey: resource.MustParse("32Gi"),
		}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "second", Namespace: "tenant"}, Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			cpuKey: resource.MustParse("4"), memoryKey: resource.MustParse("64Gi"), storageKey: resource.MustParse("100Gi"),
		}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "third", Namespace: "tenant"}, Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			corev1.ResourceRequestsCPU: resource.MustParse("1"), storageKey: resource.MustParse("80Gi"),
		}}},
	}}

	limits, err := namespaceClusterInstanceResourceLimits(quotas)
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[corev1.ResourceName]string{
		cpuKey: "4", memoryKey: "32Gi", storageKey: "80Gi",
	} {
		assertResourceListQuantity(t, limits, key, want)
	}
}

func TestNamespaceClusterInstanceResourceLimitsAllowNoLimitsAndRejectFractionalUnits(t *testing.T) {
	limits, err := namespaceClusterInstanceResourceLimits(&corev1.ResourceQuotaList{Items: []corev1.ResourceQuota{{
		Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{corev1.ResourceRequestsCPU: resource.MustParse("100")}},
	}}})
	if err != nil || len(limits) != 0 {
		t.Fatalf("built-in-only ResourceQuota produced Guestcluster limits %v, error %v", limits, err)
	}

	_, err = namespaceClusterInstanceResourceLimits(&corev1.ResourceQuotaList{Items: []corev1.ResourceQuota{{
		ObjectMeta: metav1.ObjectMeta{Name: "fractional", Namespace: "tenant"},
		Spec:       corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{corev1.ResourceName(clusterInstanceCPUQuota): resource.MustParse("500m")}},
	}}})
	if err == nil {
		t.Fatal("fractional extended CPU limit was accepted; expected whole cores")
	}
}

func TestClusterInstanceResourceUsageUsesTopologyDefaultsAndHCPReplicas(t *testing.T) {
	three := int32(3)
	cases := []struct {
		name     string
		instance *brokerv1alpha1.ClusterInstance
		want     map[corev1.ResourceName]string
	}{
		{
			name: "CRC defaults to one VM and 80Gi root disk",
			instance: &brokerv1alpha1.ClusterInstance{ObjectMeta: metav1.ObjectMeta{Name: "crc", Namespace: "tenant"}, Spec: brokerv1alpha1.ClusterInstanceSpec{
				Type: brokerv1alpha1.TopologyCRC, Template: brokerv1alpha1.ClusterTemplate{Cores: 4, Memory: "16Gi"},
			}},
			want: map[corev1.ResourceName]string{
				corev1.ResourceName(clusterInstanceCPUQuota): "4", corev1.ResourceName(clusterInstanceMemoryQuota): "16Gi", corev1.ResourceName(clusterInstanceStorageQuota): "80Gi",
			},
		},
		{
			name: "HCP multiplies worker requests and disk by replicas",
			instance: &brokerv1alpha1.ClusterInstance{ObjectMeta: metav1.ObjectMeta{Name: "hcp-three", Namespace: "tenant"}, Spec: brokerv1alpha1.ClusterInstanceSpec{
				Type: brokerv1alpha1.TopologyHCP, Template: brokerv1alpha1.ClusterTemplate{Cores: 2, Memory: "8Gi", NodePoolReplicas: &three},
			}},
			want: map[corev1.ResourceName]string{
				corev1.ResourceName(clusterInstanceCPUQuota): "6", corev1.ResourceName(clusterInstanceMemoryQuota): "24Gi", corev1.ResourceName(clusterInstanceStorageQuota): "96Gi",
			},
		},
		{
			name: "HCP defaults to one worker and 32Gi root disk",
			instance: &brokerv1alpha1.ClusterInstance{ObjectMeta: metav1.ObjectMeta{Name: "hcp-default", Namespace: "tenant"}, Spec: brokerv1alpha1.ClusterInstanceSpec{
				Type: brokerv1alpha1.TopologyHCP, Template: brokerv1alpha1.ClusterTemplate{Cores: 2, Memory: "8Gi"},
			}},
			want: map[corev1.ResourceName]string{
				corev1.ResourceName(clusterInstanceCPUQuota): "2", corev1.ResourceName(clusterInstanceMemoryQuota): "8Gi", corev1.ResourceName(clusterInstanceStorageQuota): "32Gi",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			usage, err := clusterInstanceResourceUsage(tc.instance)
			if err != nil {
				t.Fatal(err)
			}
			for key, want := range tc.want {
				assertResourceListQuantity(t, usage, key, want)
			}
		})
	}
}

func TestNamespaceClusterInstanceResourceViolationChecksAllLimitsAndNamespaceScope(t *testing.T) {
	poolInstance := brokerv1alpha1.ClusterInstance{ObjectMeta: metav1.ObjectMeta{Name: "pool-0", Namespace: "tenant"}, Spec: brokerv1alpha1.ClusterInstanceSpec{
		Type: brokerv1alpha1.TopologyCRC, PoolRef: corev1.LocalObjectReference{Name: "pool"}, Template: brokerv1alpha1.ClusterTemplate{Cores: 4, Memory: "16Gi"},
	}}
	unstartedStandalone := brokerv1alpha1.ClusterInstance{ObjectMeta: metav1.ObjectMeta{Name: "waiting", Namespace: "tenant"}, Spec: brokerv1alpha1.ClusterInstanceSpec{
		Type: brokerv1alpha1.TopologyCRC, Template: brokerv1alpha1.ClusterTemplate{Cores: 100, Memory: "100Gi"},
	}}
	otherNamespace := poolInstance.DeepCopy()
	otherNamespace.Name = "other-namespace-pool"
	otherNamespace.Namespace = "other"
	candidate := &brokerv1alpha1.ClusterInstance{ObjectMeta: metav1.ObjectMeta{Name: "standalone", Namespace: "tenant", UID: "candidate-uid"}, Spec: brokerv1alpha1.ClusterInstanceSpec{
		Type: brokerv1alpha1.TopologyHCP, PoolRef: corev1.LocalObjectReference{Name: "pool"}, Template: brokerv1alpha1.ClusterTemplate{Cores: 2, Memory: "8Gi"},
	}}
	instances := []brokerv1alpha1.ClusterInstance{poolInstance, unstartedStandalone, *candidate, *otherNamespace}
	limits := corev1.ResourceList{
		corev1.ResourceName(clusterInstanceCPUQuota):     resource.MustParse("6"),
		corev1.ResourceName(clusterInstanceMemoryQuota):  resource.MustParse("24Gi"),
		corev1.ResourceName(clusterInstanceStorageQuota): resource.MustParse("112Gi"),
	}

	violation, err := namespaceClusterInstanceResourceViolation(instances, limits, candidate)
	if err != nil || violation != nil {
		t.Fatalf("exact-fit candidate was rejected: violation=%v, error=%v", violation, err)
	}

	limits[corev1.ResourceName(clusterInstanceCPUQuota)] = resource.MustParse("5")
	violation, err = namespaceClusterInstanceResourceViolation(instances, limits, candidate)
	if err != nil || violation == nil || violation.dimension.quotaKey != corev1.ResourceName(clusterInstanceCPUQuota) {
		t.Fatalf("CPU over-limit candidate = (%v, %v), want CPU violation", violation, err)
	}

	limits[corev1.ResourceName(clusterInstanceCPUQuota)] = resource.MustParse("6")
	limits[corev1.ResourceName(clusterInstanceMemoryQuota)] = resource.MustParse("23Gi")
	violation, err = namespaceClusterInstanceResourceViolation(instances, limits, candidate)
	if err != nil || violation == nil || violation.dimension.quotaKey != corev1.ResourceName(clusterInstanceMemoryQuota) {
		t.Fatalf("memory over-limit candidate = (%v, %v), want memory violation", violation, err)
	}

	limits[corev1.ResourceName(clusterInstanceMemoryQuota)] = resource.MustParse("24Gi")
	limits[corev1.ResourceName(clusterInstanceStorageQuota)] = resource.MustParse("111Gi")
	violation, err = namespaceClusterInstanceResourceViolation(instances, limits, candidate)
	if err != nil || violation == nil || violation.dimension.quotaKey != corev1.ResourceName(clusterInstanceStorageQuota) {
		t.Fatalf("storage over-limit candidate = (%v, %v), want storage violation", violation, err)
	}
}

func TestNamespaceClusterInstanceResourceViolationCountsUnlistedCandidateOnce(t *testing.T) {
	candidate := &brokerv1alpha1.ClusterInstance{ObjectMeta: metav1.ObjectMeta{Name: "candidate", Namespace: "tenant"}, Spec: brokerv1alpha1.ClusterInstanceSpec{
		Type: brokerv1alpha1.TopologyCRC, PoolRef: corev1.LocalObjectReference{Name: "pool"}, Template: brokerv1alpha1.ClusterTemplate{Cores: 4, Memory: "16Gi", RootVolumeSize: "80Gi"},
	}}
	limits := corev1.ResourceList{
		corev1.ResourceName(clusterInstanceCPUQuota):     resource.MustParse("4"),
		corev1.ResourceName(clusterInstanceMemoryQuota):  resource.MustParse("16Gi"),
		corev1.ResourceName(clusterInstanceStorageQuota): resource.MustParse("80Gi"),
	}

	violation, err := namespaceClusterInstanceResourceViolation(nil, limits, candidate)
	if err != nil || violation != nil {
		t.Fatalf("unlisted candidate at exact limits was rejected: violation=%v, error=%v", violation, err)
	}

	limits[corev1.ResourceName(clusterInstanceCPUQuota)] = resource.MustParse("3")
	violation, err = namespaceClusterInstanceResourceViolation(nil, limits, candidate)
	if err != nil || violation == nil || violation.dimension.quotaKey != corev1.ResourceName(clusterInstanceCPUQuota) {
		t.Fatalf("unlisted candidate exceeding CPU limit = (%v, %v), want CPU violation", violation, err)
	}
}

func TestNamespaceClusterInstanceResourceViolationCountsSameNameWithDifferentUID(t *testing.T) {
	candidate := &brokerv1alpha1.ClusterInstance{ObjectMeta: metav1.ObjectMeta{Name: "recreated", Namespace: "tenant", UID: "new-uid"}, Spec: brokerv1alpha1.ClusterInstanceSpec{
		Type: brokerv1alpha1.TopologyCRC, PoolRef: corev1.LocalObjectReference{Name: "pool"}, Template: brokerv1alpha1.ClusterTemplate{Cores: 2, Memory: "8Gi", RootVolumeSize: "32Gi"},
	}}
	existing := candidate.DeepCopy()
	existing.UID = "old-uid"
	limits := corev1.ResourceList{
		corev1.ResourceName(clusterInstanceCPUQuota):     resource.MustParse("3"),
		corev1.ResourceName(clusterInstanceMemoryQuota):  resource.MustParse("16Gi"),
		corev1.ResourceName(clusterInstanceStorageQuota): resource.MustParse("64Gi"),
	}

	violation, err := namespaceClusterInstanceResourceViolation([]brokerv1alpha1.ClusterInstance{*existing}, limits, candidate)
	if err != nil || violation == nil || violation.dimension.quotaKey != corev1.ResourceName(clusterInstanceCPUQuota) {
		t.Fatalf("same-name object with a different UID = (%v, %v), want its allocation counted", violation, err)
	}
}

func TestNamespaceClusterInstanceResourceViolationFallsBackToNameWhenUIDMissing(t *testing.T) {
	cases := []struct {
		name         string
		existingUID  string
		candidateUID string
	}{
		{name: "existing UID missing", candidateUID: "candidate-uid"},
		{name: "candidate UID missing", existingUID: "existing-uid"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidate := &brokerv1alpha1.ClusterInstance{ObjectMeta: metav1.ObjectMeta{Name: "candidate", Namespace: "tenant", UID: types.UID(tc.candidateUID)}, Spec: brokerv1alpha1.ClusterInstanceSpec{
				Type: brokerv1alpha1.TopologyCRC, PoolRef: corev1.LocalObjectReference{Name: "pool"}, Template: brokerv1alpha1.ClusterTemplate{Cores: 4, Memory: "16Gi", RootVolumeSize: "80Gi"},
			}}
			existing := candidate.DeepCopy()
			existing.UID = types.UID(tc.existingUID)
			limits := corev1.ResourceList{
				corev1.ResourceName(clusterInstanceCPUQuota):     resource.MustParse("4"),
				corev1.ResourceName(clusterInstanceMemoryQuota):  resource.MustParse("16Gi"),
				corev1.ResourceName(clusterInstanceStorageQuota): resource.MustParse("80Gi"),
			}

			violation, err := namespaceClusterInstanceResourceViolation([]brokerv1alpha1.ClusterInstance{*existing}, limits, candidate)
			if err != nil || violation != nil {
				t.Fatalf("candidate with a missing UID was double-counted: violation=%v, error=%v", violation, err)
			}
		})
	}
}

func TestNamespaceClusterInstanceResourceViolationCountsReservationsForUnnamedPoolCandidate(t *testing.T) {
	existing := brokerv1alpha1.ClusterInstance{ObjectMeta: metav1.ObjectMeta{Name: "pool-instance", Namespace: "tenant"}, Spec: brokerv1alpha1.ClusterInstanceSpec{
		Type: brokerv1alpha1.TopologyCRC, PoolRef: corev1.LocalObjectReference{Name: "existing-pool"}, Template: brokerv1alpha1.ClusterTemplate{Cores: 4, Memory: "16Gi", RootVolumeSize: "80Gi"},
	}}
	candidate := &brokerv1alpha1.ClusterInstance{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant"}, Spec: brokerv1alpha1.ClusterInstanceSpec{
		Type: brokerv1alpha1.TopologyHCP, PoolRef: corev1.LocalObjectReference{Name: "new-pool"}, Template: brokerv1alpha1.ClusterTemplate{Cores: 2, Memory: "8Gi"},
	}}
	limits := corev1.ResourceList{
		corev1.ResourceName(clusterInstanceCPUQuota):     resource.MustParse("5"),
		corev1.ResourceName(clusterInstanceMemoryQuota):  resource.MustParse("24Gi"),
		corev1.ResourceName(clusterInstanceStorageQuota): resource.MustParse("112Gi"),
	}

	violation, err := namespaceClusterInstanceResourceViolation([]brokerv1alpha1.ClusterInstance{existing}, limits, candidate)
	if err != nil || violation == nil || violation.dimension.quotaKey != corev1.ResourceName(clusterInstanceCPUQuota) {
		t.Fatalf("unnamed pool candidate did not include existing reservations: violation=%v, error=%v", violation, err)
	}
}

func assertResourceListQuantity(t *testing.T, list corev1.ResourceList, key corev1.ResourceName, want string) {
	t.Helper()
	got, found := list[key]
	if !found {
		t.Fatalf("resource %q missing from %v", key, list)
	}
	if expected := resource.MustParse(want); got.Cmp(expected) != 0 {
		t.Fatalf("resource %q = %s, want %s", key, got.String(), expected.String())
	}
}
