package controller

import (
	"context"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubevirtv1 "kubevirt.io/api/core/v1"
	cdiv1beta1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
)

const (
	bootKeySourceNamespace = "operator"
	invalidBundleSource    = "invalid or incomplete"
	changedBundleSource    = "changed"
	repreparedSourceUID    = "reprepared"
)

func bootKeyFixture() (*brokerv1alpha1.ClusterInstance, *brokerv1alpha1.CRCBundle, *corev1.PersistentVolumeClaim, *corev1.Secret) {
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "crc-one", Namespace: "tenant-one", UID: types.UID("instance-one")},
		Spec: brokerv1alpha1.ClusterInstanceSpec{Type: brokerv1alpha1.TopologyCRC,
			Template: brokerv1alpha1.ClusterTemplate{CRCVersion: "4.16.0"}},
	}
	bundle := &brokerv1alpha1.CRCBundle{
		ObjectMeta: metav1.ObjectMeta{Name: resources.CRCBundleName("4.16.0", resources.DefaultCRCArch), UID: types.UID("bundle-one")},
		Spec:       brokerv1alpha1.CRCBundleSpec{StorageClassName: "golden-storage"},
		Status: brokerv1alpha1.CRCBundleStatus{
			Phase: brokerv1alpha1.CRCBundlePhaseReady, QCOW2PVCNamespace: bootKeySourceNamespace, SHA256: "bundle-checksum-one",
			QCOW2PVCRef:     &corev1.LocalObjectReference{Name: "golden"},
			SSHKeySecretRef: &corev1.LocalObjectReference{Name: "source-key"},
		},
	}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "golden", Namespace: bootKeySourceNamespace, UID: types.UID("disk-one")}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "source-key", Namespace: bootKeySourceNamespace, UID: types.UID("source-one")}, Data: map[string][]byte{crcBundleSSHKeyDataKey: []byte("private-key-one")}}
	return instance, bundle, pvc, secret
}

func bindAndCopyBootKey(t *testing.T, ctx context.Context, r *ClusterInstanceReconciler, instance *brokerv1alpha1.ClusterInstance) {
	t.Helper()
	dv := &cdiv1beta1.DataVolume{ObjectMeta: metav1.ObjectMeta{Name: resources.DataVolumeName(instance.Name), Namespace: instance.Namespace}}
	if _, pending, err := r.ensureCRCBootKey(ctx, instance, dv); err != nil || !pending {
		t.Fatalf("recording disk/key binding: pending=%t err=%v", pending, err)
	}
	if _, pending, err := r.ensureCRCBootKey(ctx, instance, dv); err != nil || pending {
		t.Fatalf("creating local boot key: pending=%t err=%v", pending, err)
	}
}

func TestCRCBootKeyIndependentCopiesAndCleanup(t *testing.T) {
	ctx := context.Background()
	one, bundle, pvc, source := bootKeyFixture()
	two := one.DeepCopy()
	two.Name, two.Namespace, two.UID = "crc-two", "tenant-two", types.UID("instance-two")
	c := newCRCRecoveryFakeClient(t, one, two, bundle, pvc, source)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	bindAndCopyBootKey(t, ctx, r, one)
	bindAndCopyBootKey(t, ctx, r, two)
	for _, instance := range []*brokerv1alpha1.ClusterInstance{one, two} {
		copy := &corev1.Secret{}
		key := types.NamespacedName{Name: resources.CRCBootKeySecretName(instance.Name), Namespace: instance.Namespace}
		if err := c.Get(ctx, key, copy); err != nil || !metav1.IsControlledBy(copy, instance) || string(copy.Data[crcBundleSSHKeyDataKey]) != "private-key-one" {
			t.Fatalf("copy %s: secret=%+v err=%v", key, copy, err)
		}
	}
	if pending, err := r.teardownCRCBacking(ctx, one); err != nil || pending {
		t.Fatalf("teardown: pending=%t err=%v", pending, err)
	}
	if err := c.Get(ctx, types.NamespacedName{Name: resources.CRCBootKeySecretName(one.Name), Namespace: one.Namespace}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("deleted instance copy still exists: %v", err)
	}
	for _, obj := range []client.Object{source, pvc, two} {
		if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
			t.Fatalf("shared source or other instance removed: %v", err)
		}
	}
	if err := c.Get(ctx, types.NamespacedName{Name: resources.CRCBootKeySecretName(two.Name), Namespace: two.Namespace}, &corev1.Secret{}); err != nil {
		t.Fatalf("other instance copy removed: %v", err)
	}
}

func TestCRCBootKeyInvalidSources(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		modify     func(*brokerv1alpha1.CRCBundle, *corev1.Secret)
		omitSecret bool
	}{
		{"missing namespace", invalidBundleSource, func(b *brokerv1alpha1.CRCBundle, _ *corev1.Secret) { b.Status.QCOW2PVCNamespace = "" }, false},
		{"missing reference", invalidBundleSource, func(b *brokerv1alpha1.CRCBundle, _ *corev1.Secret) { b.Status.SSHKeySecretRef = nil }, false},
		{"empty reference", invalidBundleSource, func(b *brokerv1alpha1.CRCBundle, _ *corev1.Secret) { b.Status.SSHKeySecretRef.Name = "" }, false},
		{"missing secret", "getting CRCBundle SSH key", nil, true},
		{"empty data", "empty or missing id_ecdsa", func(_ *brokerv1alpha1.CRCBundle, s *corev1.Secret) { s.Data = nil }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			instance, bundle, pvc, source := bootKeyFixture()
			if tc.modify != nil {
				tc.modify(bundle, source)
			}
			objects := []client.Object{instance, bundle, pvc}
			if !tc.omitSecret {
				objects = append(objects, source)
			}
			c := newCRCRecoveryFakeClient(t, objects...)
			r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
			if _, _, err := r.ensureCRCBootKey(ctx, instance, nil); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q error, got %v", tc.want, err)
			}
		})
	}
}

func TestCRCBootKeyRecoveryAndSourceDrift(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		change     func(*brokerv1alpha1.CRCBundle, *corev1.PersistentVolumeClaim, *corev1.Secret)
	}{
		{"same source", "", nil},
		{"new key data", changedBundleSource, func(_ *brokerv1alpha1.CRCBundle, _ *corev1.PersistentVolumeClaim, s *corev1.Secret) {
			s.Data[crcBundleSSHKeyDataKey] = []byte("other")
		}},
		{"new source UID", changedBundleSource, func(_ *brokerv1alpha1.CRCBundle, _ *corev1.PersistentVolumeClaim, s *corev1.Secret) {
			s.UID = repreparedSourceUID
		}},
		{"new disk UID", changedBundleSource, func(_ *brokerv1alpha1.CRCBundle, p *corev1.PersistentVolumeClaim, _ *corev1.Secret) {
			p.UID = repreparedSourceUID
		}},
		{"new bundle UID", changedBundleSource, func(b *brokerv1alpha1.CRCBundle, _ *corev1.PersistentVolumeClaim, _ *corev1.Secret) {
			b.UID = repreparedSourceUID
		}},
		{"new bundle checksum", changedBundleSource, func(b *brokerv1alpha1.CRCBundle, _ *corev1.PersistentVolumeClaim, _ *corev1.Secret) {
			b.Status.SHA256 = "new-bundle-checksum"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			instance, bundle, pvc, source := bootKeyFixture()
			c := newCRCRecoveryFakeClient(t, instance, bundle, pvc, source)
			r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
			bindAndCopyBootKey(t, ctx, r, instance)
			key := types.NamespacedName{Name: resources.CRCBootKeySecretName(instance.Name), Namespace: instance.Namespace}
			copy := &corev1.Secret{}
			if err := c.Get(ctx, key, copy); err != nil {
				t.Fatal(err)
			}
			if err := c.Delete(ctx, copy); err != nil {
				t.Fatal(err)
			}
			if tc.change != nil {
				tc.change(bundle, pvc, source)
				if err := c.Update(ctx, bundle); err != nil {
					t.Fatal(err)
				}
				if err := c.Update(ctx, pvc); err != nil {
					t.Fatal(err)
				}
				if err := c.Update(ctx, source); err != nil {
					t.Fatal(err)
				}
			}
			_, _, err := r.ensureCRCBootKey(ctx, instance, nil)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				if err := c.Get(ctx, key, copy); err != nil || !metav1.IsControlledBy(copy, instance) {
					t.Fatalf("copy recovery: %v", err)
				}
			} else {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("expected source %s, got %v", tc.want, err)
				}
				if err := c.Get(ctx, key, copy); !apierrors.IsNotFound(err) {
					t.Fatalf("unsafe copy restored: %v", err)
				}
			}
		})
	}
}

func TestCRCBootKeyRejectsConflictsAndReusedName(t *testing.T) {
	ctx := context.Background()
	instance, bundle, pvc, source := bootKeyFixture()
	c := newCRCRecoveryFakeClient(t, instance, bundle, pvc, source)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	bindAndCopyBootKey(t, ctx, r, instance)
	copy := &corev1.Secret{}
	key := types.NamespacedName{Name: resources.CRCBootKeySecretName(instance.Name), Namespace: instance.Namespace}
	if err := c.Get(ctx, key, copy); err != nil {
		t.Fatal(err)
	}
	newInstance := instance.DeepCopy()
	newInstance.UID = "instance-reused-name"
	if _, _, err := r.ensureCRCBootKey(ctx, newInstance, nil); err == nil || !strings.Contains(err.Error(), "owner UID differs") {
		t.Fatalf("expected owner UID conflict, got %v", err)
	}
	if err := c.Delete(ctx, copy); err != nil {
		t.Fatal(err)
	}
	unrelated := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}, Data: map[string][]byte{crcBundleSSHKeyDataKey: []byte("unrelated")}}
	if err := c.Create(ctx, unrelated); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.ensureCRCBootKey(ctx, instance, nil); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("expected unrelated Secret conflict, got %v", err)
	}
	if err := c.Get(ctx, key, unrelated); err != nil || string(unrelated.Data[crcBundleSSHKeyDataKey]) != "unrelated" {
		t.Fatalf("unrelated Secret overwritten: %v", err)
	}
	if pending, err := r.teardownCRCBacking(ctx, instance); err != nil || pending {
		t.Fatalf("expected teardown to preserve unrelated Secret, pending=%t err=%v", pending, err)
	}
	if err := c.Get(ctx, key, unrelated); err != nil {
		t.Fatalf("unrelated Secret removed during teardown: %v", err)
	}
}

func TestCRCBootKeyRecordsBindingBeforeDiskAndUsesLocalCopy(t *testing.T) {
	ctx := context.Background()
	instance, bundle, pvc, source := bootKeyFixture()
	instance.Spec.Template.Memory = testMemory
	instance.Spec.Template.Cores = 4
	c := newCRCRecoveryFakeClient(t, instance, bundle, pvc, source)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	for attempt := range 3 {
		if _, err := r.ensureCRCBacking(ctx, instance, "", "pull-secret"); err != nil {
			t.Fatal(err)
		}
		dv := &cdiv1beta1.DataVolume{}
		err := c.Get(ctx, types.NamespacedName{Name: resources.DataVolumeName(instance.Name), Namespace: instance.Namespace}, dv)
		if attempt == 0 && !apierrors.IsNotFound(err) {
			t.Fatalf("disk was created before binding was stored: %v", err)
		}
		if attempt > 0 && err != nil {
			t.Fatalf("disk not created after binding: %v", err)
		}
		if attempt > 0 && (dv.Spec.Storage.StorageClassName == nil || *dv.Spec.Storage.StorageClassName != "golden-storage") {
			t.Fatalf("disk clone did not keep the bundle storage class: %+v", dv.Spec.Storage.StorageClassName)
		}
	}
	stored := &brokerv1alpha1.ClusterInstance{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.CRC == nil || stored.Status.CRC.BootKey == nil || stored.Status.CRC.BootKey.KeySHA256 != bootKeyHash(source.Data[crcBundleSSHKeyDataKey]) {
		t.Fatalf("binding not persisted: %+v", stored.Status.CRC)
	}
	job := resources.BuildCRCAgentJob(instance, "192.0.2.1", "vmi-one", resources.CRCBootKeySecretName(instance.Name), crcBundleSSHKeyDataKey, "identity", "agent:test", "api.example.test", "pull-secret")
	volume := job.Spec.Template.Spec.Volumes[1]
	if volume.Secret == nil || volume.Secret.SecretName != resources.CRCBootKeySecretName(instance.Name) || job.Namespace != instance.Namespace {
		t.Fatalf("agent does not mount the instance-local key: %+v", job.Spec.Template.Spec.Volumes)
	}
}

func TestCRCBootKeyRejectsDiskRecreationAfterBundleRepreparation(t *testing.T) {
	ctx := context.Background()
	instance, bundle, pvc, source := bootKeyFixture()
	c := newCRCRecoveryFakeClient(t, instance, bundle, pvc, source)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	bindAndCopyBootKey(t, ctx, r, instance)
	pvc.UID = "replacement-disk"
	if err := c.Update(ctx, pvc); err != nil {
		t.Fatal(err)
	}
	dv := &cdiv1beta1.DataVolume{ObjectMeta: metav1.ObjectMeta{Name: resources.DataVolumeName(instance.Name), Namespace: instance.Namespace}}
	if _, _, err := r.ensureCRCBootKey(ctx, instance, dv); err == nil || !strings.Contains(err.Error(), "changed source") {
		t.Fatalf("expected disk recreation to be blocked, got %v", err)
	}
}

func TestCRCBootKeyChecksSourcePVCUntilCloneSucceeds(t *testing.T) {
	for _, tc := range []struct {
		name      string
		phase     cdiv1beta1.DataVolumePhase
		replace   bool
		wantError bool
	}{
		{name: "pending clone with original PVC", phase: cdiv1beta1.CloneScheduled},
		{name: "pending clone with replaced PVC", phase: cdiv1beta1.CloneScheduled, replace: true, wantError: true},
		{name: "unreported clone with replaced PVC", replace: true, wantError: true},
		{name: "completed clone with replaced PVC", phase: cdiv1beta1.Succeeded, replace: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			instance, bundle, pvc, source := bootKeyFixture()
			instance.Spec.Template.Memory = testMemory
			instance.Spec.Template.Cores = 4
			c := newCRCRecoveryFakeClient(t, instance, bundle, pvc, source)
			r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
			bindAndCopyBootKey(t, ctx, r, instance)
			dv := boundCRCSource(instance, crcBootKeyBinding(instance)).dv
			dv.Status.Phase = tc.phase
			if err := c.Create(ctx, dv); err != nil {
				t.Fatal(err)
			}
			if tc.replace {
				pvc.UID = "replacement-disk"
				if err := c.Update(ctx, pvc); err != nil {
					t.Fatal(err)
				}
			}
			_, err := r.ensureCRCBacking(ctx, instance, "", "pull-secret")
			if tc.wantError {
				if err == nil || !strings.Contains(err.Error(), "changed UID while DataVolume") {
					t.Fatalf("expected pending clone to reject replaced source PVC, got %v", err)
				}
				vm := &kubevirtv1.VirtualMachine{}
				if err := c.Get(ctx, types.NamespacedName{Name: resources.VMName(instance.Name), Namespace: instance.Namespace}, vm); !apierrors.IsNotFound(err) {
					t.Fatalf("VM was created for an unverified disk/key pair: %v", err)
				}
			} else if err != nil {
				t.Fatalf("expected valid clone to proceed, got %v", err)
			}
		})
	}
}

func TestCRCBootKeyRequiresBindingForLegacyDisk(t *testing.T) {
	ctx := context.Background()
	instance, bundle, pvc, source := bootKeyFixture()
	dv := &cdiv1beta1.DataVolume{ObjectMeta: metav1.ObjectMeta{Name: resources.DataVolumeName(instance.Name), Namespace: instance.Namespace}}
	c := newCRCRecoveryFakeClient(t, instance, bundle, pvc, source, dv)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	if _, _, err := r.ensureCRCBootKey(ctx, instance, dv); err == nil || !strings.Contains(err.Error(), "without a recorded disk/key binding") {
		t.Fatalf("expected unverified legacy disk to be blocked, got %v", err)
	}
}

func TestCRCBootKeyWaitsForAgentJobBeforeCleanup(t *testing.T) {
	ctx := context.Background()
	instance, bundle, pvc, source := bootKeyFixture()
	c := newCRCRecoveryFakeClient(t, instance, bundle, pvc, source)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	bindAndCopyBootKey(t, ctx, r, instance)
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: resources.CRCAgentJobName(instance.Name, "running"), Namespace: instance.Namespace,
		Labels: resources.CommonLabels(instance), Finalizers: []string{"test.example/hold"},
	}}
	if err := c.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	if pending, err := r.teardownCRCBacking(ctx, instance); err != nil || !pending {
		t.Fatalf("expected cleanup to wait for Job: pending=%t err=%v", pending, err)
	}
	copy := &corev1.Secret{}
	key := types.NamespacedName{Name: resources.CRCBootKeySecretName(instance.Name), Namespace: instance.Namespace}
	if err := c.Get(ctx, key, copy); err != nil {
		t.Fatalf("boot key removed while Job was present: %v", err)
	}
}

func TestCRCBootKeyUnavailableConditionRetriesAfterSourceReturns(t *testing.T) {
	ctx := context.Background()
	instance, bundle, pvc, source := bootKeyFixture()
	instance.Spec.Template.Memory = testMemory
	instance.Spec.Template.Cores = 4
	pull := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: resources.ClusterPullSecretName, Namespace: instance.Namespace}, Data: map[string][]byte{resources.PullSecretDataKey: []byte("pull")}}
	c := newCRCRecoveryFakeClient(t, instance, bundle, pvc, source, pull)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	bindAndCopyBootKey(t, ctx, r, instance)
	copy := &corev1.Secret{}
	key := types.NamespacedName{Name: resources.CRCBootKeySecretName(instance.Name), Namespace: instance.Namespace}
	if err := c.Get(ctx, key, copy); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, copy); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, source); err != nil {
		t.Fatal(err)
	}
	source.ResourceVersion = ""
	result, err := r.reconcileCRC(ctx, instance)
	if err != nil || result.RequeueAfter == 0 {
		t.Fatalf("expected retryable condition, result=%+v err=%v", result, err)
	}
	got := &brokerv1alpha1.ClusterInstance{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), got); err != nil {
		t.Fatal(err)
	}
	condition := apimeta.FindStatusCondition(got.Status.Conditions, conditionTypeReady)
	if got.Status.Phase != brokerv1alpha1.PhaseProvisioning || condition == nil || condition.Reason != "BootKeyUnavailable" {
		t.Fatalf("expected BootKeyUnavailable provisioning condition: %+v", got.Status)
	}
	if err := c.Create(ctx, source); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileCRC(ctx, got); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, key, copy); err != nil {
		t.Fatalf("boot key not restored after source returned: %v", err)
	}
}
