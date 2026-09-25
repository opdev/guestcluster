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

package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
	configv1 "github.com/openshift/api/config/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubevirtv1 "kubevirt.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	crcTestInstanceUID         = "instance-uid"
	crcTestBundleSSHKeyDataKey = "id_rsa"
	crcTestAgentServiceAccount = "agent-account"
	crcTestAgentPodName        = "agent-pod"
	crcTestJobNameLabel        = "job-name"
)

func agentDiagnosticFixture() (*brokerv1alpha1.ClusterInstance, *batchv1.Job, []client.Object) {
	instance := &brokerv1alpha1.ClusterInstance{ObjectMeta: metav1.ObjectMeta{Name: "agent-test", Namespace: "tenant", UID: crcTestInstanceUID}}
	job := resources.BuildCRCAgentJob(instance, "192.0.2.1", "vmi-uid", "ssh", crcTestBundleSSHKeyDataKey, "identity", "agent-image", "api.test", "pull")
	job.Spec.Template.Spec.ServiceAccountName = crcTestAgentServiceAccount
	objects := []client.Object{
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: crcTestAgentServiceAccount, Namespace: instance.Namespace}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "ssh", Namespace: instance.Namespace}, Data: map[string][]byte{crcTestBundleSSHKeyDataKey: []byte("key")}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "identity", Namespace: instance.Namespace}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "pull", Namespace: instance.Namespace}},
	}
	return instance, job, objects
}

func TestInspectCRCAgentStartupAndRecovery(t *testing.T) {
	for _, tc := range []struct {
		name, reason, detail string
		change               func(*batchv1.Job, *[]client.Object)
	}{
		{"no Pod", "AgentPodPending", "Job Pod", nil},
		{"Job cannot create Pod", "AgentPodCreationFailed", "FailedCreate", func(job *batchv1.Job, objects *[]client.Object) {
			*objects = append(*objects, &corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: "creation", Namespace: job.Namespace}, InvolvedObject: corev1.ObjectReference{Kind: "Job", Name: job.Name}, Reason: "FailedCreate"})
		}},
		{"image pull", "AgentPodBlocked", "ImagePullBackOff", func(job *batchv1.Job, objects *[]client.Object) {
			*objects = append(*objects, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: crcTestAgentPodName, Namespace: job.Namespace, Labels: map[string]string{"batch.kubernetes.io/job-name": job.Name}}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "crc-agent", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}}}}})
		}},
		{"unschedulable", "AgentPodBlocked", "Unschedulable", func(job *batchv1.Job, objects *[]client.Object) {
			*objects = append(*objects, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: crcTestAgentPodName, Namespace: job.Namespace, Labels: map[string]string{crcTestJobNameLabel: job.Name}}, Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: "Unschedulable"}}}})
		}},
		{"agent working", "AgentWorking", "working", func(job *batchv1.Job, objects *[]client.Object) {
			*objects = append(*objects, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: crcTestAgentPodName, Namespace: job.Namespace, Labels: map[string]string{crcTestJobNameLabel: job.Name}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			instance, job, objects := agentDiagnosticFixture()
			if tc.change != nil {
				tc.change(job, &objects)
			}
			c := newCRCRecoveryFakeClient(t, objects...)
			r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
			condition, err := r.inspectCRCAgent(context.Background(), instance, job, false)
			if err != nil || condition == nil || condition.Reason != tc.reason || !strings.Contains(condition.Message, tc.detail) || !strings.Contains(condition.Message, job.Namespace+"/"+job.Name) {
				t.Fatalf("condition = %+v, err = %v", condition, err)
			}
			if tc.reason == "AgentWorking" && condition.Status != metav1.ConditionUnknown {
				t.Fatalf("progressing Job marked blocked: %+v", condition)
			}
		})
	}
}

func TestInspectCRCAgentPrerequisiteCanRecover(t *testing.T) {
	ctx := context.Background()
	instance, job, objects := agentDiagnosticFixture()
	objects = objects[1:] // no ServiceAccount
	c := newCRCRecoveryFakeClient(t, objects...)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	check := func(want string) {
		t.Helper()
		condition, err := r.inspectCRCAgent(ctx, instance, job, false)
		if err != nil || condition == nil || condition.Reason != want {
			t.Fatalf("condition = %+v, err = %v; want %s", condition, err, want)
		}
	}
	check("AgentPrerequisiteMissing")
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: crcTestAgentServiceAccount, Namespace: instance.Namespace}}
	if err := c.Create(ctx, sa); err != nil {
		t.Fatal(err)
	}
	check("AgentPodPending")
	// Instance-owned accounts require their own binding. A legacy account
	// without an instance owner does not require that binding.
	sa.OwnerReferences = []metav1.OwnerReference{{APIVersion: brokerv1alpha1.GroupVersion.String(), Kind: "ClusterInstance", Name: instance.Name, UID: instance.UID, Controller: ptrBool(true)}}
	if err := c.Update(ctx, sa); err != nil {
		t.Fatal(err)
	}
	check("AgentPrerequisiteMissing")
	binding := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "agent-binding", Namespace: instance.Namespace, OwnerReferences: sa.OwnerReferences}, Subjects: []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: sa.Name, Namespace: instance.Namespace}}}
	if err := c.Create(ctx, binding); err != nil {
		t.Fatal(err)
	}
	check("AgentPodPending")
}

func TestInspectCRCAgentCompletedHandoff(t *testing.T) {
	for _, tc := range []struct {
		name, detail string
		secret       map[string][]byte
	}{
		{"missing", "is missing", nil},
		{"empty", "empty or missing kubeconfig", map[string][]byte{}},
		{"wrong VMI", "mismatched VMI UID", map[string][]byte{resources.KubeconfigSecretKey: []byte("config"), resources.VMIUIDSecretKey: []byte("other")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			instance, job, objects := agentDiagnosticFixture()
			completed := metav1.NewTime(time.Now().Add(-time.Minute))
			job.Status.Succeeded = 1
			job.Status.CompletionTime = &completed
			if tc.secret != nil {
				objects = append(objects, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: resources.RawKubeconfigSecretNameForVMI(instance.Name, "vmi-uid"), Namespace: instance.Namespace}, Data: tc.secret})
			}
			c := newCRCRecoveryFakeClient(t, objects...)
			r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
			condition, err := r.inspectCRCAgent(context.Background(), instance, job, false)
			if err != nil || condition.Reason != "HandoffInvalid" || condition.Status != metav1.ConditionFalse || !strings.Contains(condition.Message, tc.detail) {
				t.Fatalf("condition = %+v, err = %v", condition, err)
			}
			job.Status.CompletionTime = &metav1.Time{Time: time.Now()}
			condition, err = r.inspectCRCAgent(context.Background(), instance, job, false)
			if err != nil || condition.Reason != "HandoffPending" {
				t.Fatalf("cache delay: condition = %+v, err = %v", condition, err)
			}
		})
	}
}

func TestInspectCRCAgentFailedJobNamesPod(t *testing.T) {
	instance, job, objects := agentDiagnosticFixture()
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"}}
	objects = append(objects, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "failed-pod", Namespace: job.Namespace, UID: types.UID("pod-uid"), Labels: map[string]string{"job-name": job.Name}}})
	c := newCRCRecoveryFakeClient(t, objects...)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	_, err := r.inspectCRCAgent(context.Background(), instance, job, false)
	if err == nil || !strings.Contains(err.Error(), "failed-pod") || !strings.Contains(err.Error(), "BackoffLimitExceeded") {
		t.Fatalf("missing failed Job evidence: %v", err)
	}
}

func TestReconcileCRCAgentPrerequisiteClearsBlockedStatus(t *testing.T) {
	ctx := context.Background()
	instance, job, objects := agentDiagnosticFixture()
	instance.Spec.Type = brokerv1alpha1.TopologyCRC
	instance.Spec.Template.PullSecretRef.Name = "pull"
	instance.Spec.Template.BundleSSHKeyRef = &corev1.LocalObjectReference{Name: "ssh"}
	instance.Spec.Template.Memory = testMemory
	instance.Spec.Template.Cores = 4
	objects = append(objects[1:], instance, job,
		&kubevirtv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{Name: resources.VMName(instance.Name), Namespace: instance.Namespace}, Status: kubevirtv1.VirtualMachineStatus{Ready: true}},
		&kubevirtv1.VirtualMachineInstance{ObjectMeta: metav1.ObjectMeta{Name: resources.VMName(instance.Name), Namespace: instance.Namespace, UID: "vmi-uid"}, Status: kubevirtv1.VirtualMachineInstanceStatus{Phase: kubevirtv1.Running, Interfaces: []kubevirtv1.VirtualMachineInstanceNetworkInterface{{IP: "192.0.2.1"}}}},
		&configv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: statusIngressName}, Spec: configv1.IngressSpec{Domain: statusIngressDomain}},
	)
	objects[2].(*corev1.Secret).Data = map[string][]byte{resources.PullSecretDataKey: []byte("pull")}
	c := newCRCRecoveryFakeClient(t, objects...)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	assertReason := func(want string) {
		t.Helper()
		if _, err := r.reconcileCRC(ctx, instance); err != nil {
			t.Fatalf("reconcileCRC: %v", err)
		}
		got := &brokerv1alpha1.ClusterInstance{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(instance), got); err != nil {
			t.Fatal(err)
		}
		for _, condition := range got.Status.Conditions {
			if condition.Type == conditionTypeCRCAgent && condition.Reason == want && got.Status.Phase == brokerv1alpha1.PhaseProvisioning {
				return
			}
		}
		t.Fatalf("expected %s while provisioning, got %+v", want, got.Status)
	}
	assertReason("AgentPrerequisiteMissing")
	if err := c.Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: crcTestAgentServiceAccount, Namespace: instance.Namespace}}); err != nil {
		t.Fatal(err)
	}
	assertReason("AgentPodPending")

	var apiReady atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if !apiReady.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer server.Close()
	raw := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: resources.RawKubeconfigSecretNameForVMI(instance.Name, "vmi-uid"), Namespace: instance.Namespace},
		Data: map[string][]byte{
			resources.VMIUIDSecretKey: []byte("vmi-uid"),
			resources.KubeconfigSecretKey: []byte(fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- cluster:
    server: %s
    insecure-skip-tls-verify: true
  name: guest
contexts:
- context:
    cluster: guest
  name: guest
current-context: guest
`, server.URL)),
		},
	}
	if err := c.Create(ctx, raw); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileCRC(ctx, instance); err != nil {
		t.Fatal(err)
	}
	got := &brokerv1alpha1.ClusterInstance{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), got); err != nil {
		t.Fatal(err)
	}
	agentCondition := apimeta.FindStatusCondition(got.Status.Conditions, conditionTypeCRCAgent)
	readyCondition := apimeta.FindStatusCondition(got.Status.Conditions, conditionTypeReady)
	if agentCondition == nil || agentCondition.Status != metav1.ConditionTrue ||
		readyCondition == nil || readyCondition.Status != metav1.ConditionFalse || readyCondition.Reason != "GuestAPIUnavailable" || got.Status.Phase != brokerv1alpha1.PhaseProvisioning {
		t.Fatalf("expected received handoff with guest API pending, got %+v", got.Status)
	}

	apiReady.Store(true)
	if _, err := r.reconcileCRC(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != brokerv1alpha1.PhaseReady || apimeta.FindStatusCondition(got.Status.Conditions, conditionTypeCRCAgent) != nil {
		t.Fatalf("expected Ready with cleared agent condition, got %+v", got.Status)
	}
}

func ptrBool(value bool) *bool { return &value }
