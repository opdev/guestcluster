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
	"time"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const crcHandoffCacheDelay = 30 * time.Second

type crcAgentFailure struct{ message string }

func (e crcAgentFailure) Error() string { return e.message }

// inspectCRCAgent reads the existing Job template, including its account and
// Secret names. This also supports Jobs created with the legacy shared account.
// Only reasons, not event messages or Secret contents, go into status.
func (r *ClusterInstanceReconciler) inspectCRCAgent(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, job *batchv1.Job, handoffReady bool) (*metav1.Condition, error) {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return nil, r.crcAgentJobFailure(ctx, job, &c)
		}
	}
	if handoffReady {
		return crcAgentDiagnostic(ctx, job, metav1.ConditionTrue, "HandoffReceived", "valid VMI handoff received"), nil
	}
	if condition, complete, err := r.inspectCompletedCRCAgentJob(ctx, instance, job); complete || err != nil {
		return condition, err
	}
	if condition, err := r.inspectCRCAgentPrerequisites(ctx, instance, job); condition != nil || err != nil {
		return condition, err
	}
	return r.inspectCRCAgentPods(ctx, job)
}

func (r *ClusterInstanceReconciler) crcAgentJobFailure(ctx context.Context, job *batchv1.Job, failed *batchv1.JobCondition) error {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(job.Namespace)); err != nil {
		return fmt.Errorf("listing failed agent Job Pods: %w", err)
	}
	podName := ""
	for _, pod := range pods.Items {
		if pod.Labels["batch.kubernetes.io/job-name"] == job.Name || pod.Labels["job-name"] == job.Name {
			podName = fmt.Sprintf(" Pod %s", pod.Name)
			break
		}
	}
	detail := fmt.Sprintf("Job failed (%s); inspect%s events and crc-agent logs", failed.Reason, podName)
	condition := crcAgentDiagnostic(ctx, job, metav1.ConditionFalse, "AgentJobFailed", detail)
	return crcAgentFailure{condition.Message}
}

func crcAgentDiagnostic(ctx context.Context, job *batchv1.Job, status metav1.ConditionStatus, reason, detail string) *metav1.Condition {
	location := fmt.Sprintf("crc-agent Job %s/%s", job.Namespace, job.Name)
	message := fmt.Sprintf("%s: %s; inspect with kubectl -n %s describe job %s", location, detail, job.Namespace, job.Name)
	logf.FromContext(ctx).Info("CRC agent diagnostic", "job", job.Name, "namespace", job.Namespace, "reason", reason, "detail", detail)
	return &metav1.Condition{Type: conditionTypeCRCAgent, Status: status, Reason: reason, Message: message}
}

func (r *ClusterInstanceReconciler) inspectCompletedCRCAgentJob(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, job *batchv1.Job) (*metav1.Condition, bool, error) {
	completedAt := job.Status.CompletionTime
	completed := job.Status.Succeeded > 0
	for i := range job.Status.Conditions {
		condition := &job.Status.Conditions[i]
		if condition.Type != batchv1.JobComplete || condition.Status != corev1.ConditionTrue {
			continue
		}
		completed = true
		if completedAt == nil && !condition.LastTransitionTime.IsZero() {
			completedAt = &condition.LastTransitionTime
		}
	}
	if !completed {
		return nil, false, nil
	}
	if completedAt == nil || time.Since(completedAt.Time) < crcHandoffCacheDelay {
		condition := crcAgentDiagnostic(ctx, job, metav1.ConditionUnknown, "HandoffPending", "Job completed; waiting for the handoff Secret to reach the cache")
		return condition, true, nil
	}
	secretName := resources.RawKubeconfigSecretNameForVMI(instance.Name, stringFromJobVMI(job))
	key := types.NamespacedName{Namespace: job.Namespace, Name: secretName}
	secret := &corev1.Secret{}
	if err := r.Get(ctx, key, secret); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, true, fmt.Errorf("getting agent handoff Secret %s: %w", key, err)
		}
		detail := fmt.Sprintf("Job completed but handoff Secret %s is missing; inspect crc-agent logs", key)
		return crcAgentDiagnostic(ctx, job, metav1.ConditionFalse, "HandoffInvalid", detail), true, nil
	}
	if len(secret.Data[resources.KubeconfigSecretKey]) == 0 {
		detail := fmt.Sprintf("Job completed but handoff Secret %s has an empty or missing kubeconfig; inspect crc-agent logs", key)
		return crcAgentDiagnostic(ctx, job, metav1.ConditionFalse, "HandoffInvalid", detail), true, nil
	}
	detail := fmt.Sprintf("Job completed but handoff Secret %s has a mismatched VMI UID; inspect crc-agent logs", key)
	return crcAgentDiagnostic(ctx, job, metav1.ConditionFalse, "HandoffInvalid", detail), true, nil
}

func (r *ClusterInstanceReconciler) inspectCRCAgentPrerequisites(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, job *batchv1.Job) (*metav1.Condition, error) {
	account := job.Spec.Template.Spec.ServiceAccountName
	if account == "" {
		account = "default"
	}
	sa := &corev1.ServiceAccount{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: account}, sa); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("getting agent ServiceAccount %s/%s: %w", job.Namespace, account, err)
		}
		detail := fmt.Sprintf("ServiceAccount %s/%s is missing", job.Namespace, account)
		return crcAgentDiagnostic(ctx, job, metav1.ConditionFalse, "AgentPrerequisiteMissing", detail), nil
	}
	if condition, err := r.inspectCRCAgentRoleBinding(ctx, instance, job, sa, account); condition != nil || err != nil {
		return condition, err
	}
	return r.inspectCRCAgentVolumeSecrets(ctx, job)
}

func (r *ClusterInstanceReconciler) inspectCRCAgentRoleBinding(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, job *batchv1.Job, sa *corev1.ServiceAccount, account string) (*metav1.Condition, error) {
	if !metav1.IsControlledBy(sa, instance) {
		return nil, nil
	}
	bindings := &rbacv1.RoleBindingList{}
	if err := r.platformReader().List(ctx, bindings, client.InNamespace(job.Namespace)); err != nil {
		return nil, fmt.Errorf("listing agent RoleBindings in %s: %w", job.Namespace, err)
	}
	for i := range bindings.Items {
		binding := &bindings.Items[i]
		if !metav1.IsControlledBy(binding, instance) {
			continue
		}
		for _, subject := range binding.Subjects {
			if subject.Kind == rbacv1.ServiceAccountKind && subject.Name == account && subject.Namespace == job.Namespace {
				return nil, nil
			}
		}
	}
	detail := fmt.Sprintf("instance-owned RoleBinding for ServiceAccount %s/%s is missing", job.Namespace, account)
	return crcAgentDiagnostic(ctx, job, metav1.ConditionFalse, "AgentPrerequisiteMissing", detail), nil
}

func (r *ClusterInstanceReconciler) inspectCRCAgentVolumeSecrets(ctx context.Context, job *batchv1.Job) (*metav1.Condition, error) {
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.Secret == nil {
			continue
		}
		key := types.NamespacedName{Namespace: job.Namespace, Name: volume.Secret.SecretName}
		secret := &corev1.Secret{}
		if err := r.Get(ctx, key, secret); err != nil {
			if !apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("getting agent Secret %s: %w", key, err)
			}
			detail := fmt.Sprintf("mounted Secret %s is missing", key)
			return crcAgentDiagnostic(ctx, job, metav1.ConditionFalse, "AgentPrerequisiteMissing", detail), nil
		}
		for _, item := range volume.Secret.Items {
			if len(secret.Data[item.Key]) == 0 {
				detail := fmt.Sprintf("mounted Secret %s has no data for key %s", key, item.Key)
				return crcAgentDiagnostic(ctx, job, metav1.ConditionFalse, "AgentPrerequisiteMissing", detail), nil
			}
		}
	}
	return nil, nil
}

func (r *ClusterInstanceReconciler) inspectCRCAgentPods(ctx context.Context, job *batchv1.Job) (*metav1.Condition, error) {
	location := fmt.Sprintf("crc-agent Job %s/%s", job.Namespace, job.Name)
	pods, err := r.listCRCAgentPods(ctx, job, location)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for i := range pods {
		if seen[pods[i].Name] {
			continue
		}
		seen[pods[i].Name] = true
		if condition := inspectCRCAgentPodStatus(ctx, job, &pods[i]); condition != nil {
			return condition, nil
		}
	}
	return r.inspectCRCAgentEvents(ctx, job, pods, seen)
}

func (r *ClusterInstanceReconciler) listCRCAgentPods(ctx context.Context, job *batchv1.Job, location string) ([]corev1.Pod, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(job.Namespace), client.MatchingLabels{"batch.kubernetes.io/job-name": job.Name}); err != nil {
		return nil, fmt.Errorf("listing Pods for %s: %w", location, err)
	}
	legacy := &corev1.PodList{}
	if err := r.List(ctx, legacy, client.InNamespace(job.Namespace), client.MatchingLabels{"job-name": job.Name}); err != nil {
		return nil, fmt.Errorf("listing legacy Pods for %s: %w", location, err)
	}
	return append(pods.Items, legacy.Items...), nil
}

func inspectCRCAgentPodStatus(ctx context.Context, job *batchv1.Job, pod *corev1.Pod) *metav1.Condition {
	for _, status := range append(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses...) {
		if detail := agentContainerBlocked(pod, status); detail != "" {
			return crcAgentDiagnostic(ctx, job, metav1.ConditionFalse, "AgentPodBlocked", detail)
		}
	}
	if pod.Status.Phase == corev1.PodPending {
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodScheduled && condition.Status == corev1.ConditionFalse {
				detail := fmt.Sprintf("Pod %s cannot be scheduled (%s); describe the Pod", pod.Name, condition.Reason)
				return crcAgentDiagnostic(ctx, job, metav1.ConditionFalse, "AgentPodBlocked", detail)
			}
		}
	}
	if pod.Status.Phase == corev1.PodFailed {
		detail := fmt.Sprintf("Pod %s failed (%s); inspect crc-agent logs; Job may retry", pod.Name, pod.Status.Reason)
		return crcAgentDiagnostic(ctx, job, metav1.ConditionUnknown, "AgentRetrying", detail)
	}
	return nil
}

func (r *ClusterInstanceReconciler) inspectCRCAgentEvents(ctx context.Context, job *batchv1.Job, pods []corev1.Pod, seen map[string]bool) (*metav1.Condition, error) {
	podNames := map[string]bool{}
	pendingPods := map[string]bool{}
	for _, pod := range pods {
		podNames[pod.Name] = true
		pendingPods[pod.Name] = pod.Status.Phase == corev1.PodPending
	}
	events := &corev1.EventList{}
	if err := r.platformReader().List(ctx, events, client.InNamespace(job.Namespace)); err != nil {
		return nil, fmt.Errorf("listing events for crc-agent Job %s/%s: %w", job.Namespace, job.Name, err)
	}
	for _, event := range events.Items {
		if len(seen) == 0 && event.InvolvedObject.Kind == "Job" && event.InvolvedObject.Name == job.Name && event.Reason == "FailedCreate" {
			detail := "Job cannot create a Pod (FailedCreate); inspect Job events"
			return crcAgentDiagnostic(ctx, job, metav1.ConditionFalse, "AgentPodCreationFailed", detail), nil
		}
		if event.InvolvedObject.Kind == "Pod" && podNames[event.InvolvedObject.Name] && pendingPods[event.InvolvedObject.Name] {
			if agentPodEventBlocked(event.Reason) {
				detail := fmt.Sprintf("Pod %s cannot start (%s); inspect Pod events", event.InvolvedObject.Name, event.Reason)
				return crcAgentDiagnostic(ctx, job, metav1.ConditionFalse, "AgentPodBlocked", detail), nil
			}
		}
	}
	if len(seen) == 0 {
		return crcAgentDiagnostic(ctx, job, metav1.ConditionUnknown, "AgentPodPending", "waiting for Job Pod; inspect Job events if it does not start"), nil
	}
	return crcAgentDiagnostic(ctx, job, metav1.ConditionUnknown, "AgentWorking", "agent is working; inspect crc-agent Pod logs if handoff does not arrive"), nil
}

func agentPodEventBlocked(reason string) bool {
	switch reason {
	case "FailedMount", "FailedScheduling", "Failed", "FailedCreatePodSandBox":
		return true
	default:
		return false
	}
}

func agentContainerBlocked(pod *corev1.Pod, status corev1.ContainerStatus) string {
	if status.State.Waiting == nil {
		return ""
	}
	switch status.State.Waiting.Reason {
	case "ImagePullBackOff", "ErrImagePull", "InvalidImageName", "CreateContainerConfigError", "CreateContainerError", "RunContainerError", "CrashLoopBackOff":
		return fmt.Sprintf("Pod %s container %s cannot start (%s); describe the Pod and inspect its events", pod.Name, status.Name, status.State.Waiting.Reason)
	}
	return ""
}

func stringFromJobVMI(job *batchv1.Job) string {
	for _, container := range job.Spec.Template.Spec.Containers {
		for _, env := range container.Env {
			if env.Name == "CRC_VMI_UID" {
				return env.Value
			}
		}
	}
	return ""
}
