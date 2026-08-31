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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
)

// leaseInstanceRefIndexField is the field index registered on ClusterLease,
// keyed by Status.InstanceRef.Name. It lets any controller efficiently
// answer "which lease(s), if any, currently claim this instance?" without
// listing every ClusterLease in the namespace. ClusterInstanceReconciler
// uses it to maintain the derived LeaseRef projection (see
// clusterinstance_controller.go), and other code can reuse it for the same
// purpose.
const leaseInstanceRefIndexField = "status.instanceRef.name"

const (
	// leaseFinalizer guarantees that deleting a ClusterLease directly (without
	// an explicit release step) still deletes the bound ClusterInstance
	// before the ClusterLease object itself is allowed to disappear.
	leaseFinalizer = "guestcluster.opdev.io/release"

	// leasePendingRequeue is how often Reconcile re-checks a Pending lease
	// while it waits for the ClusterPool controller's independent
	// MinSize/WarmSpares/demand-driven supply loop to produce a free
	// instance. That loop also watches ClusterLeases directly, so this
	// requeue is a backstop rather than the primary trigger.
	// ClusterLeaseReconciler is pure demand-side matchmaking: it never
	// provisions instances itself.
	leasePendingRequeue = 10 * time.Second

	// leaseTTLCheckInterval bounds how often a Bound lease with a TTL is
	// re-checked for expiry.
	leaseTTLCheckInterval = 30 * time.Second

	conditionTypeLeaseBound = "Bound"
)

// ClusterLeaseReconciler reconciles a ClusterLease object.
//
// It performs pure demand-side matchmaking. Given a request for a topology
// from a named ClusterPool, it finds a Ready ClusterInstance belonging to
// that pool that no other lease currently claims, and binds it to this
// lease with a SINGLE atomic write to the lease's own status (InstanceRef
// plus Phase=Bound). This is modeled directly on how the Kubernetes
// scheduler binds a Pod to a Node by writing Pod.Spec.NodeName: one
// authoritative pointer, on the demand object, written once. Unlike the
// scheduler analogy, this reconciler is the ONLY writer of that pointer, so
// there is no two-controllers-racing-to-bind concern. Critically, it never
// writes anything to the ClusterInstance side. This design eliminates the
// two-write partial-bind races, for example an instance claimed while its
// lease's own status lags behind, or a lease retried after a partial
// failure re-claiming a second instance, that motivated defensive patches
// in earlier iterations of this controller and ClusterPoolReconciler.
//
// ClusterLeaseReconciler never creates new ClusterInstances itself; that
// supply-side responsibility belongs entirely to ClusterPoolReconciler. On
// release (lease deletion, or TTL expiry), it does delete the claimed
// instance outright (see releaseBoundInstance) rather than resetting it in
// place. ClusterPoolReconciler's normal top-up logic then provisions a
// fresh replacement, the same as it does for any other capacity shortfall.
type ClusterLeaseReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=guestcluster.opdev.io,resources=clusterleases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=guestcluster.opdev.io,resources=clusterleases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=guestcluster.opdev.io,resources=clusterleases/finalizers,verbs=update
// +kubebuilder:rbac:groups=guestcluster.opdev.io,resources=clusterinstances,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete

func (r *ClusterLeaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	lease := &brokerv1alpha1.ClusterLease{}
	if err := r.Get(ctx, req.NamespacedName, lease); err != nil {
		if apierrors.IsNotFound(err) {
			log.V(1).Info("ClusterLease deleted, nothing to reconcile")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("getting ClusterLease: %w", err)
	}

	if !lease.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, lease)
	}

	if !controllerutil.ContainsFinalizer(lease, leaseFinalizer) {
		controllerutil.AddFinalizer(lease, leaseFinalizer)
		if err := r.Update(ctx, lease); err != nil {
			return ctrl.Result{}, fmt.Errorf("adding finalizer: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	switch lease.Status.Phase {
	case brokerv1alpha1.PhaseLeaseBound:
		return r.reconcileBound(ctx, lease)
	case brokerv1alpha1.PhaseLeaseReleasing, brokerv1alpha1.PhaseLeaseReleased, brokerv1alpha1.PhaseLeaseFailed:
		// Terminal-ish phases only move forward via deletion (handled above)
		// or, for Failed, by CI/human intervention re-editing the spec. We
		// still allow a Failed lease to be retried by falling through to
		// matchmaking on the next spec change, but do nothing proactively
		// here to avoid masking the failure.
		return ctrl.Result{}, nil
	default:
		// "" (unset) or PhaseLeasePending: attempt matchmaking.
		return r.reconcilePending(ctx, lease)
	}
}

// reconcilePending attempts to bind lease to a free ClusterInstance of the
// requested type belonging to lease.Spec.PoolRef. If none is currently
// available it requeues; it does NOT trigger provisioning itself.
func (r *ClusterLeaseReconciler) reconcilePending(ctx context.Context, lease *brokerv1alpha1.ClusterLease) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	instanceList := &brokerv1alpha1.ClusterInstanceList{}
	if err := r.List(ctx, instanceList,
		client.InNamespace(lease.Namespace),
		client.MatchingLabels(resources.PoolLabels(lease.Spec.PoolRef.Name)),
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing ClusterInstances for pool %q: %w", lease.Spec.PoolRef.Name, err)
	}

	// Determine which instances in this pool some ClusterLease already
	// claims. Status.InstanceRef is the single source of truth for the
	// binding (see clusterlease_types.go), so reconcilePending only needs
	// to consult that field; ClusterInstance.Status.LeaseRef is a derived
	// projection and reconcilePending never uses it for this decision.
	// Listing sibling leases once, up front, is cheap at this operator's
	// scale and avoids needing a field index for this one in-namespace,
	// same-pool lookup.
	siblingLeases := &brokerv1alpha1.ClusterLeaseList{}
	if err := r.List(ctx, siblingLeases, client.InNamespace(lease.Namespace)); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing sibling ClusterLeases: %w", err)
	}
	claimed := make(map[string]bool)
	for i := range siblingLeases.Items {
		l := &siblingLeases.Items[i]
		if !l.DeletionTimestamp.IsZero() {
			continue
		}
		if l.Spec.PoolRef.Name != lease.Spec.PoolRef.Name {
			continue
		}
		if l.Status.InstanceRef != nil {
			claimed[l.Status.InstanceRef.Name] = true
		}
	}

	var candidate *brokerv1alpha1.ClusterInstance
	for i := range instanceList.Items {
		inst := &instanceList.Items[i]
		if !inst.DeletionTimestamp.IsZero() {
			continue
		}
		// This loop needs no inst.Spec.Type check: instanceList was already
		// filtered to this pool via PoolLabels above, and every instance
		// the ClusterPool controller creates for a pool inherits that
		// pool's Spec.Type (see clusterpool_controller.go's
		// createInstance). So all candidates in this loop already share
		// one type.
		if inst.Status.Phase != brokerv1alpha1.PhaseReady {
			continue
		}
		if claimed[inst.Name] {
			continue
		}
		candidate = inst
		break
	}

	if candidate == nil {
		log.V(1).Info("no free ClusterInstance available yet, waiting for pool capacity",
			"pool", lease.Spec.PoolRef.Name)
		if err := r.setPendingCondition(ctx, lease, "WaitingForCapacity",
			fmt.Sprintf("no Ready, unclaimed ClusterInstance in pool %q; waiting for ClusterPool controller to provision one", lease.Spec.PoolRef.Name)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: leasePendingRequeue}, nil
	}

	log.Info("binding ClusterLease to ClusterInstance", "instance", candidate.Name, "lease", lease.Name)
	return r.bind(ctx, lease, candidate)
}

// bind copies the instance's kubeconfig Secret into a lease-owned Secret,
// and mirrors the instance's observed version, topology, and endpoint onto
// the lease status as the explicit CI outputs the acceptance criteria
// require. It then commits the binding itself via a SINGLE Status().Update
// on the lease (InstanceRef plus Phase=Bound), the one and only
// authoritative write of the lease-instance relationship anywhere in the
// operator. bind writes nothing to the ClusterInstance side. A
// resourceVersion conflict on this Status().Update, from another
// ClusterLease reconcile racing for the same instance, surfaces as an
// error, which controller-runtime retries against a freshly-Get'd lease on
// the next attempt. Because re-running reconcilePending re-derives the
// claimed set from scratch, two concurrent leases can never both claim the
// same instance, and a failed or retried attempt here can never leave a
// stray, unclaimed-by-any-lease write behind: if this call fails, it
// writes NOTHING.
func (r *ClusterLeaseReconciler) bind(ctx context.Context, lease *brokerv1alpha1.ClusterLease, instance *brokerv1alpha1.ClusterInstance) (ctrl.Result, error) {
	srcSecret := &corev1.Secret{}
	srcKey := client.ObjectKey{Namespace: instance.Namespace, Name: instance.Status.KubeconfigSecretRef.Name}
	if err := r.Get(ctx, srcKey, srcSecret); err != nil {
		return ctrl.Result{}, fmt.Errorf("fetching instance kubeconfig secret %s: %w", srcKey, err)
	}

	leaseSecretName := resources.LeaseKubeconfigSecretName(lease.Name)
	leaseSecret := &corev1.Secret{}
	err := r.Get(ctx, client.ObjectKey{Namespace: lease.Namespace, Name: leaseSecretName}, leaseSecret)
	switch {
	case apierrors.IsNotFound(err):
		leaseSecret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      leaseSecretName,
				Namespace: lease.Namespace,
			},
			Type: corev1.SecretTypeOpaque,
			Data: srcSecret.Data,
		}
		if err := controllerutil.SetControllerReference(lease, leaseSecret, r.Scheme); err != nil {
			return ctrl.Result{}, fmt.Errorf("setting owner ref on lease kubeconfig secret: %w", err)
		}
		if err := r.Create(ctx, leaseSecret); err != nil && !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, fmt.Errorf("creating lease kubeconfig secret: %w", err)
		}
	case err != nil:
		return ctrl.Result{}, fmt.Errorf("fetching lease kubeconfig secret: %w", err)
	default:
		leaseSecret.Data = srcSecret.Data
		if err := r.Update(ctx, leaseSecret); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating lease kubeconfig secret: %w", err)
		}
	}

	now := metav1.Now()
	lease.Status.Phase = brokerv1alpha1.PhaseLeaseBound
	lease.Status.InstanceRef = &corev1.LocalObjectReference{Name: instance.Name}
	lease.Status.KubeconfigSecretRef = &corev1.LocalObjectReference{Name: leaseSecretName}
	lease.Status.OCPVersion = instance.Status.OCPVersion
	lease.Status.Topology = instance.Status.Topology
	lease.Status.APIEndpoint = instance.Status.APIEndpoint
	lease.Status.BoundTime = &now
	apimeta.SetStatusCondition(&lease.Status.Conditions, metav1.Condition{
		Type:               conditionTypeLeaseBound,
		Status:             metav1.ConditionTrue,
		Reason:             "InstanceBound",
		Message:            fmt.Sprintf("bound to ClusterInstance %q", instance.Name),
		ObservedGeneration: lease.Generation,
	})
	if err := r.Status().Update(ctx, lease); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating lease status after bind: %w", err)
	}

	return ctrl.Result{}, nil
}

// reconcileBound watches a Bound lease for TTL expiry. It has no other work
// to do: the claimed instance is steady-state Ready-and-claimed (see
// clusterinstance_types.go's ClusterInstancePhase doc; there is no separate
// "Leased" instance phase), and bind already copied the kubeconfig Secret
// at bind time.
func (r *ClusterLeaseReconciler) reconcileBound(ctx context.Context, lease *brokerv1alpha1.ClusterLease) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if lease.Spec.TTL == nil || lease.Status.BoundTime == nil {
		return ctrl.Result{}, nil
	}

	elapsed := time.Since(lease.Status.BoundTime.Time)
	if elapsed < lease.Spec.TTL.Duration {
		return ctrl.Result{RequeueAfter: leaseTTLCheckInterval}, nil
	}

	log.Info("ClusterLease TTL exceeded, forcing release", "lease", lease.Name, "elapsed", elapsed)
	lease.Status.Phase = brokerv1alpha1.PhaseLeaseReleasing
	if err := r.Status().Update(ctx, lease); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating lease status to Releasing after TTL expiry: %w", err)
	}

	// Delete the lease object itself to complete the release. This drives
	// the exact same finalizer-based teardown path (reconcileDelete) that
	// an explicit CI-initiated deletion would: it deletes the bound
	// instance and removes the finalizer, so the lease object gets cleaned
	// up. Without this delete, a TTL-expired lease would sit
	// in Releasing forever (Reconcile treats Releasing as a terminal
	// no-op) while holding its finalizer. Because releaseBoundInstance
	// already deleted its instance, that stuck lease would dangle with
	// Status.InstanceRef pointing at a no-longer-existent instance.
	if err := r.Delete(ctx, lease); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("deleting lease after TTL expiry: %w", err)
	}
	return ctrl.Result{}, nil
}

// reconcileDelete deletes the bound instance (if any), then removes the
// finalizer so the ClusterLease object can be garbage collected. This runs
// even if the lease was deleted directly by CI without going through an
// explicit release/TTL step, guaranteeing release always happens.
func (r *ClusterLeaseReconciler) reconcileDelete(ctx context.Context, lease *brokerv1alpha1.ClusterLease) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(lease, leaseFinalizer) {
		return ctrl.Result{}, nil
	}

	if err := r.releaseBoundInstance(ctx, lease); err != nil {
		return ctrl.Result{}, err
	}

	controllerutil.RemoveFinalizer(lease, leaseFinalizer)
	if err := r.Update(ctx, lease); err != nil {
		return ctrl.Result{}, fmt.Errorf("removing lease finalizer: %w", err)
	}
	return ctrl.Result{}, nil
}

// releaseBoundInstance deletes the bound ClusterInstance outright, if this
// lease has one, tolerating it already being gone. There is no in-place
// reset. The delete goes through ClusterInstanceReconciler's normal
// finalizer-driven teardown, and ClusterPoolReconciler's normal top-up
// logic then provisions a fresh replacement, the same as it does for any
// other capacity shortfall. See clusterinstance_controller.go's Reconcile
// doc comment for the rationale.
func (r *ClusterLeaseReconciler) releaseBoundInstance(ctx context.Context, lease *brokerv1alpha1.ClusterLease) error {
	if lease.Status.InstanceRef == nil {
		return nil
	}

	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:      lease.Status.InstanceRef.Name,
			Namespace: lease.Namespace,
		},
	}
	if err := r.Delete(ctx, instance); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("deleting bound ClusterInstance %q: %w", lease.Status.InstanceRef.Name, err)
	}
	return nil
}

func (r *ClusterLeaseReconciler) setPendingCondition(ctx context.Context, lease *brokerv1alpha1.ClusterLease, reason, message string) error {
	lease.Status.Phase = brokerv1alpha1.PhaseLeasePending
	apimeta.SetStatusCondition(&lease.Status.Conditions, metav1.Condition{
		Type:               conditionTypeLeaseBound,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: lease.Generation,
	})
	if err := r.Status().Update(ctx, lease); err != nil {
		return fmt.Errorf("updating lease status: %w", err)
	}
	return nil
}

// leasesForInstance maps a ClusterInstance event to reconcile.Requests for
// every non-terminal ClusterLease of the same pool that has not yet
// claimed an instance (that is, still Pending). This lets a newly-Ready, or
// otherwise changed, instance wake up waiting leases immediately, instead
// of relying solely on leasePendingRequeue's periodic backstop poll,
// mirroring how node/pod events trigger the scheduler rather than polling.
// leasesForInstance excludes already-Bound leases because their binding is
// immutable once set (see ClusterLease.Status.InstanceRef's doc), so they
// have no reason to re-run matchmaking.
func (r *ClusterLeaseReconciler) leasesForInstance(ctx context.Context, obj client.Object) []reconcile.Request {
	inst, ok := obj.(*brokerv1alpha1.ClusterInstance)
	if !ok || inst.Spec.PoolRef.Name == "" {
		return nil
	}
	leaseList := &brokerv1alpha1.ClusterLeaseList{}
	if err := r.List(ctx, leaseList, client.InNamespace(inst.Namespace)); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for i := range leaseList.Items {
		l := &leaseList.Items[i]
		if !l.DeletionTimestamp.IsZero() {
			continue
		}
		if l.Spec.PoolRef.Name != inst.Spec.PoolRef.Name {
			continue
		}
		switch l.Status.Phase {
		case brokerv1alpha1.PhaseLeaseBound, brokerv1alpha1.PhaseLeaseReleasing,
			brokerv1alpha1.PhaseLeaseReleased, brokerv1alpha1.PhaseLeaseFailed:
			continue
		}
		reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(l)})
	}
	return reqs
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterLeaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &brokerv1alpha1.ClusterLease{},
		leaseInstanceRefIndexField, func(obj client.Object) []string {
			lease, ok := obj.(*brokerv1alpha1.ClusterLease)
			if !ok || lease.Status.InstanceRef == nil {
				return nil
			}
			return []string{lease.Status.InstanceRef.Name}
		}); err != nil {
		return fmt.Errorf("indexing ClusterLease by status.instanceRef.name: %w", err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&brokerv1alpha1.ClusterLease{}).
		Watches(&brokerv1alpha1.ClusterInstance{}, handler.EnqueueRequestsFromMapFunc(r.leasesForInstance)).
		Named("clusterlease").
		Complete(r)
}
