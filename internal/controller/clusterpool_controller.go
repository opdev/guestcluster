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
	"strconv"
	"strings"
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

// ClusterPoolReconciler reconciles a ClusterPool object. It maintains the
// pool's budget along three independent floors, always bounded by
// Spec.MaxSize. This is analogous to a cluster autoscaler's node-group
// min/max size plus over-provisioning ("pause pod") pattern:
//   - Spec.MinSize: a stable, total-count floor (any non-terminal phase:
//     Provisioning, Ready, or claimed-by-a-lease), i.e. an autoscaler-style
//     minimum node-group size, independent of current demand.
//   - Spec.WarmSpares: a spare-capacity floor measured against Ready,
//     UNCLAIMED instances, kept ahead of demand (autoscaler
//     over-provisioning) so a ClusterLease can bind instantly instead of
//     waiting for a full provision.
//   - Pending ClusterLease demand: provisions on-demand for leases that
//     outnumber what MinSize/WarmSpares alone would produce. This is what
//     makes MinSize=0, WarmSpares=0 a valid pure-on-demand configuration
//     instead of a deadlock, since ClusterLeaseReconciler never provisions.
//
// ClusterPoolReconciler always determines "claimed" from
// ClusterLease.Status.InstanceRef, the single source of truth for the
// lease-instance binding (see clusterlease_types.go and
// clusterlease_controller.go). It never derives "claimed" from anything
// stored on the ClusterInstance side, which only carries a read-only
// derived projection for observability. This controller never binds or
// mutates a lease itself; that responsibility belongs entirely to
// ClusterLeaseReconciler. It also reports a CapacityAvailable status
// condition that summarizes whether the floors are structurally reachable
// given MaxSize, and whether the pool is currently saturated.
type ClusterPoolReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// APIReader is an uncached client (see manager.Manager.GetAPIReader) that
	// this reconciler's capacity accounting uses specifically for the
	// ClusterInstance/ClusterLease List() calls it depends on. The regular
	// (cached) client.Client above is backed by an informer whose local
	// store only reflects a given write once its own watch round-trip
	// completes, asynchronously relative to the write itself. An unrelated
	// event (for example, a ClusterPool spec/status update, not just the
	// Owns(ClusterInstance) watch) can re-trigger Reconcile before that
	// round-trip finishes. In that case a cached List() would still miss an
	// instance this reconciler itself just created or deleted moments
	// earlier, and Reconcile would double-act on the same floor deficit.
	// Testing verified this empirically: a single warmSpares increase from
	// 1 to 2 produced two new ClusterInstances instead of one. Reading
	// these two lists live from the API server closes that race outright.
	// ClusterPool reconciles are infrequent enough that the extra apiserver
	// round-trip cost is immaterial.
	APIReader client.Reader
}

const (
	// conditionTypeCapacityAvailable summarizes whether a ClusterPool's
	// MinSize/WarmSpares floors are reachable given MaxSize, and whether
	// current demand can be satisfied within MaxSize right now.
	conditionTypeCapacityAvailable = "CapacityAvailable"

	// scaleDownStabilityPeriod mirrors cluster-autoscaler's
	// --scale-down-unneeded-time: a Ready, unclaimed instance must stay idle
	// for at least this long before it becomes eligible for trimming as
	// excess. This is defense-in-depth against thrashing a spare that is
	// about to be claimed. The single-write, assume-immediately binding
	// model (see clusterlease_controller.go) already closes most such races
	// structurally, but a short grace period costs nothing and protects
	// against races not yet anticipated (for example, an unrelated
	// conflict/backoff that delays a lease's own reconcile between finding
	// a candidate and committing the bind).
	scaleDownStabilityPeriod = 2 * time.Minute
)

// +kubebuilder:rbac:groups=guestcluster.opdev.io,resources=clusterpools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=guestcluster.opdev.io,resources=clusterpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=guestcluster.opdev.io,resources=clusterpools/finalizers,verbs=update
// +kubebuilder:rbac:groups=guestcluster.opdev.io,resources=clusterinstances,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=guestcluster.opdev.io,resources=clusterleases,verbs=get;list;watch
// +kubebuilder:rbac:groups=guestcluster.opdev.io,resources=crcbundles,verbs=get;list;watch;create

// Reconcile drives ClusterPool.Status to reflect the current supply of
// ClusterInstances. It tops up MinSize, WarmSpares, and pending demand
// (bounded by MaxSize) by creating new ClusterInstance objects owned by
// this pool. It scales back down when there is excess beyond all three
// floors, for example when excess is left over from a since-fixed
// over-provisioning bug, or because a previously-leased instance was
// released and is no longer needed.
func (r *ClusterPoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	pool := &brokerv1alpha1.ClusterPool{}
	if err := r.Get(ctx, req.NamespacedName, pool); err != nil {
		if apierrors.IsNotFound(err) {
			log.V(1).Info("ClusterPool deleted, nothing to reconcile")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("getting ClusterPool: %w", err)
	}

	if pool.Spec.Type == brokerv1alpha1.TopologyCRC && pool.Spec.Template.CRCVersion != "" {
		if err := r.ensureCRCBundle(ctx, pool); err != nil {
			return ctrl.Result{}, fmt.Errorf("ensuring CRCBundle for pool %s: %w", pool.Name, err)
		}
	}

	instanceList := &brokerv1alpha1.ClusterInstanceList{}
	if err := r.APIReader.List(ctx, instanceList,
		client.InNamespace(pool.Namespace),
		client.MatchingLabels(resources.PoolLabels(pool.Name)),
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing ClusterInstances for pool: %w", err)
	}

	// claimed records, by ClusterInstance name, every instance named by some
	// non-deleted ClusterLease's Status.InstanceRef, the single source of
	// truth for the lease-instance binding (see clusterlease_types.go). This
	// is the ONLY place that determines "is this instance in use";
	// ClusterInstance.Status.LeaseRef is a read-only derived projection and
	// Reconcile never consults it for accounting decisions.
	// ClusterLeaseReconciler commits a claim with a single atomic write to
	// the lease and never touches the instance, so Reconcile considers an
	// instance claimed the moment its lease names it. It is "assumed"
	// immediately, exactly like the Kubernetes scheduler's binding cache, so
	// there is no window where the claim exists on one side but not the
	// other for this accounting to observe.
	//
	// pendingDemand counts, in the same pass, non-terminal ClusterLeases
	// that target this pool (same PoolRef) and have not yet claimed an
	// instance. ClusterLeaseReconciler is pure demand-side matchmaking: it
	// never provisions instances itself (see clusterlease_controller.go).
	// Without pendingDemand, a pool with MinSize=0 and WarmSpares=0 (pure
	// on-demand) would never create an instance for a Pending lease,
	// because `need` below would always be 0 with no warm floor to
	// maintain. pendingDemand makes MinSize=0, WarmSpares=0 a valid,
	// working configuration instead of a deadlock.
	leaseList := &brokerv1alpha1.ClusterLeaseList{}
	if err := r.APIReader.List(ctx, leaseList, client.InNamespace(pool.Namespace)); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing ClusterLeases: %w", err)
	}
	claimed := make(map[string]bool)
	var pendingDemand int32
	for i := range leaseList.Items {
		l := &leaseList.Items[i]
		if !l.DeletionTimestamp.IsZero() {
			continue
		}
		if l.Status.InstanceRef != nil {
			claimed[l.Status.InstanceRef.Name] = true
		}
		if l.Spec.PoolRef.Name != pool.Name {
			continue
		}
		switch l.Status.Phase {
		case brokerv1alpha1.PhaseLeaseBound, brokerv1alpha1.PhaseLeaseReleasing,
			brokerv1alpha1.PhaseLeaseReleased, brokerv1alpha1.PhaseLeaseFailed:
			// Already satisfied, being torn down, or needing manual
			// intervention: none of these represent supply demand.
		default:
			// "" (unset) or PhaseLeasePending: this lease is actively
			// waiting for a free instance.
			pendingDemand++
		}
	}

	var total, available, leasedCount, pending int32
	var availableInstances []*brokerv1alpha1.ClusterInstance
	for i := range instanceList.Items {
		inst := &instanceList.Items[i]
		if !inst.DeletionTimestamp.IsZero() {
			// Being torn down; does not count against the budget.
			continue
		}
		total++
		switch inst.Status.Phase {
		case brokerv1alpha1.PhaseReady:
			if claimed[inst.Name] {
				leasedCount++
			} else {
				available++
				availableInstances = append(availableInstances, inst)
			}
		case brokerv1alpha1.PhaseFailed:
			// Reconcile does not count a Failed instance as pending,
			// because a Failed instance does not self-heal into Ready
			// without manual intervention (recycle via deletion). Counting
			// it as pending would suppress topping up a real replacement.
			// A Failed instance still counts toward `total` (room), so a
			// pile-up of Failed instances still cannot cause unbounded
			// creation past MaxSize.
		default:
			// Everything else, including Provisioning and the brief
			// zero-value "" phase a just-Create()'d instance has before
			// ClusterInstanceReconciler's own first reconcile sets it to
			// Provisioning, is already-committed supply that will become
			// Ready on its own without Reconcile creating a new instance
			// for it. Counting only explicitly-known in-progress phases
			// here missed that "" window: this controller's own immediate
			// self-requeue after each Create() reliably outraces
			// ClusterInstanceReconciler's first pass at the brand new
			// object. As a result, `need` never shrank, and Reconcile
			// created instances every cycle right up to MaxSize instead of
			// stopping once enough instances were already in flight to
			// satisfy the active floor.
			pending++
		}
	}

	pool.Status.TotalInstances = total
	pool.Status.AvailableInstances = available
	pool.Status.LeasedInstances = leasedCount

	// "need" combines three independent floors, each measured against the
	// supply that can satisfy it. See the pending case above for why
	// still-Provisioning instances count as already-committed supply:
	//   - minSizeNeed: MinSize is a stable, total-count floor.
	//   - warmSpareNeed: WarmSpares is a spare-capacity floor kept ahead of
	//     demand.
	//   - demandNeed: a Pending lease grabs a Ready+unclaimed instance
	//     (available) or one still provisioning (pending) as soon as that
	//     instance becomes Ready, so only demand beyond that requires NEW
	//     supply.
	// Reconcile creates one instance per reconcile, for thundering-herd
	// avoidance and to keep each Create() error individually retryable,
	// for whichever floor currently drives the largest deficit.
	minSizeNeed := pool.Spec.MinSize - total
	warmSpareNeed := pool.Spec.WarmSpares - (available + pending)
	demandNeed := pendingDemand - (available + pending)
	need := minSizeNeed
	if warmSpareNeed > need {
		need = warmSpareNeed
	}
	if demandNeed > need {
		need = demandNeed
	}
	room := pool.Spec.MaxSize - total

	log.V(1).Info("evaluated pool capacity",
		"pool", pool.Name, "total", total, "available", available, "pending", pending, "leasedCount", leasedCount,
		"pendingDemand", pendingDemand, "minSizeNeed", minSizeNeed, "warmSpareNeed", warmSpareNeed,
		"demandNeed", demandNeed, "need", need, "room", room)

	// CapacityAvailable summarizes, for external consumers, whether the
	// pool's floors are reachable given MaxSize, and whether current demand
	// can be satisfied within it. Reconcile computes it once, before the
	// branches below return early, so whichever Status().Update fires
	// carries it.
	switch {
	case pool.Spec.MaxSize < pool.Spec.MinSize || pool.Spec.MaxSize < pool.Spec.WarmSpares:
		// Structurally unreachable: MaxSize cannot accommodate the
		// configured floors. The CEL validation on ClusterPoolSpec rejects
		// this at admission time, so this branch should only be reachable
		// via a CRD that predates that validation, or an update path that
		// bypasses it. Reconcile reports it here as a belt-and-suspenders
		// safety net.
		apimeta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
			Type:   conditionTypeCapacityAvailable,
			Status: metav1.ConditionFalse,
			Reason: "MaxSizeTooSmall",
			Message: fmt.Sprintf("maxSize (%d) is below minSize (%d) and/or warmSpares (%d): pool cannot reach its configured floors",
				pool.Spec.MaxSize, pool.Spec.MinSize, pool.Spec.WarmSpares),
			ObservedGeneration: pool.Generation,
		})
	case need > 0 && room <= 0:
		// Floors are reachable in principle, but the pool is currently
		// saturated at MaxSize with a real unmet deficit or demand, for
		// example Pending ClusterLeases that cannot be fulfilled until
		// capacity is released. Previously this condition was silent; a
		// lease would sit Pending indefinitely with no pool-side signal
		// explaining why.
		apimeta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
			Type:   conditionTypeCapacityAvailable,
			Status: metav1.ConditionFalse,
			Reason: "AtCapacity",
			Message: fmt.Sprintf("pool is at maxSize (%d); unmet deficit of %d instance(s) (pendingDemand=%d) cannot be satisfied until capacity is released",
				pool.Spec.MaxSize, need, pendingDemand),
			ObservedGeneration: pool.Generation,
		})
	default:
		apimeta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
			Type:               conditionTypeCapacityAvailable,
			Status:             metav1.ConditionTrue,
			Reason:             "CapacitySufficient",
			Message:            "pool has capacity to satisfy minSize/warmSpares and pending lease demand",
			ObservedGeneration: pool.Generation,
		})
	}

	if need > 0 && room > 0 {
		log.Info("creating ClusterInstance to satisfy pool capacity",
			"pool", pool.Name, "total", total, "available", available, "pending", pending, "need", need, "room", room)
		if err := r.createInstance(ctx, pool, instanceList); err != nil {
			return ctrl.Result{}, fmt.Errorf("creating ClusterInstance for pool %s: %w", pool.Name, err)
		}
		// Reconcile does NOT requeue immediately here, because r.List()
		// above reads from the manager's cached client, which syncs
		// asynchronously via watch and is not guaranteed to observe the
		// Create() that just happened. An immediate Requeue would race
		// ahead of that cache sync: if the informer has not yet observed
		// the new ClusterInstance by the next reconcile, Reconcile reads
		// `total` as unchanged and need stays positive, causing a second
		// (or more) instance to be over-created for a single floor
		// deficit. SetupWithManager's Owns(ClusterInstance) watch already
		// guarantees a fresh reconcile once the cache actually reflects the
		// new instance, so no explicit Requeue is needed here.
		if err := r.Status().Update(ctx, pool); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating ClusterPool status: %w", err)
		}
		return ctrl.Result{}, nil
	}

	// Scale back down, one instance per reconcile (same thundering-herd
	// rationale as topping up). Reconcile only ever considers
	// Ready+unclaimed instances here. A still-Provisioning instance left
	// over from a since-fixed over-provisioning bug is left to finish
	// rather than cancelled mid-boot; it becomes a candidate for this same
	// trim once Ready, on a later reconcile, if still in excess by then.
	//
	// An instance is excess only if deleting it would not violate
	// either floor:
	//   - spareFloor (raised above WarmSpares by outstanding pendingDemand,
	//     for the same reason as warmSpareNeed/demandNeed above): without
	//     this floor, a pool with WarmSpares=0 would target the very
	//     instance it just provisioned for a Pending lease as excess the
	//     instant that instance turned Ready, before ClusterLeaseReconciler
	//     got a chance to claim it.
	//   - MinSize, measured against `total` (not `available`): total does
	//     NOT change when a lease claims an instance, so this floor is
	//     immune to the same accounting race.
	// excess is the minimum of what each floor allows trimming.
	spareFloor := pool.Spec.WarmSpares
	if pendingDemand > spareFloor {
		spareFloor = pendingDemand
	}
	excessBySpare := available - spareFloor
	excessByTotal := total - pool.Spec.MinSize
	excess := excessBySpare
	if excessByTotal < excess {
		excess = excessByTotal
	}
	if excess > 0 {
		// Among the excess candidates, only instances that have been
		// sitting Ready-and-unclaimed for at least scaleDownStabilityPeriod
		// are eligible for deletion this reconcile; see
		// scaleDownStabilityPeriod's doc. instanceIdleSince uses the Ready
		// condition's LastTransitionTime. A claimed instance is deleted
		// outright on release rather than ever returning to
		// unclaimed-Ready (see clusterinstance_controller.go), so that
		// timestamp accurately reflects how long this instance has been
		// idle, and not merely when it last became Ready.
		var eligible []*brokerv1alpha1.ClusterInstance
		minRemaining := scaleDownStabilityPeriod
		now := time.Now()
		for _, inst := range availableInstances {
			idleSince := instanceIdleSince(inst)
			age := now.Sub(idleSince)
			if age >= scaleDownStabilityPeriod {
				eligible = append(eligible, inst)
				continue
			}
			if remaining := scaleDownStabilityPeriod - age; remaining < minRemaining {
				minRemaining = remaining
			}
		}

		if len(eligible) == 0 {
			// Nothing has cleared the stability window yet; come back once
			// the soonest candidate will have, instead of busy-looping.
			log.V(1).Info("excess capacity detected but no candidate has cleared the scale-down stability window yet",
				"pool", pool.Name, "available", available, "total", total, "requeueAfter", minRemaining)
			if err := r.Status().Update(ctx, pool); err != nil {
				return ctrl.Result{}, fmt.Errorf("updating ClusterPool status: %w", err)
			}
			return ctrl.Result{RequeueAfter: minRemaining}, nil
		}

		victim := newestInstance(eligible)
		log.Info("deleting excess Ready ClusterInstance to scale pool back down",
			"pool", pool.Name, "instance", victim.Name, "available", available, "total", total,
			"minSize", pool.Spec.MinSize, "warmSpares", pool.Spec.WarmSpares,
			"pendingDemand", pendingDemand, "spareFloor", spareFloor)
		if err := r.Delete(ctx, victim); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("deleting excess ClusterInstance %s for pool %s: %w", victim.Name, pool.Name, err)
		}
		// This is the same read-your-own-write cache race as the top-up
		// path above: an immediate Requeue here could re-evaluate `excess`
		// against a cache that has not yet observed this Delete(),
		// over-trimming a second instance for what was only a deficit of
		// one. Owns(ClusterInstance) guarantees a fresh, cache-consistent
		// reconcile once the deletion is observed.
		if err := r.Status().Update(ctx, pool); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating ClusterPool status: %w", err)
		}
		return ctrl.Result{}, nil
	}

	if err := r.Status().Update(ctx, pool); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating ClusterPool status: %w", err)
	}

	return ctrl.Result{}, nil
}

// instanceIdleSince returns when inst became eligible for the scale-down
// stability window: the Ready condition's LastTransitionTime, if present.
// It falls back to CreationTimestamp for the case where that condition is
// missing; this fallback is defensive only, since markReady always sets
// this condition.
func instanceIdleSince(inst *brokerv1alpha1.ClusterInstance) time.Time {
	for _, c := range inst.Status.Conditions {
		if c.Type == conditionTypeReady {
			return c.LastTransitionTime.Time
		}
	}
	return inst.CreationTimestamp.Time
}

// newestInstance returns the instance with the latest CreationTimestamp
// from candidates (the caller guarantees all candidates are Ready and
// unclaimed). Picking the most recently created instance is an arbitrary
// but deterministic tie-breaker among otherwise-interchangeable instances
// of the same pool and template.
func newestInstance(candidates []*brokerv1alpha1.ClusterInstance) *brokerv1alpha1.ClusterInstance {
	newest := candidates[0]
	for _, c := range candidates[1:] {
		if c.CreationTimestamp.After(newest.CreationTimestamp.Time) {
			newest = c
		}
	}
	return newest
}

// createInstance provisions a single new ClusterInstance owned by pool,
// inheriting Spec.Type and Spec.Template from the pool.
//
// nextInstanceName returns a deterministic name for the next ClusterInstance
// this pool should create: "<pool.Name>-<N>", where N is the smallest
// non-negative integer not already used by an existing instance owned by
// this pool (per instanceList, the same list Reconcile's capacity
// accounting already used to compute `need`).
//
// nextInstanceName replaces metadata.generateName to make createInstance's
// Create() call safely idempotent and retryable. With generateName, if the
// exact same logical "need one more instance" decision is ever acted on
// twice for what is a single deficit, for example a client-side transport
// retry this code cannot observe, each Create() mints a brand new random
// suffix. A retried request then silently creates a second, distinct
// object instead of colliding safely. Testing observed
// this empirically: a single createInstance call (confirmed by log line
// count) left two ClusterInstances persisted server-side. A deterministic
// name makes a retried Create() collide on the same name and fail with
// AlreadyExists instead, which createInstance treats as a success.
func nextInstanceName(pool *brokerv1alpha1.ClusterPool, instanceList *brokerv1alpha1.ClusterInstanceList) string {
	prefix := pool.Name + "-"
	used := make(map[int]bool, len(instanceList.Items))
	for i := range instanceList.Items {
		suffix, ok := strings.CutPrefix(instanceList.Items[i].Name, prefix)
		if !ok {
			continue
		}
		if idx, err := strconv.Atoi(suffix); err == nil {
			used[idx] = true
		}
	}
	idx := 0
	for used[idx] {
		idx++
	}
	return fmt.Sprintf("%s%d", prefix, idx)
}

func (r *ClusterPoolReconciler) createInstance(ctx context.Context, pool *brokerv1alpha1.ClusterPool, instanceList *brokerv1alpha1.ClusterInstanceList) error {
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:      nextInstanceName(pool, instanceList),
			Namespace: pool.Namespace,
			Labels:    resources.PoolLabels(pool.Name),
		},
		Spec: brokerv1alpha1.ClusterInstanceSpec{
			Type:     pool.Spec.Type,
			PoolRef:  corev1.LocalObjectReference{Name: pool.Name},
			Template: pool.Spec.Template,
		},
	}
	if err := controllerutil.SetControllerReference(pool, instance, r.Scheme); err != nil {
		return fmt.Errorf("setting owner reference: %w", err)
	}
	if err := r.Create(ctx, instance); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

// ensureCRCBundle creates the cluster-scoped CRCBundle matching
// pool.Spec.Template.CRCVersion (plus CRCArch) if one does not already
// exist. This lets an admin set crcVersion on a ClusterPool to trigger the
// turnkey bundle-preparation flow (see
// internal/controller/crcbundle_controller.go), with nothing else
// required.
//
// ensureCRCBundle does NOT set an owner reference. CRCBundle is a shared,
// version-keyed cache that many ClusterPools, potentially in different
// namespaces, may reference simultaneously. Kubernetes also disallows a
// namespaced object like this ClusterPool from owning a cluster-scoped
// one. For both reasons, a CRCBundle's lifecycle stays independent of any
// single pool's. If a CRCBundle for this version and arch already exists,
// whether created by this pool or another one, ensureCRCBundle leaves it
// untouched and reuses it as-is.
func (r *ClusterPoolReconciler) ensureCRCBundle(ctx context.Context, pool *brokerv1alpha1.ClusterPool) error {
	tmpl := pool.Spec.Template
	arch := tmpl.CRCArch
	if arch == "" {
		arch = resources.DefaultCRCArch
	}

	name := resources.CRCBundleName(tmpl.CRCVersion, arch)
	existing := &brokerv1alpha1.CRCBundle{}
	err := r.Get(ctx, client.ObjectKey{Name: name}, existing)
	if err == nil {
		return nil // already exists (created by this pool or another); reuse as-is.
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("getting CRCBundle %s: %w", name, err)
	}

	bundle := &brokerv1alpha1.CRCBundle{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: brokerv1alpha1.CRCBundleSpec{
			Version:          tmpl.CRCVersion,
			Arch:             arch,
			StorageClassName: tmpl.StorageClassName,
		},
	}
	if err := r.Create(ctx, bundle); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("creating CRCBundle %s: %w", name, err)
	}
	logf.FromContext(ctx).Info("created CRCBundle for pool", "pool", pool.Name, "crcBundle", name, "version", tmpl.CRCVersion, "arch", arch)
	return nil
}

// poolForLease maps a ClusterLease to a reconcile.Request for the
// ClusterPool it targets (Spec.PoolRef, same namespace). This lets creating
// or updating a lease immediately wake the pool's on-demand-provisioning
// logic, instead of relying solely on the lease controller's own periodic
// requeue.
func (r *ClusterPoolReconciler) poolForLease(_ context.Context, obj client.Object) []reconcile.Request {
	lease, ok := obj.(*brokerv1alpha1.ClusterLease)
	if !ok {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: client.ObjectKey{Namespace: lease.Namespace, Name: lease.Spec.PoolRef.Name},
	}}
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterPoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&brokerv1alpha1.ClusterPool{}).
		Owns(&brokerv1alpha1.ClusterInstance{}).
		Watches(&brokerv1alpha1.ClusterLease{}, handler.EnqueueRequestsFromMapFunc(r.poolForLease)).
		Named("clusterpool").
		Complete(r)
}
