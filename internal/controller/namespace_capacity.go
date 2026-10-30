package controller

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	brokerv1alpha1 "github.com/opdev/guestcluster/api/v1alpha1"
	"github.com/opdev/guestcluster/internal/resources"
)

const (
	clusterInstanceCountQuota    = "count/clusterinstances.guestcluster.opdev.io"
	clusterInstanceCPUQuota      = "requests.guestcluster.opdev.io/cpu"
	clusterInstanceMemoryQuota   = "requests.guestcluster.opdev.io/memory"
	clusterInstanceStorageQuota  = "requests.guestcluster.opdev.io/storage"
	namespaceResourceQuotaReason = "NamespaceResourceQuotaExceeded"
)

type namespaceResourceQuotaDimension struct {
	quotaKey corev1.ResourceName
	label    string
}

var namespaceResourceQuotaDimensions = [...]namespaceResourceQuotaDimension{
	{quotaKey: corev1.ResourceName(clusterInstanceCPUQuota), label: "CPU"},
	{quotaKey: corev1.ResourceName(clusterInstanceMemoryQuota), label: "memory"},
	{quotaKey: corev1.ResourceName(clusterInstanceStorageQuota), label: "storage"},
}

type namespaceInstanceCapacity struct {
	total   int64
	limit   int64
	limited bool
}

func (c namespaceInstanceCapacity) atLimit() bool {
	return c.limited && c.total >= c.limit
}

func namespaceClusterInstanceLimit(quotas *corev1.ResourceQuotaList) (int64, bool) {
	var limit int64
	limited := false
	resourceName := corev1.ResourceName(clusterInstanceCountQuota)
	for i := range quotas.Items {
		quotaLimit, found := quotas.Items[i].Spec.Hard[resourceName]
		if !found {
			continue
		}
		value := quotaLimit.Value()
		if !limited || value < limit {
			limit = value
			limited = true
		}
	}
	return limit, limited
}

// namespaceClusterInstanceResourceLimits returns the lowest hard limit for
// each Guestcluster resource across all ResourceQuotas in a namespace. The
// values represent whole CPU cores or whole bytes, depending on the key.
func namespaceClusterInstanceResourceLimits(quotas *corev1.ResourceQuotaList) (corev1.ResourceList, error) {
	limits := corev1.ResourceList{}
	for i := range quotas.Items {
		quota := &quotas.Items[i]
		for _, dimension := range namespaceResourceQuotaDimensions {
			limit, found := quota.Spec.Hard[dimension.quotaKey]
			if !found {
				continue
			}
			if value, wholeUnit := limit.AsInt64(); !wholeUnit || value < 0 {
				return nil, fmt.Errorf("ResourceQuota %s/%s has invalid %s limit %q: expected a non-negative whole number of cores or bytes",
					quota.Namespace, quota.Name, dimension.label, limit.String())
			}
			current, found := limits[dimension.quotaKey]
			if !found || limit.Cmp(current) < 0 {
				limits[dimension.quotaKey] = limit.DeepCopy()
			}
		}
	}
	return limits, nil
}

// clusterInstanceResourceUsage estimates the resources allocated to one
// ClusterInstance from its copied template. CRC is one VM. HCP accounts for
// worker NodePool replicas; its separate control-plane overhead is not
// represented by ClusterTemplate and is intentionally excluded.
func clusterInstanceResourceUsage(instance *brokerv1alpha1.ClusterInstance) (corev1.ResourceList, error) {
	template := instance.Spec.Template
	replicas := int64(1)
	switch instance.Spec.Type {
	case brokerv1alpha1.TopologyCRC:
	case brokerv1alpha1.TopologyHCP:
		replicas = int64(desiredReplicas(instance))
	default:
		return nil, fmt.Errorf("ClusterInstance %s/%s has unsupported topology %q for resource accounting",
			instance.Namespace, instance.Name, instance.Spec.Type)
	}

	if template.Cores < 1 {
		return nil, fmt.Errorf("ClusterInstance %s/%s template.cores must be at least 1 for resource accounting", instance.Namespace, instance.Name)
	}
	cpu := resource.NewQuantity(int64(template.Cores)*replicas, resource.DecimalSI)

	memory, err := parseWholePositiveQuantity(template.Memory, "template.memory")
	if err != nil {
		return nil, fmt.Errorf("ClusterInstance %s/%s: %w", instance.Namespace, instance.Name, err)
	}
	memory, err = multiplyWholeQuantity(memory, replicas, "template.memory")
	if err != nil {
		return nil, fmt.Errorf("ClusterInstance %s/%s: %w", instance.Namespace, instance.Name, err)
	}

	rootVolumeSize := template.RootVolumeSize
	if rootVolumeSize == "" {
		if instance.Spec.Type == brokerv1alpha1.TopologyCRC {
			rootVolumeSize = resources.DefaultCRCRootVolumeSize
		} else {
			rootVolumeSize = resources.DefaultNodePoolRootVolumeSize
		}
	}
	storage, err := parseWholePositiveQuantity(rootVolumeSize, "template.rootVolumeSize")
	if err != nil {
		return nil, fmt.Errorf("ClusterInstance %s/%s: %w", instance.Namespace, instance.Name, err)
	}
	storage, err = multiplyWholeQuantity(storage, replicas, "template.rootVolumeSize")
	if err != nil {
		return nil, fmt.Errorf("ClusterInstance %s/%s: %w", instance.Namespace, instance.Name, err)
	}

	return corev1.ResourceList{
		corev1.ResourceName(clusterInstanceCPUQuota):     *cpu,
		corev1.ResourceName(clusterInstanceMemoryQuota):  memory,
		corev1.ResourceName(clusterInstanceStorageQuota): storage,
	}, nil
}

func parseWholePositiveQuantity(value, field string) (resource.Quantity, error) {
	quantity, err := resource.ParseQuantity(value)
	if err != nil {
		return resource.Quantity{}, fmt.Errorf("%s is not a valid Kubernetes quantity: %w", field, err)
	}
	bytes, wholeUnit := quantity.AsInt64()
	if !wholeUnit || bytes <= 0 {
		return resource.Quantity{}, fmt.Errorf("%s must be a positive whole number of bytes", field)
	}
	return quantity, nil
}

func multiplyWholeQuantity(quantity resource.Quantity, multiplier int64, field string) (resource.Quantity, error) {
	if multiplier < 1 {
		return resource.Quantity{}, fmt.Errorf("%s multiplier must be at least 1", field)
	}
	quantity = quantity.DeepCopy()
	if !quantity.Mul(multiplier) {
		return resource.Quantity{}, fmt.Errorf("%s multiplied by %d is outside the supported quantity range", field, multiplier)
	}
	if _, wholeUnit := quantity.AsInt64(); !wholeUnit {
		return resource.Quantity{}, fmt.Errorf("%s multiplied by %d is not a whole number of bytes", field, multiplier)
	}
	return quantity, nil
}

type namespaceResourceQuotaViolation struct {
	dimension namespaceResourceQuotaDimension
	used      resource.Quantity
	requested resource.Quantity
	limit     resource.Quantity
}

func (v *namespaceResourceQuotaViolation) message() string {
	return fmt.Sprintf("namespace Guestcluster %s quota exceeded: existing allocations request %s, proposed ClusterInstance requests %s, limit is %s",
		v.dimension.label, v.used.String(), v.requested.String(), v.limit.String())
}

func namespaceResourceQuotaCondition(generation int64, violation *namespaceResourceQuotaViolation) metav1.Condition {
	return metav1.Condition{
		Type:               conditionTypeCapacityAvailable,
		Status:             metav1.ConditionFalse,
		Reason:             namespaceResourceQuotaReason,
		Message:            violation.message(),
		ObservedGeneration: generation,
	}
}

// namespaceClusterInstanceResourceViolation checks whether adding candidate
// would exceed any configured Guestcluster resource limit. Pool-created
// instances reserve resources when their API objects are created. Standalone
// instances reserve resources after provisioning authorization; unstarted
// standalone objects do not consume the budget until they are considered as
// the candidate. Failed and terminating instances remain reservations until
// deletion completes.
func namespaceClusterInstanceResourceViolation(instances []brokerv1alpha1.ClusterInstance, limits corev1.ResourceList, candidate *brokerv1alpha1.ClusterInstance) (*namespaceResourceQuotaViolation, error) {
	if len(limits) == 0 {
		return nil, nil
	}
	if candidate == nil {
		return nil, fmt.Errorf("candidate ClusterInstance is required for resource quota evaluation")
	}

	used := corev1.ResourceList{}
	for i := range instances {
		instance := &instances[i]
		if instance.Namespace != candidate.Namespace {
			continue
		}
		if sameClusterInstanceIdentity(instance, candidate) {
			// The candidate is added once below, even when its pool-linked API
			// object is already present in the live namespace list.
			continue
		}
		reservesResources := clusterInstanceReservesResources(instance)
		if !reservesResources {
			continue
		}
		allocation, err := clusterInstanceResourceUsage(instance)
		if err != nil {
			return nil, err
		}
		addClusterInstanceResourceUsage(used, allocation)
	}

	requested, err := clusterInstanceResourceUsage(candidate)
	if err != nil {
		return nil, err
	}
	for _, dimension := range namespaceResourceQuotaDimensions {
		limit, limited := limits[dimension.quotaKey]
		current := used[dimension.quotaKey]
		proposed := requested[dimension.quotaKey]
		total := current.DeepCopy()
		total.Add(proposed)
		if limited && total.Cmp(limit) > 0 {
			return &namespaceResourceQuotaViolation{
				dimension: dimension,
				used:      current.DeepCopy(),
				requested: proposed.DeepCopy(),
				limit:     limit.DeepCopy(),
			}, nil
		}
	}
	return nil, nil
}

// sameClusterInstanceIdentity matches a persisted candidate by namespace and
// name. Synthetic pool candidates have no name and therefore never match a
// listed instance. When both UIDs are available, they must agree so a stale
// candidate cannot hide a different object recreated under the same name.
func sameClusterInstanceIdentity(existing, candidate *brokerv1alpha1.ClusterInstance) bool {
	if existing == nil || candidate == nil || candidate.Namespace == "" || candidate.Name == "" {
		return false
	}
	if existing.Namespace != candidate.Namespace || existing.Name != candidate.Name {
		return false
	}
	return existing.UID == "" || candidate.UID == "" || existing.UID == candidate.UID
}

func (r *ClusterInstanceReconciler) readNamespaceResourceQuotaViolation(ctx context.Context, candidate *brokerv1alpha1.ClusterInstance) (*namespaceResourceQuotaViolation, error) {
	log := logf.FromContext(ctx).WithValues(
		"namespace", candidate.Namespace,
		"clusterInstance", candidate.Name,
		"topology", candidate.Spec.Type,
		"pool", candidate.Spec.PoolRef.Name,
	)
	reader := r.platformReader()
	quotas := &corev1.ResourceQuotaList{}
	if err := reader.List(ctx, quotas, client.InNamespace(candidate.Namespace)); err != nil {
		log.Error(err, "failed to list namespace ResourceQuotas for resource evaluation")
		return nil, fmt.Errorf("listing ResourceQuotas in namespace %s: %w", candidate.Namespace, err)
	}
	limits, err := namespaceClusterInstanceResourceLimits(quotas)
	if err != nil {
		log.Error(err, "failed to calculate effective namespace Guestcluster resource limits")
		return nil, err
	}
	if len(limits) == 0 {
		return nil, nil
	}

	instances := &brokerv1alpha1.ClusterInstanceList{}
	if err := reader.List(ctx, instances, client.InNamespace(candidate.Namespace)); err != nil {
		log.Error(err, "failed to list namespace ClusterInstances for resource evaluation")
		return nil, fmt.Errorf("listing ClusterInstances in namespace %s: %w", candidate.Namespace, err)
	}
	return namespaceClusterInstanceResourceViolation(instances.Items, limits, candidate)
}

func clusterInstanceReservesResources(instance *brokerv1alpha1.ClusterInstance) bool {
	if !instance.DeletionTimestamp.IsZero() || instance.Spec.PoolRef.Name != "" || instance.Status.Provisioning != nil {
		return true
	}
	switch instance.Status.Phase {
	case brokerv1alpha1.PhaseReady, brokerv1alpha1.PhaseFailed, brokerv1alpha1.PhaseTerminating:
		return true
	default:
		return false
	}
}

func addClusterInstanceResourceUsage(total, allocation corev1.ResourceList) {
	for _, dimension := range namespaceResourceQuotaDimensions {
		amount, found := allocation[dimension.quotaKey]
		if !found {
			continue
		}
		current := total[dimension.quotaKey].DeepCopy()
		current.Add(amount)
		total[dimension.quotaKey] = current
	}
}

func namespaceLimitCondition(generation int64, capacity namespaceInstanceCapacity) metav1.Condition {
	message := "ResourceQuota blocks creation of another ClusterInstance in this namespace"
	if capacity.limited {
		message = fmt.Sprintf("namespace ClusterInstance limit reached: %d of %d instances exist", capacity.total, capacity.limit)
	}
	return metav1.Condition{
		Type:               conditionTypeCapacityAvailable,
		Status:             metav1.ConditionFalse,
		Reason:             "NamespaceAtCapacity",
		Message:            message,
		ObservedGeneration: generation,
	}
}

func isClusterInstanceQuotaExceeded(err error) bool {
	if !apierrors.IsForbidden(err) {
		return false
	}
	message := err.Error()
	return strings.Contains(message, "exceeded quota:") && strings.Contains(message, clusterInstanceCountQuota)
}

func poolRequestsForNamespace(ctx context.Context, c client.Client, namespace string) []reconcile.Request {
	list := &brokerv1alpha1.ClusterPoolList{}
	if err := c.List(ctx, list, client.InNamespace(namespace)); err != nil {
		logf.FromContext(ctx).Error(err, "listing pools for namespace capacity")
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for i := range list.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&list.Items[i])})
	}
	return requests
}
