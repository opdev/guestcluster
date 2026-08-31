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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// deleteIfExists deletes obj (which must have Name/Namespace set) if it
// exists. It tolerates NotFound, both from the delete call itself and,
// implicitly, from the case where obj was already gone.
//
// The various teardown paths (CRC and HyperShift backing objects)
// previously each open-coded a Get-then-conditionally-Delete block only to
// decide whether to log. deleteIfExists replaces that pattern: Delete is
// idempotent, so callers do not need an existence check first.
//
// label is a short, human-readable description of obj, used in the log
// message and wrapped errors (for example, "HostedCluster", "crc-agent
// Job").
func (r *ClusterInstanceReconciler) deleteIfExists(ctx context.Context, obj client.Object, label string, opts ...client.DeleteOption) error {
	log := logf.FromContext(ctx)
	key := client.ObjectKeyFromObject(obj)

	err := r.Delete(ctx, obj, opts...)
	if err == nil {
		log.Info("deleted "+label+" for teardown", "name", key.Name, "namespace", key.Namespace)
		return nil
	}
	if apierrors.IsNotFound(err) {
		return nil
	}
	return fmt.Errorf("deleting %s %s/%s for teardown: %w", label, key.Namespace, key.Name, err)
}

// upsertSecret gets or creates desired. If changed(existing) reports drift,
// upsertSecret updates the existing Secret's Data, Type, and Labels to
// match desired.
//
// Several near-identical "materialize a Secret and keep it in sync" call
// sites use upsertSecret: the pull-secret copy, the HCP worker SSH key
// copy, the KAS serving certificate, and the canonical kubeconfig secret.
// These call sites previously each open-coded the same
// Get/Create/Update-on-drift skeleton.
func (r *ClusterInstanceReconciler) upsertSecret(ctx context.Context, desired *corev1.Secret, changed func(existing *corev1.Secret) bool) error {
	existing := &corev1.Secret{}
	key := types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}
	if err := r.Get(ctx, key, existing); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, desired); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("creating secret %s/%s: %w", key.Namespace, key.Name, err)
		}
		return nil
	} else if err != nil {
		return fmt.Errorf("getting secret %s/%s: %w", key.Namespace, key.Name, err)
	} else if changed(existing) {
		existing.Type = desired.Type
		existing.Data = desired.Data
		existing.Labels = desired.Labels
		existing.OwnerReferences = desired.OwnerReferences
		if err := r.Update(ctx, existing); err != nil {
			return fmt.Errorf("updating secret %s/%s: %w", key.Namespace, key.Name, err)
		}
	}
	return nil
}
