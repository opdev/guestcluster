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

// deleteIfExists requests deletion of obj (which must have Name/Namespace set)
// if it exists. The returned bool is true when the object still exists after
// the delete request, usually because another finalizer is still running.
// NotFound is treated as a completed deletion.
//
// The various teardown paths (CRC and HyperShift backing objects)
// previously each open-coded a Get-then-conditionally-Delete block only to
// decide whether to log. deleteIfExists replaces that pattern: Delete is
// idempotent, so callers do not need an existence check first.
//
// label is a short, human-readable description of obj, used in the log
// message and wrapped errors (for example, "HostedCluster", "crc-agent
// Job").
func (r *ClusterInstanceReconciler) deleteIfExists(ctx context.Context, obj client.Object, label string, opts ...client.DeleteOption) (bool, error) {
	log := logf.FromContext(ctx)
	key := client.ObjectKeyFromObject(obj)

	err := r.Delete(ctx, obj, opts...)
	if err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("deleting %s %s/%s for teardown: %w", label, key.Namespace, key.Name, err)
	}
	if apierrors.IsNotFound(err) {
		return false, nil
	}

	if err := r.platformReader().Get(ctx, key, obj); err == nil {
		log.Info("waiting for "+label+" teardown", "name", key.Name, "namespace", key.Namespace)
		return true, nil
	} else if !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("checking %s %s/%s after delete: %w", label, key.Namespace, key.Name, err)
	}

	log.Info("deleted "+label+" for teardown", "name", key.Name, "namespace", key.Namespace)
	return false, nil
}

// upsertSecret gets or creates desired. Optional checks run before an existing
// Secret is reused or updated. If changed reports drift, the Secret is updated.
//
// Several near-identical "materialize a Secret and keep it in sync" call
// sites use upsertSecret: the pull-secret copy, the HCP worker SSH key
// copy, and the canonical kubeconfig secret.
// These call sites previously each open-coded the same
// Get/Create/Update-on-drift skeleton.
func (r *ClusterInstanceReconciler) upsertSecret(ctx context.Context, desired *corev1.Secret, changed func(existing *corev1.Secret) bool, verify ...func(*corev1.Secret) error) error {
	existing := &corev1.Secret{}
	key := types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}
	if err := r.Get(ctx, key, existing); apierrors.IsNotFound(err) {
		if createErr := r.Create(ctx, desired); createErr == nil {
			return nil
		} else if !apierrors.IsAlreadyExists(createErr) {
			return fmt.Errorf("creating secret %s/%s: %w", key.Namespace, key.Name, createErr)
		}
		if err := r.Get(ctx, key, existing); err != nil {
			return fmt.Errorf("getting concurrently created secret %s/%s: %w", key.Namespace, key.Name, err)
		}
	} else if err != nil {
		return fmt.Errorf("getting secret %s/%s: %w", key.Namespace, key.Name, err)
	}
	for _, check := range verify {
		if err := check(existing); err != nil {
			return err
		}
	}
	if changed(existing) {
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
