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
	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
)

// Shared literal values used across this package's unit and verification
// tests, pulled out as named constants (instead of repeated string/number
// literals) so the reconcile logic under test always sees identical fixture
// data no matter which spec file constructs it.
const (
	testNamespace  = "default"
	testOCPVersion = "4.16.0"
	testMemory     = "16Gi"
)

// allVerificationTopologies is the set of topologies the binding-model
// verification matrix (see clusterlease_verification_test.go and
// clusterpool_verification_test.go) is run against. The pool/lease
// reconcile logic under test (matchmaking, double-claim prevention, TTL
// release, scale-up/down, capacity accounting, the stability window) never
// branches on ClusterPool/ClusterInstance Spec.Type -- it only looks at
// Status.Phase, labels, and lease claims. Registering the identical specs
// once per topology both proves that parity today and guards against any
// future topology-specific branching silently creeping into what's meant
// to stay shared code paths.
var allVerificationTopologies = []brokerv1alpha1.ClusterTopology{
	brokerv1alpha1.TopologyCRC,
	brokerv1alpha1.TopologyHCP,
}

// verificationTemplateFor returns a minimal, schema-valid ClusterTemplate
// for the given topology, using topology-appropriate fields so each fixture
// at least resembles what a real ClusterPool/ClusterInstance of that
// topology would carry. None of the binding-model verification specs ever
// invoke ClusterInstanceReconciler's real provisioning (reconcileCRC /
// reconcileHyperShift) or ClusterPoolReconciler's ensureCRCBundle (CRC-only,
// and gated on CRCVersion being set, which is deliberately omitted here),
// so nothing in the returned template needs to resolve to real backing
// infrastructure.
func verificationTemplateFor(topology brokerv1alpha1.ClusterTopology) brokerv1alpha1.ClusterTemplate {
	switch topology {
	case brokerv1alpha1.TopologyHCP:
		return brokerv1alpha1.ClusterTemplate{
			OCPVersion:   testOCPVersion,
			ReleaseImage: "quay.io/openshift-release-dev/ocp-release:4.16.0-x86_64",
			Memory:       "8Gi",
			Cores:        2,
		}
	default: // TopologyCRC
		return brokerv1alpha1.ClusterTemplate{
			OCPVersion: testOCPVersion,
			Memory:     testMemory,
			Cores:      4,
		}
	}
}
