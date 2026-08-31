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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
)

// TestDesiredReplicas is a plain testing.T table test (not ginkgo/envtest --
// desiredReplicas is a pure function of ClusterInstance.Spec, so it needs no
// live cluster) covering the topology=hcp worker replica default/override
// behavior now that hcp-1/hcp-n no longer exist as separate topologies.
func TestDesiredReplicas(t *testing.T) {
	int32Ptr := func(v int32) *int32 { return &v }

	cases := []struct {
		name     string
		replicas *int32
		want     int32
	}{
		{name: "unset defaults to 1", replicas: nil, want: 1},
		{name: "explicit 1", replicas: int32Ptr(1), want: 1},
		{name: "explicit N", replicas: int32Ptr(3), want: 3},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			instance := &brokerv1alpha1.ClusterInstance{
				ObjectMeta: metav1.ObjectMeta{Name: "hcp-pool-99h8d"},
				Spec: brokerv1alpha1.ClusterInstanceSpec{
					Type: brokerv1alpha1.TopologyHCP,
					Template: brokerv1alpha1.ClusterTemplate{
						NodePoolReplicas: tc.replicas,
					},
				},
			}
			if got := desiredReplicas(instance); got != tc.want {
				t.Errorf("desiredReplicas() = %d, want %d", got, tc.want)
			}
		})
	}
}
