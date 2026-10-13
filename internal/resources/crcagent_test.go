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

package resources

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation"
)

func TestCRCAgentJobNameIsVMIScopedAndValid(t *testing.T) {
	instanceName := strings.Repeat("a", 63)
	first := CRCAgentJobName(instanceName, "first-vmi")
	second := CRCAgentJobName(instanceName, "second-vmi")
	if first == second {
		t.Fatalf("job names for different VMIs must differ: %q", first)
	}

	for _, name := range []string{
		instanceName,                   // longest instance name
		strings.Repeat("a", 39) + ".b", // cut ends at a dot
		strings.Repeat("a.", 21),       // dotted subdomain, cut ends at a dot
	} {
		jobName := CRCAgentJobName(name, "vmi-uid")
		if len(jobName) > 63 {
			t.Fatalf("job name %q for %q: length = %d, want at most 63", jobName, name, len(jobName))
		}
		if errs := validation.IsDNS1123Subdomain(jobName); len(errs) != 0 {
			t.Errorf("job name %q for %q is not a valid DNS subdomain: %v", jobName, name, errs)
		}
	}
}
