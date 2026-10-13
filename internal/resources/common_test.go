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

func TestHashSuffixedNameStaysWithinDNSLimit(t *testing.T) {
	const suffix = "-suffix-"
	for _, tc := range []struct {
		name   string
		prefix string
	}{
		{"short prefix", "instance"},
		{"long prefix", strings.Repeat("a", 80)},
		{"cut ends at a dot", strings.Repeat("a", 42) + ".b"},
		{"cut ends at a hyphen", strings.Repeat("a", 42) + "-b"},
		{"dotted subdomain", strings.Repeat("ab.", 20)},
	} {
		got := hashSuffixedName(tc.prefix, suffix, "hash-input")
		if len(got) > dns1123LabelMaxLength {
			t.Errorf("%s: length = %d, want at most %d (%q)", tc.name, len(got), dns1123LabelMaxLength, got)
		}
		if errs := validation.IsDNS1123Subdomain(got); len(errs) != 0 {
			t.Errorf("%s: %q is not a valid DNS subdomain: %v", tc.name, got, errs)
		}
	}
	if hashSuffixedName("prefix", suffix, "one") == hashSuffixedName("prefix", suffix, "two") {
		t.Error("names for different hash inputs must differ")
	}
	first := hashSuffixedName("prefix", suffix, "one")
	second := hashSuffixedName("prefix", suffix, "one")
	if first != second {
		t.Error("name must be deterministic")
	}
}
