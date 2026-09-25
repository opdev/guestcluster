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

package deployment_test

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"
)

const (
	defaultNamespace = "guestcluster-operator-system"
	managerImage     = "registry.example.test/guestcluster-operator:test"
	crcAgentImage    = "registry.example.test/guestcluster-operator-crc-agent:test"
	undeployTarget   = "undeploy"
)

func TestBuildInstallerRendersDeploymentAndPreservesSources(t *testing.T) {
	repoRoot := repositoryRoot(t)
	kustomize := kustomizePath(t, repoRoot)
	sources := readDeploymentSources(t, repoRoot)
	output := filepath.Join(t.TempDir(), "install.yaml")

	runRenderer(t, repoRoot, nil, "build-installer", kustomize, managerImage, crcAgentImage, output)
	objects := readManifests(t, output)

	deployment := resource(t, objects, "Deployment", "guestcluster-operator-controller-manager")
	if namespace := deployment.GetNamespace(); namespace != defaultNamespace {
		t.Fatalf("deployment namespace = %q, want %q", namespace, defaultNamespace)
	}
	containers, found, err := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
	if err != nil || !found || len(containers) != 1 {
		t.Fatalf("deployment containers = %#v, want one container: %v", containers, err)
	}
	container, ok := containers[0].(map[string]interface{})
	if !ok || container["image"] != managerImage {
		t.Fatalf("manager image = %#v, want %q", container["image"], managerImage)
	}
	env, found, err := unstructured.NestedSlice(container, "env")
	if err != nil || !found {
		t.Fatalf("manager environment = %#v, want CRC agent image: %v", env, err)
	}
	crcAgentFound := false
	for _, value := range env {
		entry, ok := value.(map[string]interface{})
		if ok && entry["name"] == "CRC_AGENT_IMAGE" && entry["value"] == crcAgentImage {
			crcAgentFound = true
		}
	}
	if !crcAgentFound {
		t.Fatalf("manager environment = %#v, want CRC_AGENT_IMAGE=%q", env, crcAgentImage)
	}

	resource(t, objects, "Namespace", defaultNamespace)
	for _, object := range objects {
		if object.GetNamespace() == "openshift-config" {
			t.Fatalf("direct manifest unexpectedly contains openshift-config resource %s/%s", object.GetKind(), object.GetName())
		}
	}

	assertDeploymentSourcesUnchanged(t, repoRoot, sources)
}

func TestCDICloneSourcePermissionIsLimitedToOperatorNamespace(t *testing.T) {
	repoRoot := repositoryRoot(t)
	kustomize := kustomizePath(t, repoRoot)
	output := filepath.Join(t.TempDir(), "install.yaml")
	runRenderer(t, repoRoot, nil, "build-installer", kustomize, managerImage, crcAgentImage, output)
	objects := readManifests(t, output)

	role := resource(t, objects, "Role", "guestcluster-operator-manager-cdi-clone-source")
	if role.GetNamespace() != defaultNamespace {
		t.Fatalf("CDI clone source Role namespace = %q, want %q", role.GetNamespace(), defaultNamespace)
	}
	rules, found, err := unstructured.NestedSlice(role.Object, "rules")
	if err != nil || !found || len(rules) != 1 {
		t.Fatalf("CDI clone source Role rules = %#v, want one rule: %v", rules, err)
	}
	rule, ok := rules[0].(map[string]interface{})
	if !ok || !containsString(rule["apiGroups"], "cdi.kubevirt.io") ||
		!containsString(rule["resources"], "datavolumes/source") || !containsString(rule["verbs"], "create") {
		t.Fatalf("CDI clone source Role rule = %#v, want create on cdi.kubevirt.io/datavolumes/source", rules[0])
	}

	binding := resource(t, objects, "RoleBinding", "guestcluster-operator-manager-cdi-clone-source")
	if binding.GetNamespace() != defaultNamespace {
		t.Fatalf("CDI clone source RoleBinding namespace = %q, want %q", binding.GetNamespace(), defaultNamespace)
	}
	roleName, found, err := unstructured.NestedString(binding.Object, "roleRef", "name")
	if err != nil || !found || roleName != role.GetName() {
		t.Fatalf("CDI clone source RoleBinding roleRef.name = %q, want %q: %v", roleName, role.GetName(), err)
	}
	subjects, found, err := unstructured.NestedSlice(binding.Object, "subjects")
	if err != nil || !found || len(subjects) != 1 {
		t.Fatalf("CDI clone source RoleBinding subjects = %#v, want one manager service account: %v", subjects, err)
	}
	subject, ok := subjects[0].(map[string]interface{})
	validSubject := ok && subject["kind"] == "ServiceAccount" &&
		subject["name"] == "guestcluster-operator-controller-manager" &&
		subject["namespace"] == defaultNamespace
	if !validSubject {
		t.Fatalf("CDI clone source RoleBinding subject = %#v, want the manager ServiceAccount in %q",
			subjects[0], defaultNamespace)
	}

	managerRole := resource(t, objects, "ClusterRole", "guestcluster-operator-manager-role")
	clusterRules, found, err := unstructured.NestedSlice(managerRole.Object, "rules")
	if err != nil || !found {
		t.Fatalf("manager ClusterRole rules = %#v: %v", clusterRules, err)
	}
	for _, item := range clusterRules {
		clusterRule, ok := item.(map[string]interface{})
		if ok && containsString(clusterRule["resources"], "datavolumes/source") {
			t.Fatal("manager ClusterRole grants cluster-wide datavolumes/source access")
		}
	}
}

func TestRendererPreservesImageTagsAndDigests(t *testing.T) {
	repoRoot := repositoryRoot(t)
	kustomize := kustomizePath(t, repoRoot)
	for _, image := range []string{
		"registry.example.test/guestcluster-operator:stable",
		"registry.example.test/guestcluster-operator@sha256:0123456789abcdef",
	} {
		output := filepath.Join(t.TempDir(), "install.yaml")
		runRenderer(t, repoRoot, nil, "build-installer", kustomize, image, crcAgentImage, output)
		deployment := resource(t, readManifests(t, output), "Deployment", "guestcluster-operator-controller-manager")
		containers, found, err := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
		if err != nil || !found || len(containers) != 1 {
			t.Fatalf("image %q has invalid containers: %#v: %v", image, containers, err)
		}
		container, ok := containers[0].(map[string]interface{})
		if !ok || container["image"] != image {
			t.Errorf("rendered image = %#v, want %q", container["image"], image)
		}
	}
}

func TestRendererAppliesRenderedFilesAndUndeploysWithMatchingResources(t *testing.T) {
	repoRoot := repositoryRoot(t)
	kustomize := kustomizePath(t, repoRoot)
	sources := readDeploymentSources(t, repoRoot)

	kubectl, log, captures := fakeKubectl(t)
	env := []string{
		"KUBECTL_LOG=" + log,
		"KUBECTL_CAPTURE_DIR=" + captures,
	}
	runRenderer(t, repoRoot, env, "deploy", kustomize, kubectl, managerImage, crcAgentImage)
	runRenderer(t, repoRoot, env, undeployTarget, kustomize, kubectl, managerImage, crcAgentImage, "true")

	logContents, err := os.ReadFile(log)
	if err != nil {
		t.Fatalf("read kubectl log: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(logContents)), "\n")
	if len(lines) != 2 {
		t.Fatalf("kubectl calls = %d, want two: %q", len(lines), lines)
	}
	if !strings.HasPrefix(lines[0], "apply -f ") {
		t.Fatalf("apply call = %q, want rendered default manifest", lines[0])
	}
	if !strings.HasPrefix(lines[1], "delete --ignore-not-found=true -f ") {
		t.Fatalf("delete call = %q, want matching rendered manifest", lines[1])
	}

	deployed := readManifests(t, filepath.Join(captures, "1.yaml"))
	deleted := readManifests(t, filepath.Join(captures, "2.yaml"))
	if manifestKeys(deployed) != manifestKeys(deleted) {
		t.Fatalf("deploy and undeploy resources differ: deploy=%q delete=%q", manifestKeys(deployed), manifestKeys(deleted))
	}
	assertDeploymentSourcesUnchanged(t, repoRoot, sources)
}

func TestRendererDoesNotApplyAfterRenderingFailure(t *testing.T) {
	repoRoot := repositoryRoot(t)
	realKustomize := kustomizePath(t, repoRoot)
	failingKustomize := failingKustomize(t, realKustomize)
	kubectl, log, _ := fakeKubectl(t)
	tmpdir := t.TempDir()
	sources := readDeploymentSources(t, repoRoot)

	cmd := exec.Command(
		filepath.Join(repoRoot, "hack", "render-deployment.sh"),
		"deploy", failingKustomize, kubectl, managerImage, crcAgentImage,
	)
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(), "KUBECTL_LOG="+log, "TMPDIR="+tmpdir)
	if output, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("render succeeded, want failure; output: %s", output)
	}
	if _, err := os.Stat(log); err == nil {
		contents, readErr := os.ReadFile(log)
		if readErr != nil || len(bytes.TrimSpace(contents)) != 0 {
			t.Fatalf("kubectl was called after rendering failure: %q", contents)
		}
	}
	entries, err := os.ReadDir(tmpdir)
	if err != nil {
		t.Fatalf("read temporary directory: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("temporary rendering files remain: %v", entries)
	}
	assertDeploymentSourcesUnchanged(t, repoRoot, sources)
}

func TestRendererPropagatesApplyFailure(t *testing.T) {
	repoRoot := repositoryRoot(t)
	kustomize := kustomizePath(t, repoRoot)
	kubectl, log, captures := fakeKubectl(t)
	sources := readDeploymentSources(t, repoRoot)
	cmd := exec.Command(
		filepath.Join(repoRoot, "hack", "render-deployment.sh"),
		"deploy", kustomize, kubectl, managerImage, crcAgentImage,
	)
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(),
		"KUBECTL_LOG="+log,
		"KUBECTL_CAPTURE_DIR="+captures,
		"KUBECTL_FAIL_ON_CALL=1",
	)
	if output, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("deploy succeeded, want apply failure; output: %s", output)
	}
	logContents, err := os.ReadFile(log)
	if err != nil {
		t.Fatalf("read kubectl log: %v", err)
	}
	if calls := strings.Split(strings.TrimSpace(string(logContents)), "\n"); len(calls) != 1 {
		t.Fatalf("kubectl calls = %d, want one: %q", len(calls), calls)
	}
	assertDeploymentSourcesUnchanged(t, repoRoot, sources)
}

func TestConcurrentRendersDoNotShareState(t *testing.T) {
	repoRoot := repositoryRoot(t)
	kustomize := kustomizePath(t, repoRoot)
	images := []string{
		"registry.example.test/guestcluster-operator:one",
		"registry.example.test/guestcluster-operator:two",
	}

	var waitGroup sync.WaitGroup
	type renderResult struct {
		image  string
		output string
		err    error
	}
	results := make(chan renderResult, len(images))
	for _, image := range images {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			output := filepath.Join(t.TempDir(), "install.yaml")
			cmd := exec.Command(
				filepath.Join(repoRoot, "hack", "render-deployment.sh"),
				"build-installer", kustomize, image, crcAgentImage, output,
			)
			cmd.Dir = repoRoot
			if outputBytes, err := cmd.CombinedOutput(); err != nil {
				results <- renderResult{image: image, err: fmt.Errorf("render: %w: %s", err, outputBytes)}
				return
			}
			results <- renderResult{image: image, output: output}
		}()
	}
	waitGroup.Wait()
	close(results)
	for result := range results {
		if result.err != nil {
			t.Errorf("render %q: %v", result.image, result.err)
			continue
		}
		objects := readManifests(t, result.output)
		deployment := resource(t, objects, "Deployment", "guestcluster-operator-controller-manager")
		containers, _, err := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
		if err != nil || len(containers) != 1 {
			t.Errorf("render %q has invalid containers: %v", result.image, err)
			continue
		}
		container, ok := containers[0].(map[string]interface{})
		if !ok || container["image"] != result.image {
			t.Errorf("render %q produced image %v", result.image, container["image"])
		}
	}
}

func TestMakeTargetsUseSharedRenderer(t *testing.T) {
	repoRoot := repositoryRoot(t)
	kustomize := kustomizePath(t, repoRoot)
	for _, target := range []string{"build-installer", "deploy", undeployTarget} {
		cmd := exec.Command(
			"make", "-n", "-o", "manifests", "-o", "generate", "-o", "kustomize", target,
			"KUSTOMIZE="+kustomize,
		)
		cmd.Dir = repoRoot
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("make %s -n failed: %v\n%s", target, err, output)
		}
		if !strings.Contains(string(output), "hack/render-deployment.sh "+target) {
			t.Errorf("make %s does not use the shared renderer:\n%s", target, output)
		}
	}
}

func TestMakeDeployAndUndeployUseRenderedManifests(t *testing.T) {
	repoRoot := repositoryRoot(t)
	kustomize := kustomizePath(t, repoRoot)
	kubectl, log, captures := fakeKubectl(t)
	sources := readDeploymentSources(t, repoRoot)
	env := append(os.Environ(), "KUBECTL_LOG="+log, "KUBECTL_CAPTURE_DIR="+captures)

	for _, target := range []string{"deploy", undeployTarget} {
		args := []string{
			"--no-print-directory",
			"-o", "manifests",
			"-o", "generate",
			"-o", "kustomize",
			target,
			"KUSTOMIZE=" + kustomize,
			"KUBECTL=" + kubectl,
			"IMG=" + managerImage,
			"CRC_AGENT_IMG=" + crcAgentImage,
		}
		if target == undeployTarget {
			args = append(args, "ignore-not-found=true")
		}
		cmd := exec.Command("make", args...)
		cmd.Dir = repoRoot
		cmd.Env = env
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("make %s failed: %v\n%s", target, err, output)
		}
	}

	logContents, err := os.ReadFile(log)
	if err != nil {
		t.Fatalf("read kubectl log: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(logContents)), "\n")
	if len(lines) != 2 {
		t.Fatalf("kubectl calls = %d, want two: %q", len(lines), lines)
	}
	if !strings.HasPrefix(lines[0], "apply -f ") {
		t.Fatalf("deploy call = %q", lines[0])
	}
	if !strings.HasPrefix(lines[1], "delete --ignore-not-found=true -f ") {
		t.Fatalf("undeploy call = %q", lines[1])
	}
	assertDeploymentSourcesUnchanged(t, repoRoot, sources)
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
}

func kustomizePath(t *testing.T, repoRoot string) string {
	t.Helper()
	path := os.Getenv("KUSTOMIZE")
	if path == "" {
		path = filepath.Join(repoRoot, "bin", "kustomize")
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(repoRoot, path)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("kustomize is not available at %s: %v; run make kustomize", path, err)
	}
	return path
}

func runRenderer(t *testing.T, repoRoot string, env []string, args ...string) {
	t.Helper()
	cmd := exec.Command(filepath.Join(repoRoot, "hack", "render-deployment.sh"), args...)
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(), env...)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("renderer failed: %v\n%s", err, output)
	}
}

func readDeploymentSources(t *testing.T, repoRoot string) map[string][]byte {
	t.Helper()
	paths := []string{
		"config/default/kustomization.yaml",
		"config/manager/kustomization.yaml",
	}
	sources := make(map[string][]byte, len(paths))
	for _, path := range paths {
		contents, err := os.ReadFile(filepath.Join(repoRoot, path))
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		sources[path] = contents
	}
	return sources
}

func assertDeploymentSourcesUnchanged(t *testing.T, repoRoot string, sources map[string][]byte) {
	t.Helper()
	for path, want := range sources {
		got, err := os.ReadFile(filepath.Join(repoRoot, path))
		if err != nil {
			t.Fatalf("read %s after render: %v", path, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("render changed %s", path)
		}
	}
}

func readManifests(t *testing.T, path string) []unstructured.Unstructured {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open manifests %s: %v", path, err)
	}
	defer func() { _ = file.Close() }()

	decoder := yaml.NewYAMLOrJSONDecoder(file, 4096)
	var objects []unstructured.Unstructured
	for {
		var object map[string]interface{}
		if err := decoder.Decode(&object); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("decode manifests %s: %v", path, err)
		}
		if len(object) == 0 {
			continue
		}
		objects = append(objects, unstructured.Unstructured{Object: object})
	}
	return objects
}

func resource(t *testing.T, objects []unstructured.Unstructured, kind, name string) *unstructured.Unstructured {
	t.Helper()
	for index := range objects {
		object := &objects[index]
		if object.GetKind() == kind && object.GetName() == name {
			return object
		}
	}
	t.Fatalf("resource %s/%s not found", kind, name)
	return nil
}

func manifestKeys(objects []unstructured.Unstructured) string {
	keys := make([]string, 0, len(objects))
	for _, object := range objects {
		keys = append(keys, object.GetKind()+"/"+object.GetNamespace()+"/"+object.GetName())
	}
	return strings.Join(keys, "\n")
}

func containsString(value interface{}, want string) bool {
	values, ok := value.([]interface{})
	if !ok {
		return false
	}
	for _, item := range values {
		if item == want {
			return true
		}
	}
	return false
}

func fakeKubectl(t *testing.T) (path, log, captures string) {
	t.Helper()
	dir := t.TempDir()
	path = filepath.Join(dir, "kubectl")
	log = filepath.Join(dir, "calls.log")
	captures = filepath.Join(dir, "captures")
	if err := os.Mkdir(captures, 0o755); err != nil {
		t.Fatalf("create capture directory: %v", err)
	}
	script := `#!/bin/sh
set -eu
log=${KUBECTL_LOG:?}
capture_dir=${KUBECTL_CAPTURE_DIR:?}
previous=
manifest=
for argument in "$@"; do
  if [ "${previous}" = "-f" ]; then
    manifest=${argument}
  fi
  previous=${argument}
done
test -n "${manifest}"
count=$(wc -l < "${log}" 2>/dev/null || true)
count=$((${count:-0} + 1))
printf '%s\n' "$*" >> "${log}"
if [ "${KUBECTL_FAIL_ON_CALL:-0}" -eq "${count}" ]; then
  exit 41
fi
cp "${manifest}" "${capture_dir}/${count}.yaml"
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake kubectl: %v", err)
	}
	return path, log, captures
}

func failingKustomize(t *testing.T, realPath string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kustomize")
	script := fmt.Sprintf("#!/bin/sh\nset -eu\nif [ \"${1:-}\" = build ]; then exit 23; fi\nexec %q \"$@\"\n", realPath)
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write failing kustomize: %v", err)
	}
	return path
}
