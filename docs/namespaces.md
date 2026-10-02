# Namespaces, ownership, and upgrades

## Placement

| Resource | Namespace and owner |
| --- | --- |
| Manager | Operator namespace |
| CRCBundle preparation Jobs, PVCs, metadata, and source SSH keys | Operator namespace; shared CRCBundle lifecycle |
| ClusterPool, ClusterInstance, ClusterLease | Same source namespace |
| Pull-secret and optional manual CRC or HCP worker SSH inputs | Source namespace; user-owned |
| CRC VM, DataVolume, Service, Route, generated Secrets, Jobs, and agent RBAC | Instance namespace; instance-owned |
| New HCP HostedCluster, NodePool, and generated certificate | Instance namespace; valid local owners. The NodePool retains its HostedCluster controller owner. |
| Published instance and lease kubeconfigs | Source namespace; instance or lease owner |
| HCP API Route and control-plane workloads | HyperShift control-plane namespace; platform lifecycle and instance finalizer cleanup |

Instances do not own or delete shared CRCBundle artifacts or user input Secrets.
CRC cleanup stops agent Jobs before removing agent access and boot-key copies.
It retains the instance finalizer until backing compute and storage are gone.
Recorded parent UIDs identify dependent VMIs, launcher Pods, and PVCs after
their parents disappear. Deletion uses UID preconditions for verified CRC objects.

The operator checks ownership before reuse, adoption, update, or deletion of
managed CRC objects. It reports foreign objects as conflicts. Do not remove
owner references to reuse a resource for another instance.

## Enable a source namespace

```sh
kubectl apply -f config/samples/namespace.yaml
kubectl -n guestcluster-demo create secret generic pull-secret \
  --from-file=.dockerconfigjson=./pull-secret.json --type=kubernetes.io/dockerconfigjson
kubectl -n guestcluster-demo apply -f config/samples/guestcluster_v1alpha1_clusterpool.yaml
```

For another namespace:

```sh
kubectl label namespace <source-namespace> guestcluster.opdev.io/enabled=true --overwrite
```

Only the exact string `"true"` enables new provisioning. Missing labels, `false`,
and other values disable it. There is no operator-namespace exception. This
policy applies to CRC and HCP pools and standalone instances.

Create input Secrets in the source namespace. `template.pullSecretRef` selects
a local Secret; the default is `pull-secret`. Manual CRC inputs use
`template.bundleSSHKeyRef`. Optional HCP worker SSH inputs use
`template.hcpWorkerSSHKeyRef`. New HCP clusters reference these local inputs
directly. CRCBundle instances use an instance-owned copy of the shared SSH key.

HyperShift must watch every source namespace used for HCP. The operator names
each HostedCluster `gc-<instance-prefix>-<identity-hash>`. The hash is the first
16 hexadecimal characters of SHA-256 over the source namespace, a NUL separator,
and the full instance name. It separates identities that simple concatenation
or dot replacement would merge, such as `tenant/a-b` and `tenant-a/b`.

HyperShift creates control-plane namespace
`<source-namespace>-<HostedCluster-name>`. The operator shortens the readable
prefix to fit the 63-character namespace limit. When necessary, it removes the
prefix and uses `gc-<identity-hash>`. It never shortens the hash. Source namespaces
longer than 43 characters cannot fit this rule and are rejected for HCP.
The operator still checks name limits and conflicts with existing resources
before HCP allocation. Verify watch scope in the installed HyperShift deployment.

## Remove and restore opt-in

```sh
kubectl label namespace <source-namespace> guestcluster.opdev.io/enabled-
```

Label removal has these effects:

- Pools stop new instance creation, expansion, replacement, and automatic
  CRCBundle preparation. They retain current capacity observations.
- Unstarted instances wait. `ProvisioningAllowed=False` with reason
  `NamespaceDisabled` identifies this policy block.
- An instance with persisted `status.provisioning` can finish provisioning.
  It continues health checks, maintenance, and recovery. Agent permissions stay
  available for the instance lifetime.
- Leases can bind existing Ready capacity. Demand that needs another instance
  waits. Lease release and TTL expiry still delete their bound instance.
- Explicit deletion and finalizers still run. A disabled pool does not replace
  the deleted instance.
- Direct administration of a cluster-scoped CRCBundle remains subject to its
  RBAC. The namespace label does not disable the shared cache controller.

The controller writes authorization before the first backing-resource write.
An existing CR, a finalizer, or a `Provisioning` phase alone is not authorization.
Namespace events wake waiting pools and instances when the label is restored.

Namespace termination is stronger than opt-out. It stops new allocations,
including lease binding, while instance and lease finalizers continue cleanup.
HCP cleanup derives the backing locations from the source namespace and hashed
HostedCluster name, including the separate control-plane namespace.

## Upgrade behavior

This release does not adopt backing resources created by earlier versions.
Delete or replace those `ClusterInstance` objects before you use this release.
HostedCluster names now include an identity hash. Delete existing HCP instances
with the previous release and wait for cleanup before installing this release.
Wait for CRC agent Jobs to finish before you remove the old shared CRC agent
account and its Role and RoleBinding.

Install the updated CRDs, manager permissions, manager image, and agent image
together, through direct manifests or an OLM bundle. Label each source namespace
where new capacity is required. Include the operator namespace if it contains
pools. Check `ProvisioningAllowed` on pools and instances.

HCP resources use the source namespace. CRC and HCP resources require current
owner references. The operator rejects resources without a verified owner.

## Permissions and conditions

Permission to create user CRs is separate from manager permission to create
agent RBAC. Users do not need to create agent accounts or bindings manually.
The manager binds an instance-local account to the installed static agent
ClusterRole. No agent ClusterRoleBinding or namespace RBAC cleanup controller
is required.

Initially, each agent can get, create, and update Secrets throughout its source
namespace, and read/watch VMIs there. Per-instance accounts separate resource
lifetimes; they do not provide Secret isolation within the same namespace.
Use separate namespaces for that boundary. See [CRC agent RBAC](crc-agent-rbac.md).

`NamespaceDisabled` and `NamespaceTerminating` are policy conditions. They are
distinct from `CRCAgent` Job and handoff diagnostics. Check the condition
message before investigating agent Pod logs.

## Test coverage and OpenShift regression tests

`make test` includes policy transitions, persisted authorization, status-write
failures, namespace watch delivery, lease behavior, ownership conflicts,
dependent cleanup, and endpoint tests. Real envtest
authorization tests exercise both direct-install and generated OLM manager
rules, agent Secret operations, and cross-namespace denial.

`make test-e2e` runs the Kind/CDI clone test and synthetic VMI replacement test.
These tests do not cover real HyperShift hosted clusters or OpenShift Routes.

The `openshift` build tag enables real CRC/CRC, HCP/HCP, and CRC/HCP regression
tests. The suite needs OpenShift, HCO, CDI, HyperShift, ingress, and storage. It
does not run as part of `make test` or `make test-e2e`.

The Go test installs one isolated direct manager. It pauses an existing manager
Deployment in `guestcluster-operator-system` while the tests run and restores
its replica count after cleanup. Each topology pair creates its own namespaces,
inputs, pools, leases, and image-pull permissions. It waits for cleanup before
it starts the next pair. It checks namespace opt-in and opt-out, real guest
readiness and kubeconfig use, lease binding, blocked expansion, CRC VMI recovery,
HCP Route repair, shared artifact preservation, re-enable, and namespace
deletion. The suite never supplies a synthetic Ready status.

Run it on a dedicated cluster with no existing `ClusterInstance` or
`ClusterLease` resources. The Go test checks this before it pauses a manager.

The Go test builds Linux/amd64 manager and CRC-agent images with Docker. It pushes
them to temporary ImageStreams in the test namespace, then removes that
namespace during teardown. It enables the image registry default route only for
the push, then restores its previous setting. Put the input Secrets referenced
by the pool templates in `OPENSHIFT_INPUT_NAMESPACE`; the default is
`guestcluster-operator-system`. The default pool templates are in
`test/openshift`. Set `OPENSHIFT_CRC_POOL` or `OPENSHIFT_HCP_POOL` to use
platform-specific templates.

Run the suite with the current `kubectl` context set to the supported OpenShift
cluster. Docker, Skopeo, `kubectl`, and `kustomize` must be available on `PATH`.
Set `DOCKER`, `SKOPEO`, `KUBECTL`, or `KUSTOMIZE` to use different paths:

```sh
make test-openshift-e2e
```

The test installs and removes its direct manager and temporary cluster RBAC in
Go. It keeps the CRDs and shared CRCBundle resources in place. Record the
platform, storage driver, HyperShift version and watch scope, and image digests
with each result. Unit and Kind tests do not replace this live platform check.
