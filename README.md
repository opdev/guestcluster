# guestcluster-operator

This Kubernetes operator manages the lifecycle of guest clusters. The guest
clusters use two topologies: **CRC** (CodeReady Containers / OpenShift
Local) and **HyperShift**. The guest clusters run inside a management
OpenShift cluster. CI systems lease, use, and recycle these disposable
clusters on a shared hypervisor. The operator removes the need for manual
bookkeeping.

## Why

- CRC covers SNO test suites and Machine Config Operator / platform-
  alteration test suites.
- HyperShift covers multi-node test suites and EaaS-parity test suites.
- Both topologies need provisioning. Each job must reuse them, isolate
  them from other jobs, and tear them down cleanly between runs. No person
  needs to track which VM or hosted cluster is free.

This operator works as a small, self-hosted **cluster broker**. CI
acquires a *lease* on a cluster of a given topology. The lease returns a
`kubeconfig` Secret. CI uses the cluster, then releases the lease. The
operator recycles the underlying compute. The next job gets a clean slate.

## Architecture

The operator has one binary, two controllers, and three CRDs. It also has
a small, out-of-band crc-agent process:

```
                 ┌─────────────────┐        watches/owns       ┌───────────────────┐
CI  ── apply ──▶ │   ClusterLease   │ ◀────────────────────────│    ClusterPool     │
                 │ (demand side)    │                           │  (supply side)     │
                 └────────┬─────────┘                           └─────────┬──────────┘
                          │ binds to a free, Ready instance                │ tops up
                          ▼                                                ▼
                 ┌──────────────────────────────────────────────────────────┐
                 │                     ClusterInstance                       │
                 │  (state machine: Provisioning → Ready → deleted;          │
                 │   "claimed by a lease" is derived, not a phase --         │
                 │   see "Binding model" below)                              │
                 └───────────────┬───────────────────────┬────────────────┘
                                 │ type: crc                       │ type: hcp
                                 ▼                                 ▼
                 ┌───────────────────────────┐     ┌───────────────────────────────┐
                 │ KubeVirt VirtualMachine   │     │ HyperShift HostedCluster        │
                 │ + CDI DataVolume (boots   │     │ + NodePool (KubeVirt workers)   │
                 │ crc.qcow2, "parked")      │     │                                 │
                 └─────────────┬─────────────┘     └───────────────┬─────────────────┘
                               │ VMI has IP → crc-agent Job created│ HC status.kubeconfig
                               ▼                                    │
                     ┌────────────────────┐                         │
                     │  crc-agent Job     │                         │
                     │  (SSH as "core",    │                         │
                     │  runs native Go     │                         │
                     │  post-boot fixups,  │                         │
                     │  publishes raw       │                         │
                     │  kubeconfig Secret)  │                         │
                     └─────────┬───────────┘                         │
                               └───────────────┬──────────────────────┘
                                               ▼
                                canonical per-instance kubeconfig Secret
                                               │
                                               ▼
                                  per-lease kubeconfig Secret (copy)
                                               │
                                               ▼
                                        CI reads it, runs `oc get nodes`
```

### CRDs (group `guestcluster.opdev.io/v1alpha1`)

| Kind | Short name | Role |
|---|---|---|
| `ClusterPool` | `cpool` | Supply side. This CRD declares a topology (`crc` or `hcp`), a `template` for provisioning, and a budget (`minSize`, `warmSpares`, `maxSize`). The operator tops up `ClusterInstance`s continuously. This satisfies `minSize`, `warmSpares`, and any Pending `ClusterLease` demand. The operator never exceeds `maxSize`. |
| `ClusterInstance` | `cinst` | This CRD represents one concrete guest cluster: a CRC VM, or a HostedCluster with a NodePool. It owns the actual KubeVirt/HyperShift objects. It drives provisioning to the ready state. It publishes a per-instance kubeconfig Secret. A lease claim is not one of its own phases (see "Binding model" below). There is no in-place reset. On release, the operator deletes a claimed instance. It does not recycle the instance back to Ready. |
| `ClusterLease` | `clease` | Demand side. CI creates this CRD. The operator matches it to a free `Ready` `ClusterInstance` of the requested type. It claims the instance with a single atomic write (`status.instanceRef` plus `Phase: Bound`). It copies the instance's kubeconfig into a lease-owned Secret. It deletes the claimed instance on release, on deletion, or on TTL expiry. |

Two controllers implement this:

- **`ClusterPoolReconciler`** handles supply only. It never binds a lease
  itself; `ClusterLeaseReconciler` does that job. It lists the
  `ClusterInstance`s it owns, using the `opdev.io/pool` label. It lists the
  `ClusterLease`s that target it. It computes `status.totalInstances`,
  `status.availableInstances`, and `status.leasedInstances`. It maintains
  three independent floors. `maxSize` always bounds these floors:
  - **`minSize`**: a stable, total-count floor. This floor counts any
    non-terminal phase. It works like a cluster autoscaler's minimum
    node-group size. It stays independent of current lease demand.
  - **`warmSpares`**: a spare-capacity floor. The operator measures this
    floor against Ready, unclaimed instances (autoscaler-style
    over-provisioning). It keeps this floor ahead of demand. A lease can
    then bind instantly instead of waiting for a full provision.
  - **Pending `ClusterLease` demand**: the operator provisions instances
    on demand when leases outnumber what `minSize` and `warmSpares` alone
    would produce. This makes `minSize: 0, warmSpares: 0` a valid, pure
    on-demand configuration. `ClusterLeaseReconciler` never provisions
    instances. Without this floor, the pool would never create an
    instance for a Pending lease.

  The controller creates or deletes **one** `ClusterInstance` per
  reconcile. This avoids a thundering herd. It targets whichever floor or
  demand has the largest deficit or surplus. It reports a
  `CapacityAvailable` status condition: `CapacitySufficient`, `AtCapacity`,
  or `MaxSizeTooSmall`. This condition shows whether the floors are
  reachable given `maxSize`, and whether the pool is saturated. For
  example, a `ClusterLease` stuck at `Pending` because the pool is at
  `maxSize` now shows `AtCapacity`. It does not wait forever in silence.
- **`ClusterInstanceReconciler`** owns the actual backing resources. It
  dispatches on `spec.type`:
  - `crc`: The controller creates a CDI `DataVolume`. This volume boots
    from `template.releaseImage`, the **extracted** `crc.qcow2` disk image
    from an official `.crcbundle` (see [CRC bundle setup](#crc-bundle-setup)
    below). The controller also creates a KubeVirt `VirtualMachine` wired
    to that volume. Once the VM's `VirtualMachineInstance` reports an IP,
    the controller creates a run-to-completion **crc-agent Job**
    (`<instance>-crc-agent-<vmi-hash>`). This Job connects to the VM over SSH as user
    `core`, using `template.bundleSSHKeyRef`. It runs every post-boot
    fixup natively, with no external orchestration binary (see
    [crc-agent](#crc-agent-cmdcrc-agent) below). The controller waits for
    the Job to publish the raw kubeconfig Secret
    (`<instance>-crc-raw-kubeconfig-<vmi-uid>`). It verifies the external API
    before it marks the instance `Ready`, and retains the result for recovery.
    KubeVirt is the only hypervisor involved. No nested
    virtualization is required.
  - `hcp`: The controller creates a HyperShift `HostedCluster` with
    `platform: KubeVirt`. It sets `controllerAvailabilityPolicy` from
    `template.controllerAvailabilityPolicy` (default: `SingleReplica`). It
    also creates a `NodePool`, sized to `template.nodePoolReplicas`
    (default: 1). It waits for `HostedCluster` to report `Available=True`.
    It waits for the `NodePool` to reach its desired replica count. Then
    it reads the admin kubeconfig Secret referenced by `status.kubeConfig`.

  In both cases, once the instance is ready, the controller writes the
  **canonical** per-instance kubeconfig Secret (`<instance>-kubeconfig`,
  with keys `kubeconfig` and `ocpVersion`). It sets `status.phase`,
  `status.ocpVersion`, `status.topology`, `status.apiEndpoint`, and
  `status.kubeconfigSecretRef`. If the observed OCP version differs from
  `spec.template.ocpVersion`, it also sets a non-fatal `VersionMismatch`
  condition. CI decides whether a version mismatch is a hard fail or a
  documented skip.

- **`ClusterLeaseReconciler`** performs demand-side matchmaking only. It
  **never** provisions instances. It lists `Ready` instances that belong
  to the requested pool and are not claimed by any other lease. It claims
  one instance with a single atomic `Status().Update` on the *lease
  itself* (see "Binding model" below). It never writes to the
  `ClusterInstance` side. If no instance is free, it sets a
  `WaitingForCapacity` condition and requeues. `ClusterPoolReconciler`'s
  independent top-up loop eventually produces a free instance. An instance
  watch wakes a `Pending` lease as soon as an instance appears. The lease
  does not wait for the requeue backstop.

  On release, the lease controller deletes the claimed `ClusterInstance`
  outright. Release happens on TTL expiry or explicit deletion of the
  `ClusterLease`.
  `ClusterPoolReconciler`'s normal top-up loop then provisions a fresh
  replacement. It treats this the same as any other capacity shortfall.

### Binding model: single source of truth (scheduler-inspired)

The operator records the lease-to-instance relationship in **exactly one
place**: `ClusterLease.Status.InstanceRef`. This design deliberately
follows how the Kubernetes scheduler binds a Pod to a Node. The scheduler
writes one authoritative pointer, `Pod.Spec.NodeName`, once, on the demand
object:

- **`ClusterLease` maps to Pod. `ClusterInstance` maps to Node.**
  `Lease.Status.InstanceRef` is the only authoritative record of the
  binding. `ClusterInstance` has no "Leased" phase. It carries no
  authoritative claim state. An instance's own lifecycle stays exactly
  Provisioning, then Ready, then deleted. This lifecycle does not depend
  on whether a lease currently claims the instance, just as a Kubernetes
  `Node` has no "occupied" phase.
- **`Status.LeaseRef` on `ClusterInstance` is a read-only, derived
  projection.** `ClusterInstanceReconciler` maintains this field by
  watching `ClusterLease`s. This lets `kubectl get clusterinstance` show
  which lease, if any, currently claims the instance. No component uses
  this field to make a scheduling or lifecycle decision.
- **Binding is a single write.** `ClusterLeaseReconciler` commits a claim
  with one `Status().Update` on the lease: it sets `InstanceRef` and
  `Phase: Bound`. It never touches the instance. This design eliminates a
  whole class of races that existed in earlier iterations of this
  operator. In one such race, an instance got claimed while its lease's
  own status had not caught up yet; this caused the operator to provision
  a second, phantom instance. In another race, a lease retry after a
  partial write re-claimed a second instance and orphaned the first. With
  a single write, a failure writes nothing. No partial state needs
  reconciling.
- **Claims are "assumed" immediately.** The operator treats an instance as
  in-use the moment any lease's `InstanceRef` names it. This holds
  regardless of the instance's own phase, mirroring how the scheduler's
  cache assumes a binding before the kubelet confirms it. For this reason,
  `ClusterPoolReconciler`'s supply accounting (`minSize`, `warmSpares`,
  and demand; see above) sources entirely from
  `ClusterLease.Status.InstanceRef`.

### Recycle semantics ("no leftover operator/CSV")

There is no in-place "recycle" phase. The operator never resets an
instance back to Ready in place. Instead, releasing a lease deletes the
whole `ClusterInstance` object. This deletion goes through the same
finalizer-driven teardown as any other deletion. If still needed,
`ClusterPoolReconciler` then provisions a new instance to top back up to
`minSize` or `warmSpares`. All leasable instances are pool-owned,
anonymous, and interchangeable. Nothing depends on a stable identity or on
a Service/Route hostname across lease cycles: the operator copies a
lease's kubeconfig Secret and `status.apiEndpoint` fresh at bind time. So
a plain delete-and-recreate is simpler than a dedicated
teardown-and-recreate-in-place step:

- **CRC**: The operator deletes the crc-agent `Job`, the KubeVirt
  `VirtualMachine`, the CDI `DataVolume`, the raw crc-agent kubeconfig
  Secret, and the API `Service` and `Route`. It also destroys the VM's
  disk (`DataVolume`). So no operator or CSV installed by the previous
  test run can survive. The replacement instance's VM boots a pristine
  CRC bundle image.
- **HyperShift**: The operator deletes the `HostedCluster`, which cascades
  to the control plane. It also deletes the `NodePool` explicitly, as a
  backup step. This is a full destroy. No residual workload state remains.

One residual is documented: **out-of-band storage** provisioned by a
`StorageClass` with a `Retain` reclaim policy. This storage stays outside
the operator's control. If your environment needs strictly zero residual,
use a default `StorageClass` with a `Delete` policy.

### CRC bundle setup

A `.crcbundle`, as distributed for CodeReady Containers / OpenShift Local,
is a zstd-compressed tar archive. It contains a **fully pre-installed**
single-node OpenShift cluster disk image (`crc.qcow2`), a `kubeconfig`, an
`id_ecdsa_crc` SSH private key for user `core`, and metadata. The
distributor ships the bundle in a "parked" state on purpose. Its kubelet
is disabled. Its pull secret is an empty `{}`. Its certificates stay valid
for only **~30 days** from the bundle's build date.

You can supply a bundle to a `crc`-topology `ClusterPool` or standalone
`ClusterInstance` in two ways: the **turnkey path** (recommended) and a
**manual fallback**.

#### Turnkey path (recommended): `template.crcVersion`

Set only two fields on the pool's `template`. The operator handles
everything else automatically:

```yaml
template:
  crcVersion: "4.16.0"   # required: which CRC bundle to use
  # crcArch: "amd64"     # optional, default "amd64" (also supports "arm64")
  pullSecretRef:
    name: crc-pull-secret
```

When `ClusterPoolReconciler` sees `template.crcVersion` set, it creates a
cluster-scoped `CRCBundle` object named `crc-<version>-<arch>` (see
`internal/resources.CRCBundleName`). If one already exists, it **reuses**
that object instead. `CRCBundleReconciler` then:

1. Creates a golden `PersistentVolumeClaim` (default size: `35Gi`,
   configurable via `CRCBundle.spec.goldenVolumeSize`) and a transient
   scratch `PersistentVolumeClaim` (`40Gi`, fixed size). Both live in the
   **operator's own namespace** (see `OPERATOR_NAMESPACE` below).
2. Runs a one-time, run-to-completion **bundle-prep Job**. This Job reuses
   the crc-agent container image (see
   [crc-agent](#crc-agent-cmdcrc-agent)) but overrides its command and
   args to run an embedded shell script instead. The Job downloads the
   official `.crcbundle` from `mirror.openshift.com`. It verifies the
   download against the `sha256sum.txt` checksum, extracts `crc.qcow2` into
   the scratch PVC, then converts it to raw format at `/disk.img` in the
   golden PVC. The scratch PVC holds both the compressed and extracted
   files during conversion.
3. Publishes the bundle's `id_ecdsa_crc` SSH key as a `Secret`. It also
   publishes a small `ConfigMap` with the derived OpenShift version and
   checksum. Both live in the operator's namespace. The `CRCBundle` owns
   both objects, for garbage collection.
4. Once ready, sets `CRCBundle.status.phase: Ready`. This status includes
   references to the golden PVC and the SSH key Secret.

Every `ClusterInstance` created against that pool then **clones** the
golden PVC, per instance, using CDI's `DataVolumeSourcePVC`. This clone is
native, cross-namespace, and host-assisted. It needs no re-download and no
shared mutable disk. The instance uses this clone instead of an HTTP
`DataVolume` import. It also resolves its crc-agent SSH key directly from
the `CRCBundle`'s Secret, not from `template.bundleSSHKeyRef`.

The `CRCBundle` object is **cluster-scoped and shared**. Any number of
`ClusterPool`s or namespaces can reference the same `crcVersion` and
`crcArch`. They then reuse the same golden PVC and pay the
mirror-download cost only once. Expect **~10-20GB of egress from the
operator's namespace to `mirror.openshift.com`** the first time you use a
given version. Expect also ~35Gi of golden PVC storage and ~40Gi of
transient scratch PVC storage per distinct version. The operator deletes
the scratch PVC automatically, on a best-effort basis, once the bundle
reaches `Ready`.

You can also apply a `CRCBundle` manually, ahead of time (see
`config/samples/guestcluster_v1alpha1_crcbundle.yaml`). Use this to
pre-warm the golden PVC before any pool references it. Use this also to
override the mirror URL, the `sha256sum.txt` URL, or the storage class. A
`ClusterPool` reuses whatever `CRCBundle` already exists with a matching
name. It does not create a new one in that case.

#### Manual fallback: `template.releaseImage` + `template.bundleSSHKeyRef`

If you leave `template.crcVersion` unset, the operator falls back to the
original, fully supported manual path. Use this path for offline or
air-gapped mirrors, for a custom bundle build, or for any source other
than the public `mirror.openshift.com` layout:

```sh
curl -L -o crc.crcbundle <official-or-custom-bundle-download-url>
tar --zstd -xf crc.crcbundle
# upload the extracted crc.qcow2 somewhere reachable over HTTP by CDI
# (an internal registry/webserver on the mgmt cluster's network is fine)
```

Then, once per `ClusterPool` (not once per lease):

- Set `template.releaseImage` to the URL of that extracted `crc.qcow2`.
- Create a `Secret` from the bundle's `id_ecdsa_crc` and reference it via
  `template.bundleSSHKeyRef`:
  ```sh
  kubectl create secret generic crc-bundle-ssh-key --from-file=id_ecdsa=./id_ecdsa_crc
  ```
- `template.pullSecretRef` stays optional, on both paths. See
  [Pull secret](#pull-secret) below.

If you set both, `template.crcVersion` takes priority. The operator
ignores `releaseImage` and `bundleSSHKeyRef` in that case.

#### Pull secret

`template.pullSecretRef` is **optional**. If you leave it unset, the
operator defaults to a copy of the management cluster's own global pull
secret. This is the `pull-secret` Secret in the `openshift-config`
namespace. Every OpenShift cluster has this Secret already, scoped to
`quay.io/openshift-release-dev`. So provisioning works out of the box. You
do not need to supply a credential the cluster already has. This default
requires the RBAC that `make deploy` installs (see
[Prerequisites](#prerequisites)). If you deploy manually, apply
`config/openshift-config-rbac` too.

Set `template.pullSecretRef` explicitly to override this default. For
example, a disconnected or mirrored registry may need a narrower or
different credential:
```sh
kubectl create secret generic crc-pull-secret --from-file=.dockerconfigjson=./pull-secret.json --type=kubernetes.io/dockerconfigjson
```
then reference it via `template.pullSecretRef.name: crc-pull-secret`.

#### Common to both paths

The affected `ClusterInstance` fails fast in two cases. First, if the
pull secret is missing or malformed: neither an explicit `pullSecretRef`
nor the cluster's default pull secret is usable. Second, if the bundle
SSH key is missing or malformed, on the manual path only (the turnkey
path derives its key from the `CRCBundle` and never checks
`bundleSSHKeyRef`). In both cases, the operator sets `status.phase:
Failed` and a `Ready=False` condition. The `reason` field reads
`InvalidPullSecret` or `MissingBundleSSHKey`. This failure happens before
the operator creates any VM or Job. You get a clear failure, not a late,
opaque SSH error.

Bundle certificates expire after ~30 days. For this reason, **recycle
always re-provisions a fresh VM and re-runs the crc-agent Job from
scratch** (see
[Recycle semantics](#recycle-semantics-no-leftover-operatorcsv)). This
also refreshes the certificates on every reuse. Both the turnkey and
manual paths work this way.

#### `OPERATOR_NAMESPACE`

The turnkey path's golden PVC, scratch PVC, SSH-key Secret, metadata
ConfigMap, and bundle-prep Job all live in "the operator's own
namespace." `internal/resources.OperatorNamespace()` resolves this
namespace in priority order: first the `OPERATOR_NAMESPACE` environment
variable, then the in-cluster service account namespace file
(`/var/run/secrets/kubernetes.io/serviceaccount/namespace`), then a fixed
value of `"default"`. `config/manager/manager.yaml` already sets
`OPERATOR_NAMESPACE` through the downward API (`fieldRef:
metadata.namespace`). So deployments through `make deploy` need no extra
configuration. If you run the manager locally (`make run`), or through
some other method where neither the env var nor the service-account
namespace file exists, export `OPERATOR_NAMESPACE` yourself. Otherwise
the turnkey path defaults to the `default` namespace.

### crc-agent (`cmd/crc-agent`)

You can only observe CRC's own health, version, and kubeconfig from
inside the guest. A freshly booted bundle VM also needs several mandatory
fixups before use. These fixups start the disabled kubelet, swap in a
fresh SSH key, regenerate the admin CA and client cert, approve pending
kubelet CSRs, inject the real pull secret, set credentials, and make the
kubeconfig's server URL routable. crc-agent implements this checklist
**natively in Go**. An SSH runner (`golang.org/x/crypto/ssh`, ported from
patterns in the upstream `crc` CLI) drives the steps that must run as
root on the guest itself: dnsmasq, guest DNS configuration, starting the
kubelet, swapping the SSH key, and the one-time CA bootstrap (see
`cmd/crc-agent/guest.go`). Typed `k8s.io/client-go` and dynamic clients
drive everything else, tunneled to the guest's API server over the same
SSH connection: CSR approval, pull-secret and htpasswd patches, and the
external API serving-cert plus apiserver patch (see
`cmd/crc-agent/cluster.go`). This design replaces an earlier one that
shelled out to the unmaintained
[`crc-cloud`](https://github.com/crc-org/crc-cloud) `upi` provider,
itself built on Pulumi's Automation API. The operator has removed that
dependency, and Pulumi, entirely.

The guest CRC VM's own pod-network IP is **not routable from outside the
management cluster**: it lives in the cluster's internal pod CIDR. So the
operator provides external access to the guest API through a
management-cluster-side **passthrough `Route`** (see "External API
access" below), not through a DNS name that points directly at the VM.

For each `ClusterInstance` of topology `crc`, once its
`VirtualMachineInstance` reports an IP, `ClusterInstanceReconciler`
ensures a `Service` and passthrough `Route` that expose the guest API
externally (see below). It then creates a run-to-completion **Kubernetes
Job** (`<instance>-crc-agent-<vmi-hash>`, see `internal/resources.BuildCRCAgentJob`)
that runs this binary. The binary:

1. Waits for the VM's SSH endpoint (port 22) to accept connections. It
   then connects as user `core`, using the bundle's SSH key.
2. Runs the guest-side fixups over that connection. It detects the
   guest's own internal IP, which differs from the externally reachable
   VMI IP on masquerade-networked KubeVirt VMs. It configures dnsmasq and
   guest DNS resolution: through NetworkManager on OVN bundles, matching
   upstream `crc`'s own approach, with a direct `/etc/resolv.conf`
   fallback for older bundles. It starts the kubelet. It generates a
   fresh SSH keypair and swaps it in. It bootstraps a new admin CA and
   client certificate, by patching `admin-kubeconfig-client-ca` through
   `oc`, run on the guest itself. This is the one deliberate exception to
   the "no more shelling out to CLIs" rule.
3. Reconnects with the new SSH key. It opens a tunnel to the guest's
   `api.crc.testing:6443`. It builds typed OpenShift and Kubernetes
   clients over that tunnel, using the freshly minted client certificate.
   It verifies this certificate against the bundle's own server CA, a
   *different* trust root than the one generated for client-cert auth
   (see `guestResult.ServerCAPEM`'s doc comment).
4. Runs the remaining cluster-side fixups through those typed clients. It
   approves pending kubelet CSRs. It injects the real pull secret. It
   sets `kubeadmin` and `developer` credentials, bcrypt-hashed
   in-process. It mints a self-signed serving certificate for the
   externally routable API hostname, `CRC_API_HOSTNAME`, the Route host
   the controller already provisioned. It patches the apiserver's
   `namedCertificates` so the API server presents this certificate for
   that hostname.
5. Reads the resulting kubeconfig. It rewrites the server URL to
   `https://<CRC_API_HOSTNAME>:443`. It embeds the same self-signed
   certificate as the trusted CA. It derives the cluster's OpenShift
   version from the typed `ClusterVersion` object.
6. Publishes both values and the configured VMI UID into
	`<instance>-crc-raw-kubeconfig-<vmi-uid>`, with keys `kubeconfig`,
	`ocpVersion`, and `vmiUID`. This Secret forms the **only** contract
   between crc-agent and `ClusterInstanceReconciler`.
   `ClusterInstanceReconciler` reads this Secret and never connects over
   SSH itself.

`BuildCRCAgentJob` sets configuration through environment variables:
`INSTANCE_NAME`, `INSTANCE_NAMESPACE`, `CRC_SSH_HOST` (the VM's IP),
`CRC_VMI_UID` (the VMI identity),
`CRC_API_HOSTNAME` (the externally routable Route host), and
`CRC_SSH_KEY_PATH` and `PULL_SECRET_PATH` (mounted Secret file paths).

#### External API access (`Service` + passthrough `Route`)

The VMI's pod-network IP is reachable only from inside the management
cluster. So `ensureCRCAPIRoute` (in
`internal/controller/clusterinstance_crc.go`) provisions, per instance:

- A `ClusterIP` `Service` (`<instance>-crc-api`, see
  `resources.BuildCRCAPIService`). This Service selects the VM's
  virt-launcher pod directly, through KubeVirt's own `vm.kubevirt.io/name`
  label. It exposes port `6443`.
- A **passthrough** `Route` (`<instance>-crc-api`, see
  `resources.BuildCRCAPIRoute`), at host
  `api-<instance>.<mgmt-ingress-domain>`. The operator reads the domain
  from the management cluster's own `ingresses.config.openshift.io/cluster`.
  The management cluster's own router fronts this Route.

The Route requires passthrough termination, not edge or reencrypt
termination. The Kubernetes API needs mTLS client-certificate
authentication and SPDY/websocket upgrades, for `exec`, `logs`, and
`port-forward`. These features work only if the router forwards the raw
TLS stream to the guest API server untouched. This exposure method does
**not** weaken security. The router performs pure L4 SNI forwarding. It
never terminates or inspects the TLS session. So the same end-to-end mTLS
and API-server authentication/authorization that protects every
OpenShift cluster's API applies here too. Reachability does not equal
access.

The Service and Route persist for the `ClusterInstance`'s whole lifetime.
So an already-distributed kubeconfig's hostname stays valid for the
duration of a bound lease. The operator deletes them only when it tears
down the `ClusterInstance` itself, which happens outright on lease
release (see
[Recycle semantics](#recycle-semantics-no-leftover-operatorcsv)).
`status.apiEndpoint` holds this Route's URL (see "Known limitations"
below; earlier versions left this field empty for the `crc` topology).
The Job runs to completion once per boot. On failure, the Job's
`backoffLimit` (2) governs retries. A fresh Job always accompanies a
fresh VM.

`Dockerfile.crc-agent` builds the crc-agent container image. This image
layers this repo's `cmd/crc-agent` binary and the `oc` CLI, fetched from
the `stable-4` mirror channel, onto `quay.io/centos/centos:stream9`. The
**same image serves, unmodified, the `CRCBundle` bundle-prep Job**
described above: its `curl`, `tar`, `zstd`, `jq`, and `oc` tooling
matches exactly what the prep script needs. The prep Job simply
overrides the image's `Command` and `Args` at the Pod-spec level, instead
of using its default `ENTRYPOINT`. Build and push it with:

```sh
make docker-build-crc-agent CRC_AGENT_IMG=<registry>/guestcluster-operator-crc-agent:<tag>
make docker-push-crc-agent  CRC_AGENT_IMG=<registry>/guestcluster-operator-crc-agent:<tag>
```

`CRC_AGENT_IMG` is a **Makefile-only** variable (default:
`opdev.io/guestcluster-operator-crc-agent:latest`). It controls the tag
that `docker-build-crc-agent` and `docker-push-crc-agent` produce. The
running manager does *not* read this variable automatically. You must
separately set the **runtime** `CRC_AGENT_IMAGE` environment variable on
the manager `Deployment`, to match whatever tag you actually pushed. If
unset, this defaults to `internal/resources.DefaultCRCAgentImage`, the
same string as `CRC_AGENT_IMG`'s default. The two variables stay
independent, at two different times: one is a build-time Makefile
variable, the other a runtime environment variable, read through
`os.Getenv` by both `ClusterInstanceReconciler` and `CRCBundleReconciler`.

For the crc-agent Job, the image must run as the `crc-agent`
`ServiceAccount` (`config/rbac/crc_agent_*.yaml`), scoped to `secrets` and
its `VirtualMachineInstance` in the operator's namespace. For the bundle-prep Job, it must
run as the `bundle-prep` `ServiceAccount`, scoped to `secrets` and
`configmaps` access in the operator's namespace only.

The image fetches the bundled `oc` CLI from the `stable-4` mirror channel
at **build** time. This CLI is not pinned to any specific bundle's exact
OpenShift version. See [Known limitations](#known-limitations--todos).

## Prerequisites

On the **management** OpenShift cluster:

- OpenShift 4.14+.
- [OpenShift Virtualization](https://docs.openshift.com/container-platform/latest/virt/about-virt.html)
  installed: KubeVirt plus CDI. Both the CRC-as-VM path and HyperShift's
  KubeVirt worker platform need this. KubeVirt itself acts as the
  hypervisor for CRC VMs; it boots the CRC bundle's `crc.qcow2` directly.
  So **no nested virtualization is required**.
- [HyperShift operator](https://hypershift-docs.netlify.app/) installed.
  Required for the `hcp` topology.
- OVNKubernetes CNI, wildcard DNS routes enabled on the default
  `IngressController`, a default `StorageClass`, and a `LoadBalancer`
  implementation, for example MetalLB. These are standard
  HyperShift-on-KubeVirt prerequisites.
- A valid pull secret for `quay.io/openshift-release-dev`.
  `template.pullSecretRef` is optional. It defaults to a copy of the
  management cluster's own global pull secret, `openshift-config/pull-secret`
  (see [Pull secret](#pull-secret)). This default requires the operator
  to have `get` access to that Secret. The RBAC in
  `config/openshift-config-rbac` grants this access; `make deploy`
  applies it automatically. To override the default, set
  `template.pullSecretRef` explicitly, pointing at an opaque `Secret` in
  the same namespace as the pool or instances.
- For `crc` pools specifically: an extracted CRC bundle `crc.qcow2`,
  hosted at an HTTP-reachable URL, and a `Secret` holding its
  `id_ecdsa_crc` SSH key (`template.bundleSSHKeyRef`). See
  [CRC bundle setup](#crc-bundle-setup) below.
- A crc-agent container image, with `oc` installed, pushed somewhere your
  cluster can pull from. Reference this image through the manager's
  `CRC_AGENT_IMAGE` environment variable. See
  [crc-agent](#crc-agent-cmdcrc-agent) below.
- Go 1.26 or later, Docker or Podman, `kubectl` or `oc`. If you plan to
  modify this repo, also install `operator-sdk` v1.42 or later.

## Getting started

**Install the CRDs and RBAC:**

```sh
make install
```

**Run the manager**, either locally against your current kubeconfig
context, or deployed in-cluster:

```sh
# local dev
make run

# or build+push an image and deploy
make docker-build docker-push IMG=<registry>/guestcluster-operator:tag
make deploy IMG=<registry>/guestcluster-operator:tag
```

**Create pools** for the topologies you need. See `config/samples/` for
ready-to-edit examples: `guestcluster_v1alpha1_clusterpool.yaml` for
`crc`, and `guestcluster_v1alpha1_clusterpool_hcp.yaml` for `hcp`:

```sh
kubectl apply -f config/samples/guestcluster_v1alpha1_clusterpool.yaml
kubectl apply -f config/samples/guestcluster_v1alpha1_clusterpool_hcp.yaml
```

Each pool starts topping up instances to satisfy `spec.minSize` and
`spec.warmSpares`. `spec.maxSize` bounds both values.

## CI usage pattern

1. CI creates a `ClusterLease` that names the pool it wants. The pool's
   `spec.type` determines the topology; for example, `crc-pool` below
   names a `crc` pool:

   ```yaml
   apiVersion: guestcluster.opdev.io/v1alpha1
   kind: ClusterLease
   metadata:
     name: ci-job-1234-lease
   spec:
     poolRef:
       name: crc-pool
     ttl: 2h               # optional safety-net auto-release
     requestedBy: ci-job-1234
   ```

   ```sh
   kubectl apply -f lease.yaml
   ```

2. CI polls the lease until it's bound:

   ```sh
   kubectl wait --for=jsonpath='{.status.phase}'=Bound clusterlease/ci-job-1234-lease --timeout=20m
   ```

3. CI reads the **explicit outputs** off the lease status and the
   kubeconfig Secret it references:

   ```sh
   kubectl get clusterlease ci-job-1234-lease -o jsonpath='{.status.topology}'   # crc | hcp
   kubectl get clusterlease ci-job-1234-lease -o jsonpath='{.status.ocpVersion}' # e.g. 4.16.0
   SECRET=$(kubectl get clusterlease ci-job-1234-lease -o jsonpath='{.status.kubeconfigSecretRef.name}')
   kubectl get secret "$SECRET" -o jsonpath='{.data.kubeconfig}' | base64 -d > kubeconfig
   KUBECONFIG=./kubeconfig oc get nodes
   ```

   Compare `status.ocpVersion` and `status.topology` against the
   artifact's documented OCP line. A mismatch is a hard fail or a
   documented skip, per your CI policy. The operator surfaces the
   mismatch as a non-fatal `VersionMismatch` condition on the underlying
   `ClusterInstance`. It does not decide pass or fail for you.

4. When the job is done, CI deletes the lease to release the slot:

   ```sh
   kubectl delete clusterlease ci-job-1234-lease
   ```

   This step is safe even without an explicit "release" step. Deleting
   the lease always deletes the bound instance, through a finalizer.
   `ClusterPoolReconciler` then provisions a fresh replacement, if
   needed, to top back up to `minSize` or `warmSpares`.

### Concurrency / isolation

Two jobs that request leases from the same pool at the same time bind to
**two different** `ClusterInstance`s. Each instance gets its own unique
VM name and network endpoint for CRC, or its own `HostedCluster` and
`NodePool` names for HyperShift. This holds as long as the pool has
enough capacity, or can grow to enough capacity within `maxSize`. Binding
stays race-safe through Kubernetes optimistic concurrency:
`resourceVersion` conflicts on `ClusterInstance.status` retry naturally.
No manual locking is required. Each pool's `spec.maxSize` alone enforces
the hypervisor budget. Size your pools to your hypervisor's real capacity.

## Uninstall

```sh
kubectl delete -k config/samples/
make uninstall
make undeploy
```

## Repository layout

```
api/v1alpha1/                     ClusterPool, ClusterInstance, ClusterLease, CRCBundle types
internal/resources/               Builder helpers for KubeVirt/CDI/HyperShift/CRCBundle objects
internal/controller/              The four reconcilers
cmd/main.go                       Manager entrypoint (registers all schemes)
cmd/crc-agent/main.go            Standalone CRC-VM kubeconfig publisher (also reused, via
                                   command/args override, as the CRCBundle bundle-prep Job)
Dockerfile.crc-agent              Multi-stage image for cmd/crc-agent + oc
config/crd, config/rbac, ...      Generated manifests (make manifests / make generate)
config/samples/                   Example CRs for crc (turnkey + manual), hcp, CRCBundle
```

## Known limitations / TODOs

- The operator observes the crc-agent Job's failure only indirectly. If
  the post-boot fixups permanently fail, once the Job's `backoffLimit` is
  exhausted, the raw kubeconfig Secret never appears. The
  `ClusterInstance` then stays `Provisioning`, with periodic requeue,
  instead of moving to a hard `Failed` state. A follow-up should watch
  the Job's status directly and surface a clear failure condition.
- The operator intentionally fixes the `<instance>-crc-api` `Route`'s hostname at
  creation time, from the management cluster's ingress domain at that
  moment. If the management cluster's own `ingresses.config.openshift.io`
  domain later changes, existing instances keep their original Route
  host. They do not migrate.
- IDMS/ICSP mirror configuration (`template.idmsRef`) is wired through to
  `HostedCluster.spec.imageContentSources` as a structural stub only.
  Parsing an actual `ImageDigestMirrorSet` or `ImageContentSourcePolicy`
  object into that field is a follow-up.
- Controllers currently poll rather than use `Owns` or `Watches` on the
  KubeVirt/HyperShift backing objects: every 20s for `ClusterInstance`,
  every 10s or 30s for `ClusterLease`. Given multi-minute provisioning
  times, this polling is acceptable. Event-driven watches would still
  reduce convergence latency further.
- The turnkey CRC bundle path's cross-namespace PVC clone relies on a
  single, cluster-wide `datavolumes/source: create` RBAC grant, for the
  manager's `ServiceAccount` (see the `+kubebuilder:rbac` marker on
  `ClusterInstanceReconciler`). This follows CDI's documented
  cross-namespace-clone authorization pattern. This project has not
  independently re-verified the pattern against CDI's source code. If
  clones fail unexpectedly with a permission error, confirm this pattern
  against your CDI/OpenShift Virtualization version.
- `Dockerfile.crc-agent` fetches the `oc` CLI from the `stable-4` mirror
  channel at image-**build** time. This CLI is not pinned to any
  particular CRC bundle's exact OpenShift version. This should remain
  compatible with the simple `oc get clusterversion`, `oc apply`, and
  `oc patch` operations that crc-agent and the bundle-prep Job perform.
  This compatibility is not guaranteed across all future OpenShift
  version skew. If this becomes a problem, rebuild the image
  periodically, or pin `OC_CHANNEL`, or vendor your own `oc` binary.

## License

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
