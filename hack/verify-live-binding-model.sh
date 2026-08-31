#!/usr/bin/env bash
#
# Distills the manual live-cluster verification matrix used to validate the
# single-source-of-truth ClusterLease<->ClusterInstance binding model (see
# README.md's "Binding model" section and clusterlease_types.go's
# Status.InstanceRef doc) into a repeatable smoke test against a REAL
# cluster with the operator already running (via `make run` or deployed).
#
# This complements, and does not replace, the fast/hermetic Go tests in
# internal/controller/clusterpool_verification_test.go,
# internal/controller/clusterlease_verification_test.go, and
# internal/controller/clusterinstance_leaseref_projection_test.go, which
# exercise the exact same scenarios in milliseconds against envtest/a fake
# client. Run this script when you specifically need to validate against
# real backing infrastructure (KubeVirt VMs, HyperShift, a real apiserver
# under real load) rather than logic in isolation.
#
# By default this only runs the FAST checks, which observe pool accounting
# immediately after a spec change (no VM boot required): the AtCapacity
# condition, the warmSpares floor, and the minSize floor. Pass --full to
# additionally run the on-demand-bind-with-no-phantom-instance check, which
# waits for a real ClusterInstance to boot (can take ~15 minutes for a CRC
# topology) and is the strongest real-world regression signal for the
# phantom-instance race this binding model eliminates.
#
# Usage:
#   ./hack/verify-live-binding-model.sh [--full]
#
# Environment variables:
#   NAMESPACE      Namespace the ClusterPool lives in (default: guestcluster-operator-system)
#   POOL           ClusterPool name to test against (default: crc-pool)
#   KUBECTL_BIN    kubectl-compatible binary to use (default: auto-detect oc, else kubectl)
#   WAIT_TIMEOUT   Seconds to wait for a ClusterInstance to reach Ready in --full mode (default: 1200)
#
# The script restores the ClusterPool's original maxSize/minSize/warmSpares
# and deletes every test ClusterLease it created, even on failure (via a
# trap), so it is safe to run against a pool already serving real demand --
# though running it against an IDLE pool is strongly recommended, since
# --full provisions and boots a real instance. Note: the minSize/warmSpares
# floor tests may leave one freshly-provisioned ClusterInstance behind after
# the spec is restored to its original values; this is expected and requires
# no manual cleanup -- ClusterPoolReconciler's scale-down stability window
# (see scaleDownStabilityPeriod in clusterpool_controller.go) will trim it
# automatically a short while after it reaches Ready.

set -euo pipefail

NAMESPACE="${NAMESPACE:-guestcluster-operator-system}"
POOL="${POOL:-crc-pool}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-1200}"
FULL=false

for arg in "$@"; do
  case "$arg" in
    --full) FULL=true ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done

if [ -n "${KUBECTL_BIN:-}" ]; then
  KC="$KUBECTL_BIN"
elif command -v oc >/dev/null 2>&1; then
  KC="oc"
else
  KC="kubectl"
fi

TEST_LEASES=()
ORIG_MAX_SIZE=""
ORIG_MIN_SIZE=""
ORIG_WARM_SPARES=""

log() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
pass() { printf '\033[1;32m  PASS:\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m  FAIL:\033[0m %s\n' "$*"; exit 1; }

pool_json() { "$KC" get clusterpool "$POOL" -n "$NAMESPACE" -o json; }
pool_field() { pool_json | python3 -c "import json,sys; print(json.load(sys.stdin)$1)"; }
instance_count() { "$KC" get clusterinstance -n "$NAMESPACE" -l "opdev.io/pool=$POOL" --no-headers 2>/dev/null | wc -l | tr -d ' '; }

cleanup() {
  log "Cleaning up test resources"
  for l in "${TEST_LEASES[@]:-}"; do
    [ -n "$l" ] && "$KC" delete clusterlease "$l" -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true
  done
  if [ -n "$ORIG_MAX_SIZE" ]; then
    "$KC" patch clusterpool "$POOL" -n "$NAMESPACE" --type=merge -p \
      "{\"spec\":{\"maxSize\":$ORIG_MAX_SIZE,\"minSize\":$ORIG_MIN_SIZE,\"warmSpares\":$ORIG_WARM_SPARES}}" \
      >/dev/null 2>&1 || true
    echo "  restored ClusterPool $POOL to maxSize=$ORIG_MAX_SIZE minSize=$ORIG_MIN_SIZE warmSpares=$ORIG_WARM_SPARES"
  fi
}
trap cleanup EXIT

log "Preflight: checking ClusterPool $POOL in namespace $NAMESPACE"
"$KC" get clusterpool "$POOL" -n "$NAMESPACE" >/dev/null || fail "ClusterPool $POOL not found in $NAMESPACE"
ORIG_MAX_SIZE="$(pool_field "['spec']['maxSize']")"
ORIG_MIN_SIZE="$(pool_field "['spec'].get('minSize', 0)")"
ORIG_WARM_SPARES="$(pool_field "['spec'].get('warmSpares', 0)")"
echo "  original spec: maxSize=$ORIG_MAX_SIZE minSize=$ORIG_MIN_SIZE warmSpares=$ORIG_WARM_SPARES"
pass "pool found"

# --- Test: warmSpares floor ---------------------------------------------
log "Test: warmSpares floor triggers top-up with zero pending demand"
BEFORE=$(instance_count)
"$KC" patch clusterpool "$POOL" -n "$NAMESPACE" --type=merge -p \
  "{\"spec\":{\"maxSize\":$((ORIG_MAX_SIZE > 4 ? ORIG_MAX_SIZE : 4)),\"warmSpares\":1}}" >/dev/null
sleep 8
AFTER=$(instance_count)
if [ "$AFTER" -gt "$BEFORE" ]; then
  pass "warmSpares=1 created a new instance (before=$BEFORE after=$AFTER)"
else
  fail "expected instance count to increase after setting warmSpares=1 (before=$BEFORE after=$AFTER)"
fi
"$KC" patch clusterpool "$POOL" -n "$NAMESPACE" --type=merge -p '{"spec":{"warmSpares":0}}' >/dev/null

# --- Test: minSize floor -------------------------------------------------
log "Test: minSize floor triggers top-up with zero pending demand and warmSpares=0"
BEFORE=$(instance_count)
"$KC" patch clusterpool "$POOL" -n "$NAMESPACE" --type=merge -p '{"spec":{"minSize":1,"warmSpares":0}}' >/dev/null
sleep 8
AFTER=$(instance_count)
if [ "$AFTER" -ge 1 ]; then
  pass "minSize=1 kept/created at least one instance (total=$AFTER)"
else
  fail "expected at least one instance with minSize=1 (total=$AFTER)"
fi
"$KC" patch clusterpool "$POOL" -n "$NAMESPACE" --type=merge -p '{"spec":{"minSize":0}}' >/dev/null

# --- Test: AtCapacity / CapacitySufficient condition ---------------------
log "Test: CapacityAvailable condition reports AtCapacity when saturated, CapacitySufficient once relieved"
CURRENT_TOTAL=$(instance_count)
CAP_MAX=$((CURRENT_TOTAL > 0 ? CURRENT_TOTAL : 1))
"$KC" patch clusterpool "$POOL" -n "$NAMESPACE" --type=merge -p "{\"spec\":{\"maxSize\":$CAP_MAX}}" >/dev/null

LEASE_A="verify-atcapacity-a-$$"
LEASE_B="verify-atcapacity-b-$$"
for l in "$LEASE_A" "$LEASE_B"; do
  TEST_LEASES+=("$l")
  cat <<EOF | "$KC" apply -f - >/dev/null
apiVersion: guestcluster.opdev.io/v1alpha1
kind: ClusterLease
metadata:
  name: $l
  namespace: $NAMESPACE
spec:
  type: crc
  poolRef:
    name: $POOL
  requestedBy: "verify-live-binding-model"
EOF
done
sleep 8
REASON="$(pool_json | python3 -c "
import json,sys
d=json.load(sys.stdin)
for c in d.get('status',{}).get('conditions',[]):
    if c['type']=='CapacityAvailable':
        print(c['reason'])
        break
")"
if [ "$REASON" = "AtCapacity" ]; then
  pass "condition reports AtCapacity when saturated"
else
  fail "expected CapacityAvailable reason=AtCapacity, got '$REASON'"
fi

"$KC" delete clusterlease "$LEASE_A" "$LEASE_B" -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1
TEST_LEASES=()
"$KC" patch clusterpool "$POOL" -n "$NAMESPACE" --type=merge -p "{\"spec\":{\"maxSize\":$ORIG_MAX_SIZE}}" >/dev/null
sleep 5
REASON="$(pool_json | python3 -c "
import json,sys
d=json.load(sys.stdin)
for c in d.get('status',{}).get('conditions',[]):
    if c['type']=='CapacityAvailable':
        print(c['reason'])
        break
")"
if [ "$REASON" = "CapacitySufficient" ]; then
  pass "condition returns to CapacitySufficient once relieved"
else
  fail "expected CapacityAvailable reason=CapacitySufficient after relief, got '$REASON'"
fi

# --- Test (--full only): on-demand bind with no phantom instance --------
if [ "$FULL" = true ]; then
  log "Test (--full): on-demand provisioning binds with NO phantom instance (waits for real boot, up to ${WAIT_TIMEOUT}s)"
  "$KC" patch clusterpool "$POOL" -n "$NAMESPACE" --type=merge -p '{"spec":{"minSize":0,"warmSpares":0}}' >/dev/null
  BEFORE=$(instance_count)

  LEASE="verify-ondemand-$$"
  TEST_LEASES+=("$LEASE")
  cat <<EOF | "$KC" apply -f - >/dev/null
apiVersion: guestcluster.opdev.io/v1alpha1
kind: ClusterLease
metadata:
  name: $LEASE
  namespace: $NAMESPACE
spec:
  type: crc
  poolRef:
    name: $POOL
  requestedBy: "verify-live-binding-model-full"
EOF

  sleep 8
  AFTER_CREATE=$(instance_count)
  EXPECTED=$((BEFORE + 1))
  if [ "$AFTER_CREATE" -ne "$EXPECTED" ]; then
    fail "expected exactly one new instance immediately after creating the pending lease (before=$BEFORE after=$AFTER_CREATE)"
  fi
  pass "exactly one instance provisioned on demand (total=$AFTER_CREATE)"

  echo "  waiting up to ${WAIT_TIMEOUT}s for the lease to reach Bound (real VM boot in progress)..."
  ELAPSED=0
  PHASE=""
  while [ "$ELAPSED" -lt "$WAIT_TIMEOUT" ]; do
    PHASE="$("$KC" get clusterlease "$LEASE" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    if [ "$PHASE" = "Bound" ]; then
      break
    fi
    CURRENT=$(instance_count)
    if [ "$CURRENT" -gt "$EXPECTED" ]; then
      fail "phantom instance detected while waiting for bind: expected total=$EXPECTED, observed total=$CURRENT"
    fi
    sleep 15
    ELAPSED=$((ELAPSED + 15))
  done

  if [ "$PHASE" != "Bound" ]; then
    fail "lease did not reach Bound within ${WAIT_TIMEOUT}s (last phase: '$PHASE') -- check manager logs"
  fi

  FINAL=$(instance_count)
  if [ "$FINAL" -ne "$EXPECTED" ]; then
    fail "phantom instance detected at bind time: expected total=$EXPECTED, observed total=$FINAL"
  fi
  pass "bound successfully with no phantom instance (total=$FINAL)"

  "$KC" delete clusterlease "$LEASE" -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1
  TEST_LEASES=()
else
  log "Skipping --full on-demand-bind check (pass --full to run it; requires a real VM boot, up to ~${WAIT_TIMEOUT}s)"
fi

log "All checks passed."
