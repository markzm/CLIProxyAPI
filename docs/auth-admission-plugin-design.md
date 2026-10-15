# Auth Admission Plugin Design

Date: 2026-10-13

Status: Proposed design; no limiter implementation is included in this document.

## 1. Requirements and terminology

All callers keep using the same frontend API key. The limiter protects each
**upstream credential**, not that shared key and not an individual caller.

- `auth`: the selected upstream OAuth account or API-key credential record.
- `client`: a person, application, or device making requests to this proxy.
- `request_id`: one logical model execution, including its waiting period.
- `attempt_id`: one actual executor invocation, including retry invocations.
- `lease_id`: an opaque host-owned concurrency reservation for one attempt.

For a credential with concurrency limit `C` and queue limit `Q`:

1. Up to `C` attempts hold execution reservations.
2. Additional eligible requests wait in a bounded FIFO queue of at most `Q`.
3. When that queue is full, return HTTP 429 without invoking an upstream.
4. Persist a rejection event identifying the caller as accurately as available
   evidence permits. The shared API key alone cannot distinguish callers.

Example: with `C = 2` and `Q = 8`, two requests execute, eight wait, and the
eleventh simultaneous request receives 429 if all target that same credential.
Another credential has a separate budget.

This is a concurrency admission mechanism, not requests-per-minute throttling.
Token quotas and client-specific rate limits are separate, optional features.

## 2. Decision: plugin policy, host enforcement

Implement a native CLIProxyAPI plugin named `auth-admission`. Do not implement
this as a Codex plugin or as an executor that replaces every provider.

The plugin owns configuration validation, immutable policy publication, client
identity policy, and audit formatting. The host owns waiting queues, atomic
reservations, cancellation, attempt cleanup, and durable audit delivery.

```text
shared API key authentication
    -> capture and validate client identity
    -> resolve model and eligible credentials
    -> preserve scheduler/pinning rules
    -> selected-credential admission
         -> reserve immediately
         -> wait in bounded FIFO
         -> persist rejection and return 429
    -> executor invocation with an attempt lease
    -> upstream cleanup and exactly-once lease release
    -> observational lifecycle/usage notifications
```

### Why existing hooks are insufficient

| Existing integration | Useful behavior | Correctness limitation |
| --- | --- | --- |
| `RequestInterceptor` | Can inspect selected-auth metadata and return a structured termination response | No paired per-attempt release; interceptor errors currently fail open |
| `RequestLifecyclePlugin` | Receives request completion | Asynchronous, request-level, and dependent on the active plugin instance |
| `Scheduler` | Chooses an available credential | No lease or rollback contract; selecting is not executing |
| `UsagePlugin` | Observes upstream usage | Rejected requests never invoked an upstream and must not become billed usage |
| `Options.ExecutionLifecycle` | Provides host-owned resource cleanup | Currently Home-specific; retained sessions are not per-response reservations |

The existing `examples/plugin/request-lifecycle` example is a useful admission
demonstration, but it is not a strict per-auth queued limiter. In particular,
do not release concurrency solely from `request.complete`, a usage event, the
first error chunk, or a selected-auth callback.

The native C ABI does not transmit a cancellable Go context into a blocking
plugin function. Waiting inside a native plugin RPC can outlive the caller even
when the host wrapper stops waiting. Keep all queue waits in host Go code.

## 3. Proposed plugin contract and configuration

The following capability, RPC method, and configuration fields are **proposed**;
they are not supported by the current host.

- Add `AuthAdmissionPolicy` to `pluginapi.Capabilities`.
- Add `admission.policy_snapshot`, called during registration/reconfiguration,
  returning a complete validated policy and its revision.
- Negotiate a new RPC schema version, currently expected to be version 7 because
  this checkout uses version 6. Recheck the next available version at implementation.
- Keep native C ABI version 1: this extension changes JSON RPC, not C exports.
- Add the generic plugin-instance field `required` to host configuration.
- Allow only one authoritative admission-policy plugin in the first release.

Do not call policy RPC for every request. Publish an immutable snapshot into the
host and evaluate it locally; no plugin callback may own an execution lease.

```yaml
plugins:
  enabled: true
  dir: "plugins"
  configs:
    auth-admission:
      enabled: true
      required: true
      priority: 100
      scope: "node"
      defaults:
        max_concurrency: 2
        max_queue: 8
        max_wait: "30s"
      overrides:
        - provider: "codex"
          auth_id: "configured-stable-auth-id"
          max_concurrency: 4
          max_queue: 16
      selection_mode: "respect-scheduler"
      include_token_count: true
      global:
        max_total_queued: 1024
        max_total_queued_bytes: 134217728
        max_request_body_bytes: 8388608
      identity:
        mode: "claimed"
        client_id_header: "X-Client-ID"
        missing_client_id: "allow-and-mark-unknown"
        max_client_id_length: 128
      audit:
        file: "logs/auth-admission-rejections.jsonl"
        durable_rejections: true
        on_write_failure: "reject-503"
      retry_after_seconds: 1
```

Values above are examples, not workload-derived production limits. Large image,
audio, and multipart requests require an appropriate body-size allowance.

### Configuration rules

- `max_concurrency >= 1`, `max_queue >= 0`; zero queue means immediate rejection
  when all execution reservations are busy. Disable the plugin explicitly to
  disable enforcement; do not overload zero concurrency to mean unlimited.
- Reject negative, overflowing, duplicate, and unknown policy fields. Validate
  durations, finite global bounds, override identifiers, and writable audit paths.
- Override precedence: exact provider/auth match, provider default if introduced,
  then global default. Omitted override fields inherit, including explicit zero queue.
- `max_wait` is optional; zero disables the queue timer. It measures queue residence
  only, before that attempt invokes an executor. It is not an upstream deadline.
- Scope and unsupported-feature combinations are validated before readiness.
- `required: true` must be understood by the host even if plugin loading fails.
  Merely passing that field to plugin YAML does not provide fail-closed behavior.

On required-plugin startup failure, fail startup/readiness rather than continuing
unlimited. On bad reload, retain the last-good snapshot, leases, and queues and
report degraded health. If no valid policy exists, return 503 for covered model
execution. Missing plugin, audit failure, and unsupported coverage are not 429.

## 4. Credential identity and scheduling

Default bucket key: `(canonical_provider, auth.ID)`. Aggregate all models,
sessions, clients, and frontend API keys using that credential into the same
bucket. Token refresh and cloned auth objects must not create new buckets.

Do not use model names, access tokens, file modification times, pointer identity,
client IDs, or the shared frontend API key as the bucket key. If duplicate auth
records represent the same physical account, require an administrator-controlled
canonical credential mapping before promising account-wide limits.

### MVP: preserve routing decisions

The initial `respect-scheduler` mode admits the credential already selected by
the existing routing/scheduler. If it is busy, queue for that credential. Other
credentials may remain idle; that is an explicit compatibility tradeoff, not a
claim that the entire provider pool is saturated.

Pinned credentials, sticky sessions, priority tiers, model compatibility,
cooldown, provider restrictions, and custom scheduler decisions remain binding.
Queue-full means this routing decision has exhausted its waiting budget.

An optional later `available-first` mode can filter the permitted candidate
pool before ordinary selection, retry atomic reservation against untried ready
candidates once, and queue only if no allowed candidate can be reserved. This
requires explicit scheduler compatibility; never silently override custom auth
selection or session affinity. Capacity snapshots are advisory: reservation must
still be atomic, and selection retries must be bounded rather than busy-spinning.

Do not mark a busy credential unhealthy, spend upstream retry budget on local
queue saturation, or try unrelated credentials after a terminal local rejection.

## 5. Queue and reservation algorithm

Use a host-owned controller and buckets with a linked FIFO, a mutex, active
reservation count, and an opaque lease table. One waiter belongs to exactly one
bucket. A waiter contains identifiers and references to existing request data,
not extra prompt/body copies.

Use a controller admission lock for atomic updates to global queue counts/bytes
and bucket admission state in the MVP. This is a short critical section; waiting,
disk writes, credential refresh, plugin callbacks, and network I/O occur outside
it. A later sharded implementation must preserve these same atomic invariants.

```text
Acquire(context, attempt):
    lock admission state
    reject canceled context before mutating counters
    find registered bucket and effective policy
    if queue is empty and active reservations < concurrency limit:
        allocate unique lease and increment active reservations
        unlock and return lease
    if per-auth count, global count, or global byte budget would overflow:
        capture bounded rejection event
        unlock
        durably append rejection
        return terminal local admission error
    append waiter and reserve global queue count/bytes atomically
    unlock
    wait for grant, caller cancellation, or queue-only timer
    resolve the winning transition under the admission lock
    return lease or terminal cancellation/expiration error

Release(lease):
    lock admission state
    ignore duplicate release using lease ownership/state
    decrement active reservations exactly once
    while capacity permits and FIFO has live waiters:
        unlink oldest waiter and remove global queue count/bytes
        allocate its lease and increment active reservations before notification
        mark waiter granted and signal its buffered notification
    unlock
```

### State machine and races

```text
new -> granted -> running -> released
new -> queued -> granted -> running -> released
new -> rejected
queued -> canceled | expired | unavailable
granted -> canceled-before-run -> released
```

- FIFO applies to admission order within one auth; new arrivals cannot steal a
  freed slot from waiters. Different auths progress independently.
- Count granted reservations as active even before their goroutines run.
- Cancellation and grant are resolved under the same lock. A grant delivered
  concurrently with cancellation must be explicitly returned/reclaimed; it must
  never disappear when a `select` chooses the cancellation branch.
- Remove canceled waiters in constant time and reclaim global bytes/count once.
- Recheck cancellation before invocation. If cancellation races after invocation
  starts, retain the lease until the execution actually cleans up.
- Revalidate credential eligibility after a queue grant. A removed credential
  cannot execute from a stale snapshot. If preparation must be rebuilt after a
  token/configuration revision changes, release the reservation and re-enter the
  preparation/admission path with a new attempt ID rather than mutating a final
  provider payload or executing with invalidated auth state.
- Notifications cannot block under the lock. Do not close a shared notification
  channel from competing release/cancel paths.
- Once granted, stop the queue timer. Do not propagate its deadline into the
  executor context. Preserve caller cancellation with a cancel-only child context.
- An expired queue returns audited local 429 with a distinct reason; a client
  disconnect is cancellation, not a newly generated 429 to a closed connection.

For a fixed policy, `active <= C`, `queued <= Q`, and global queue budgets never
overflow. After decreasing limits, existing reservations/waiters may temporarily
exceed the new values: do not kill active upstream work or evict accepted waiters.
Block new grants/appends as appropriate until counts drain below their new limits.

Queue records must not survive process restart as runnable work: their HTTP
clients and cancellation ownership are gone. Audit records do survive restart.

Account for all serialized buffers retained by queued work, including original
requests and existing clones; payload length alone can underestimate retained
memory. Enforce the request-size limit before unbounded body reads. Queue budgets
do not bound concurrent pre-admission parsing, so deployments must also cap ingress
body-reader concurrency if they need a whole-process memory bound.

## 6. Execution ownership and retry correctness

Introduce a small host interface, conceptually:

```text
AdmissionGate.Acquire(ctx, AttemptInfo) -> Lease or AdmissionError
Lease.Release(outcome)
```

`AttemptInfo` includes host-generated request/attempt IDs, provider/auth key,
operation, policy revision, bounded client metadata, retained-byte accounting,
and internal execution-source information. Lease objects remain in Go, never
JSON metadata, payloads, plugin RPC, or upstream headers.

Acquire at the actual invocation boundary after auth preparation and after-auth
interception. Apply it to **every** invocation, not only the first call in an
outer retry loop. The 401 refresh-and-retry paths can invoke executors again
without another after-auth interception.

| Operation | Hold interval |
| --- | --- |
| `Execute` | Before invocation until executor return and synchronous cleanup |
| `CountTokens` via `ExecuteCount` | Same; MVP conservatively includes local counters |
| `ExecuteStream` setup failure | Until failed setup resources are closed |
| `ExecuteStream` successful setup | Transfer ownership to the original producer/cleanup lifecycle |
| Retry or credential failover | Release ended attempt, then separately admit the next invocation |
| WebSocket generation | One response/turn, not the lifetime of an idle persistent socket |

Returning `StreamResult` or seeing an error chunk is **not** producer completion.
Wrap the original executor source, not only the manager's downstream channel.
Cancellation initiates transport cleanup and draining, but does not by itself
prove cleanup has finished. Keep the permit until source termination and resource
closure are established. Audit and alert stuck producers; do not use a lease TTL
to silently declare an active upstream finished.

Define and test the source-close/cleanup contract for all supported executors.
If source closure does not prove resource closure for a path, add an explicit
attempt-cleanup signal there before enabling strict enforcement. Keep this
attempt ownership separate from retained Home/WebSocket session ownership.

The guarantee is a bound on host-managed upstream execution. It cannot prove a
remote provider stopped processing after a lost connection, nor constrain
arbitrary networking performed by untrusted native plugin code.

### Terminal local errors

Add a typed `AdmissionError` with scope `local_admission`, reason, status, safe
headers, and optional direct protocol-compatible body. Classify it before normal
retry, bootstrap retry, quota reporting, cooldown, and `OnResult` processing.

Check this classification in inner model/auth loops as well as outer manager
loops. In particular, the current stream inner loop can continue credential
selection after an interceptor termination; the new terminal classification must
close that gap rather than relying only on the outer `ExecuteStream` check.

An admission rejection never reaches upstream usage billing or credential-health
accounting. Releasing a lease and persisting audit are independent of whether an
observational completion plugin is present, fused, replaced, or removed.

## 7. Caller identification with one shared API key

Keep ordinary shared-key authentication mandatory. Client attribution is an
additional layer, not a replacement authenticator.

| Identity mode | Evidence | Trust |
| --- | --- | --- |
| `claimed` | Caller sends `X-Client-ID`, such as `team-a-laptop` | Useful operational label; caller can impersonate another label |
| `trusted-gateway` | Authenticated gateway strips inbound identity headers and injects its verified subject | Verified only when direct bypass is blocked and gateway provenance is enforced |
| `signed` | Verify an independent identity proof with timestamp, nonce, and request binding | Requires provisioned proof credentials, replay handling, and client support |
| `unknown` | Source address, sanitized user agent, optional installation/session hint | Observed endpoint evidence, not verified person identity |

Recommended first release: support `claimed` plus explicit `unknown` fallback;
support `trusted-gateway` for deployments requiring trustworthy attribution.
Trusted proxy IP handling alone does not authenticate an arbitrary identity
header. Establish gateway provenance using network controls plus an authenticated
channel or verified signature, and strip attacker-supplied identity values.

If proof is required later, a signed request can bind client ID, timestamp, nonce,
method, canonical path, and body digest. The proof credential is separate from
the shared API key. Missing/invalid required identity should fail authentication
with 401/403, not become queue-full 429.

Capture identity from the inbound request once, before upstream header rewriting.
Propagate a typed, read-only identity snapshot through request/attempt context.
Nested execution inherits attribution, not mutable caller-controlled ownership.
Identity headers must not be sent upstream by default.

Do not rely on a non-exclusive frontend-auth plugin to reject missing/invalid
identity: its not-handled/error result can fall through to shared-key authentication.
The required identity check must be a mandatory host gate after shared-key auth.
An exclusive combined authenticator is another option only if it verifies both
the shared key and identity proof itself. Do not overwrite the authenticated
frontend principal merely to populate a client label: existing middleware also
uses that principal as usage API-key context. Explicitly propagate verified
`accessMetadata`, which is not automatically copied into execution metadata.
Bridge the identity snapshot explicitly from Gin into the context returned by
`GetContextWithCancel`, then into manager attempts and nested host executions.
That helper does not inherit arbitrary inbound request-context values, so merely
setting `c.Request.Context()` is insufficient.

Validate identifier length/character set, bound user-agent length, encode logs
as JSON, and record identity confidence. IP can be shared behind NAT, change
over time, or be forged through untrusted forwarding headers. User agent and
session IDs likewise cannot establish individual identity.

Do not introduce hard per-client quotas using unverified client IDs: clients
could evade them by inventing IDs or consume another client's quota by spoofing.

## 8. Rejection responses and audit durability

For an OpenAI-compatible request rejected before response commitment:

```http
HTTP/1.1 429 Too Many Requests
Content-Type: application/json
Cache-Control: no-store
Retry-After: 1
X-Request-ID: host-generated-request-id
```

```json
{
  "error": {
    "type": "rate_limit_error",
    "code": "auth_queue_full",
    "message": "The selected upstream credential is busy and its waiting queue is full.",
    "param": null
  }
}
```

Use existing provider-specific response envelopes for Claude/Gemini routes.
`Retry-After` is advisory, not a promised slot or queue position. Do not expose
credential IDs, account labels, other clients, or secret material in public errors.

Persist one local rejection event before committing its 429 response:

```json
{
  "schema_version": 1,
  "event_id": "host-generated-event-id",
  "event": "admission.rejected",
  "reason": "auth_queue_full",
  "time": "2026-10-13T10:00:00+08:00",
  "request_id": "host-generated-request-id",
  "attempt_id": "host-generated-attempt-id",
  "node_id": "proxy-01",
  "provider": "codex",
  "auth_id": "internal-credential-id",
  "requested_model": "client-requested-model",
  "route": "/v1/responses",
  "stream": true,
  "client_id": "team-a-laptop",
  "identity_confidence": "claimed",
  "remote_ip": "privacy-policy-controlled-address",
  "user_agent": "bounded-user-agent",
  "frontend_key_id": "operator-key-id-or-keyed-fingerprint",
  "active": 2,
  "queued": 8,
  "max_concurrency": 2,
  "max_queue": 8,
  "wait_ms": 0,
  "transport": "http",
  "response_http_status": 429,
  "error_status": 429,
  "attempt_upstream_attempted": false,
  "request_upstream_attempts": 0,
  "policy_revision": "revision-id"
}
```

MVP persistence is bounded JSONL records in a local append-only file. Append and
sync rejection records outside admission locks before returning 429. Rotate with
bounded retention and restrictive permissions. If persistence fails, return 503
and emit a critical operational diagnostic; do not claim a rejection was recorded.
Specify delivery as durable-before-response, not magical end-to-end exactly-once:
use event IDs to deduplicate exports after crashes or retries.

Serialize complete-record appends and include file creation/rotation metadata in
the persistence acknowledgment, using OS-correct file and parent-directory flush
semantics. Recover an interrupted final JSONL record before further appends;
never merge a torn tail with the next event. Actual durability depends on the
operating system and storage guarantees, not just a successful buffer write.

The example describes an initial HTTP attempt. A rejection on a later retry may
have `attempt_upstream_attempted: false` while `request_upstream_attempts > 0`.
For an upgraded WebSocket, record transport `websocket`, HTTP status 101, and
protocol error status 429 separately. Do not claim no earlier upstream work or
an HTTP 429 when the rejection is only an in-protocol WebSocket event.

Never record raw API keys, Authorization headers, OAuth/access tokens, request
bodies, or prompts. Log a configured key ID or keyed fingerprint only. The shared
key identifier will be the same for all clients and is not their identity. Route
templates exclude query strings; address retention/redaction follows deployment
privacy policy. Do not serialize an entire existing usage record into this log.
Use a dedicated rejection DTO/sink outside usage billing. Leaving an existing
usage record's `APIKey` blank is not redaction: usage publication and plugin
delivery can backfill it from context. Keep client identity, frontend-key
identifier, and authenticated usage principal separate.

Track counters/gauges for active, queued, rejected reason, cancellation, wait
duration, audit failures, policy health, and stuck cleanup. Keep client IDs/IPs
out of metric labels; they are high-cardinality caller-controlled data. Auth-level
labels require bounded cardinality and administrative access.

## 9. HTTP, streaming, WebSocket, and plugin route coverage

| Path family | Admission boundary and response rule |
| --- | --- |
| Chat/completions, Responses, Claude, Gemini | Manager invocation boundary, preserving entry protocol |
| HTTP SSE | Queue without writing headers, data, heartbeat, or flushing; reject with real HTTP 429 before commitment |
| Non-streaming keepalive | Do not start whitespace keepalive while admission or pre-output retry can still reject |
| Images/audio/media handled through Manager | Same selected-auth gate, plus appropriate body/byte bounds |
| Count tokens | Include conservatively in MVP; later exempt verified local-only implementations explicitly |
| Direct SDK Manager calls | Same host gate even without Gin context or HTTP client metadata |
| Direct plugin executor with `auth == nil` | Must declare a host-visible synthetic admission key or be rejected as unsupported in strict mode |
| Plugin `host.model.execute*` callbacks | Real nested managed execution gets its own attempt lease and inherited client identity |
| WebSocket before upgrade | Request-level authentication/policy failures can still use HTTP responses |
| WebSocket after upgrade | Per-turn queue-full emits protocol-native error carrying code/status 429; it cannot change the HTTP 101 to HTTP 429 |
| Raw HTTP passthrough or future bypass paths | Explicit gate coverage or strict-mode 503; never silently assume Manager coverage |

The current `StartNonStreamingKeepAlive` can write whitespace before execution
returns, committing a successful status while a request is queued. Fixing this
ordering is required. The safest strict MVP disables early keepalive for covered
requests; re-enable it only with a proven response-commit barrier covering all
retries. SSE heartbeats must likewise wait until rejection is no longer possible.
Ensure reverse-proxy idle settings can accommodate the chosen queue wait.

Nested orchestrator plugins must not hold an outer credential reservation and
then wait for the same credential in an inner host call when `C = 1`. Require a
host-validated orchestration mode that reserves only actual leaf upstream work,
or explicitly reject that unsupported nesting. Do not trust a caller-set metadata
flag to skip admission or accidentally double count an already leased transport.

No limiter may modify model payload semantics. Keep all provider normalization,
translation, injection, cleanup, and final user payload rules in their existing
ordering. Admission adds context/resource ownership, not post-rule payload edits.

## 10. Reload, shutdown, and distributed scope

### Single-node MVP

- Persist controller state across plugin reconfiguration/replacement, independent
  of native plugin pointers. Existing leases always release to their original
  host controller; a reload does not reset active counts.
- Increasing concurrency grants existing FIFO waiters first. Lowering concurrency
  or queue size drains existing work without killing established upstream calls.
- Removed/disabled auths reject new admission; queued requests receive audited
  availability failures, and active work releases naturally. Do not reassign a
  pinned request behind the caller's back.
- Explicit plugin disable switches new admission off but retains cleanup tracking
  until existing leases end. Unintentional unload/fusion retains last-good policy.
- Shutdown stops admission, cancels queued waiters, and lets active work drain
  according to existing service lifecycle. Do not add new upstream deadlines.
- Buckets are created only from registered/canonical credentials, not arbitrary
  headers. Retire a bucket only when no leases/waiters remain.

### Multiple replicas and Home

`scope: node` means per-process limits. If the same auth is used by `N` independent
replicas, aggregate concurrency can reach `N * C`. A Redis audit sink or shared
configuration does not turn local queues into distributed admission.

For the MVP, use one active proxy instance or partition credentials so a physical
credential has one admission owner. Reject activation in Home mode until the
policy/authority integration is explicitly implemented; do not compete with the
existing Home-authoritative credential-concurrency lifecycle.

For a later Home-global implementation, Home must own admission and queue order;
proxy nodes retain waiting HTTP connections and request bodies. Include canonical
credential IDs, queue tickets, caller cancellation, policy revision, and
exactly-once release acknowledgment. Update the CLIProxyAPIHome repository and
its tests as well as this repository. Keep per-response reservations separate
from retained session ownership and do not count one reservation twice.

If Redis is considered instead, use atomic scripts for global reservation and
bounded ticket transitions. A TTL alone cannot safely reclaim a possibly running
upstream attempt: without cleanup confirmation/fencing, expiration can produce
overlapping work. Document any best-effort guarantee; do not claim strict global
concurrency from a semaphore with automatic lease expiry.

## 11. Implementation seams and proposed write scope

The references below identify the current checkout, not implemented new APIs.

| Area | Existing seam | Planned change |
| --- | --- | --- |
| Plugin API | `sdk/pluginapi/types.go:72`, `sdk/pluginabi/types.go:6` | Snapshot capability, revision and schema negotiation |
| Plugin host | `internal/pluginhost/rpc_schema.go`, `rpc_client.go`, `config.go`, `host.go` | Load/validate required policy, last-good publication and readiness |
| Controller | New `sdk/cliproxy/admission/` | Queues, leases, typed errors, deterministic clock and audit interface |
| Configuration | `internal/config/config.go`, `config.example.yaml` | Required-plugin host semantics; example policy only after implementation |
| Native invocation | `sdk/cliproxy/auth/conductor_execution.go:595`, `:807` | All normal and refresh-retry invocation paths |
| Stream invocation | `sdk/cliproxy/auth/conductor_stream.go:265` | Original-source lease ownership and cleanup verification |
| Home execution | `sdk/cliproxy/auth/conductor_home_execution.go:237` | Reject unsupported authority in MVP; later integrate Home-global gate |
| Request metadata | `sdk/api/handlers/handlers.go:210` and `handlers_context.go` | Typed attribution propagation without exposing secrets |
| Keepalive/error handling | `sdk/api/handlers/handlers.go:602`, `handlers_errors.go:117` | Response-commit ordering and terminal admission errors |
| Direct plugin routes | `internal/pluginhost/executor_route.go:86` | Synthetic keys/strict coverage and nesting policy |
| Example plugin | New `examples/plugin/auth-admission/go/` and `README.md` | Native exports, snapshot provider and plugin validation tests |
| Service wiring | `sdk/cliproxy/service_plugins.go`, `service_executors.go` | Install controller without replacing native executor interfaces |

Keep implementation files outside `internal/runtime/executor/`; only execution
contract fixes and their unit tests belong there, with helper files under
`internal/runtime/executor/helps/` if needed. Do not make standalone translator
changes. Do not add or modify deprecated `/v0/management` endpoints.

Initial configuration uses existing plugin YAML and v8 configuration operations.
Observability uses audit files and the existing logging/metrics integration. A
new authenticated `/v8` diagnostics API can be a later, separately scoped change;
the current plugin ManagementAPI routing base is legacy v0 and is not suitable
for adding this feature under the project rules.

Build the proposed native artifact with `-buildmode=c-shared`, matching the
existing plugin examples, OS/architecture, and Go version. Artifact basename
must be `auth-admission` to match its configuration ID. Publish native artifacts
and compatibility requirements through the existing plugin-store mechanism.

## 12. Delivery phases and acceptance tests

### Delivery phases

1. **Host contract and safety:** policy snapshot, required-plugin semantics,
   typed local errors, admission controller, identity snapshot, durable audit.
2. **Invocation integration:** non-stream/count/stream, 401 retry, inner-loop
   termination, source cleanup, and keepalive response-commit regression tests.
3. **Plugin and operational release:** configuration/override validation, example
   native artifact, shadow observation, canary enforcement, documented rollback.
4. **Optional extensions:** availability-aware selection, gateway/signed identity,
   strict native WebSocket turns/direct plugin coverage, and Home-global queues.

Each release advertises a coverage matrix. If a strict deployment enables a
not-yet-supported route, reject it explicitly instead of silently bypassing.
Shadow mode observes without blocking and makes no enforcement guarantee.

### Required deterministic tests

Use barriers, controllable clocks, explicit producer closure, and fake audit
sinks. No wall-clock sleeps for queue ordering, expiration, or capacity tests.

1. For `C = 2, Q = 8`, exactly two attempts enter the blocked fake executor, eight
   remain queued, and additional requests receive audited 429 with no upstream call.
2. FIFO release admits the oldest live waiter; new arrivals cannot bypass it.
3. Different credentials progress independently; different models/clients using
   one credential share the same capacity.
4. Concurrent enqueue respects both per-auth and global count/byte budgets.
5. Canceled/expired waiters reclaim queue counts and bytes exactly once.
6. Cancellation racing with grant cannot leak a lease or oversubscribe capacity.
7. Duplicate release and duplicate observational completion cannot underflow.
8. Lowered limits drain safely; raised limits serve existing FIFO waiters first.
9. Missing/fused/replaced required plugin cannot disable enforcement implicitly;
   invalid reload preserves last-good policy and active ownership.
10. Normal errors, 401 refresh retry, auth failover, model-pool retry, and handler
    stream-bootstrap retry acquire/release the correct per-invocation leases.
11. Local queue-full/expiry does not trigger upstream retries, cooldown, health
    penalties, or token billing; inner stream loops propagate terminal errors.
12. Returning a stream handle and emitting an error chunk do not release a lease;
    the original producer/resource cleanup does.
13. A canceled stream that is still cleaning up holds its reservation; draining
    terminates without a leaked relay goroutine when the producer closes.
14. SSE and non-streaming keepalive cannot commit HTTP 200 while waiting or before
    a possible later admission rejection. Verify status, headers, and body.
15. OpenAI/Claude/Gemini error envelopes and pre-/post-upgrade WebSocket behavior
    preserve protocol semantics; no false claim of HTTP 429 after HTTP 101.
16. Shared frontend key plus distinct client IDs produces separate attribution,
    but one auth capacity pool; absent ID is explicitly marked unknown.
17. Spoofed forwarding/identity headers cannot become verified identities; logs
    omit API keys, tokens, prompts, query strings, and unbounded metadata.
18. Audit append/sync succeeds before 429 delivery; disk failure yields 503 and an
    operational failure signal, not a falsely recorded rejection.
19. Direct SDK/plugin/nested paths cannot bypass strict coverage; `C = 1` nesting
    cannot deadlock through an outer reservation waiting on its own inner call.
20. Shutdown/auth removal clears waiters and drains leases without new upstream
    deadlines; reload and idle WebSocket sessions do not reset/hold response permits.

After code implementation, run targeted tests, race-enabled admission and stream
tests, `gofmt`, and the required server build. Do not present this design document
as an implemented or production-validated limiter.
