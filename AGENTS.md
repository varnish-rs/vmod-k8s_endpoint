# AGENTS.md

Guidance for AI agents working on this repository.

## What this is

`vmod-k8s-endpoint` is a Varnish VMOD (compiled as a `cdylib`) that watches a Kubernetes service's `Endpoints` resource and exposes the live pod addresses as a randomly-selected Varnish director. All source lives in `src/lib.rs`.

## Build

```bash
cargo check          # type-check without linking (fast; use this to verify changes)
cargo build          # debug build
cargo build --release
```

`cargo build` requires Varnish dev headers installed on the host. `cargo check` is enough to confirm correctness.

## Architecture

```
VmodDirector::new()          (VCL init, synchronous)
  └─ spawns watch_endpoints  (Tokio background task)
       └─ kube watcher stream
            ├─ on Apply/InitApply → create/revive NativeBackend, update shared map
            ├─ on Delete        → clear shared map, mark owned backends for GC
            └─ on GC timer      → drop NativeBackends past grace period

VmodDirector::backend()      (VCL request path)
  └─ lock shared map, pick random SendableBackendRef, return inner BackendRef
```

Key types and functions (all in `src/lib.rs`):
- `ServiceUri` — parses `[http[s]://]service-name` URIs; extracts the service name and whether TLS is requested.
- `extract_endpoints()` — filters a Kubernetes `EndpointSlice` down to `ip:port` strings matching a named port.
- `watch_endpoints()` — async loop; consumes the kube watcher stream, drives the three-phase backend pool update.
- `OwnedBackend` — owns a `NativeBackend` + its GC expiry deadline. Lives only inside the watcher task.
- `VmodDirector` — VCL-visible object; owns the Tokio `Runtime` (keeping it alive keeps the watcher running) and the shared backend map. Constructor takes `service_uri`, `port_name`, and optional `namespace`.
- `SendableBackendRef` — newtype over `BackendRef` with `unsafe impl Send`. Needed because `BackendRef` contains C raw pointers that Rust conservatively marks `!Send`; Varnish's own internal locking makes cross-thread use safe.
- `RawVclPtr` — newtype over `*mut ffi::vcl` with `unsafe impl Send`, for the same reason.

## Patterns to follow

- **One runtime per director.** `VmodDirector` holds its own `tokio::runtime::Runtime`. VCL init is synchronous; the Tokio runtime hosts all async work for that director instance.
- **GC grace period.** Evicted backends stay in `owned` for `GC_GRACE` (1 s) before being dropped. This lets in-flight Varnish requests finish against a backend that just left Kubernetes.
- **Shared map type.** `Arc<RwLock<IndexMap<String, SendableBackendRef>>>`. `IndexMap` preserves insertion order for stable random selection.
- **Three-phase event handling.** On each watcher event: (A) lock → revive/evict/collect new addrs → unlock; (B) build new backends via FFI with no lock held; (C) lock → insert new backends → unlock. Keeps `backend()` unblocked during backend creation.
- **TLS feature gate.** TLS builder methods (`builder.tls(...)`, `hosthdr`) only exist when compiled with the `varnishsys_90_sslflags` cfg key (Varnish ≥ 7.x with SSL). Guard any TLS code with `#[cfg(varnishsys_90_sslflags)]`.

## What NOT to do

- Do not add `eprintln!` or any unconditional stderr logging — output bleeds into Varnish's log unfiltered.
- Do not remove the `runtime` field from `VmodDirector` even though it appears unused — dropping it cancels the watcher task.
- Do not implement `Send` on `BackendRef` or `NativeBackend` directly (foreign types). Use the existing newtype wrappers or add a new one following the same pattern.
- Do not change `crate-type` away from `["cdylib"]` — Varnish loads the VMOD as a shared library.

## Dependencies

All in `Cargo.toml`. Notable:
- `varnish = "0.7.0"` with `features = ["ffi"]` — provides `NativeBackendBuilder::build_with_vcl` (added in 0.7.0) for creating backends outside a VCL context.
- `kube`, `k8s-openapi`, `tokio` — Kubernetes watch loop.
- `indexmap` — ordered map for deterministic random selection.
- `rand` — `random_range` for backend selection.
