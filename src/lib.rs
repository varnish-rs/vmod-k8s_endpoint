use futures::TryStreamExt;
use http::Uri;
use indexmap::IndexMap;
use k8s_openapi::api::core::v1::Endpoints;
use kube::runtime::watcher::Config;
use kube::{api::Api, Client};
use std::collections::HashSet;
use std::ffi::CString;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};
use tokio::time::{sleep_until, Instant};
use varnish::ffi;
use varnish::vcl::{log as vsl_log, BackendRef, Ctx, LogTag, NativeBackend, NativeBackendBuilder};

// Wraps `*mut ffi::vcl` to make it `Send`.
// Safety: the VCL pointer must remain valid for the entire lifetime of the Tokio runtime that owns this pointer.
struct RawVclPtr(*mut ffi::vcl);
unsafe impl Send for RawVclPtr {}

// Wraps `BackendRef` to make it `Send`.
// Safety: Varnish backends are designed for concurrent access by multiple request threads;
// the raw pointers inside BackendRef point to C structures protected by Varnish's own locking.
struct SendableBackendRef(BackendRef);
unsafe impl Send for SendableBackendRef {}

struct ServiceUri {
    use_tls: bool,
    service: String,
}

impl ServiceUri {
    fn parse(uri: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let parsed = Uri::from_str(uri)?;

        let path = parsed.path();
        if !path.is_empty() && path != "/" {
            return Err("URI must not have a path".into());
        }
        if parsed.query().is_some() {
            return Err("URI must not have a query".into());
        }

        let use_tls = parsed.scheme().map(|s| s.as_str()) == Some("https");
        let host = parsed.host().ok_or("URI must have a host")?;

        Ok(ServiceUri {
            use_tls,
            service: host.to_string(),
        })
    }
}

fn extract_endpoints(ep: &Endpoints, port_name: &str) -> HashSet<String> {
    let mut endpoints = HashSet::new();

    for subset in ep.subsets.iter().flatten() {
        for addr in subset.addresses.iter().flatten() {
            for port in subset.ports.iter().flatten() {
                if port.name.as_deref() == Some(port_name) {
                    endpoints.insert(format!("{}:{}", addr.ip, port.port));
                }
            }
        }
    }

    endpoints
}

struct OwnedBackend {
    // None = active; Some(instant) = evicted, pending GC after that deadline.
    expiry: Option<Instant>,
    backend: NativeBackend,
}

// Safety: NativeBackend wraps Varnish C structures that are safe to move between threads;
// concurrent access is managed by Varnish's internal locking, not Rust's type system.
unsafe impl Send for OwnedBackend {}

// Run the kube watcher and update shared endpoint/backend state.
async fn watch_endpoints(
    stream: impl futures::Stream<
            Item = Result<kube::runtime::watcher::Event<Endpoints>, kube::runtime::watcher::Error>,
        > + Send
        + 'static,
    port_name: String,
    use_tls: bool,
    backends: Arc<Mutex<IndexMap<String, SendableBackendRef>>>,
    raw_vcl: RawVclPtr,
) {
    const GC_GRACE: Duration = Duration::from_secs(1);
    // Suppress unused-variable warning on Varnish builds without SSL flags.
    #[cfg(not(varnishsys_90_sslflags))]
    let _ = use_tls;

    let mut stream = Box::pin(stream);

    // Owns all NativeBackend objects, tracking which are active vs. pending GC.
    let mut owned: IndexMap<String, OwnedBackend> = IndexMap::new();
    // Dummy initial deadline; the select! guard prevents this from firing until an eviction occurs.
    let gc_timer = sleep_until(Instant::now());
    tokio::pin!(gc_timer);

    loop {
        tokio::select! {
            result = stream.try_next() => {
                let result = match result {
                    Ok(r) => r,
                    Err(e) => {
                        vsl_log(LogTag::Error, format!("k8s-endpoint: watcher error: {e}"));
                        continue;
                    }
                };
                let Some(status) = result else { break };

                use kube::runtime::watcher::Event;
                let new_endpoints = match status {
                    Event::Apply(ep) | Event::InitApply(ep) => extract_endpoints(&ep, &port_name),
                    Event::Delete(_) => HashSet::new(),
                    Event::Init | Event::InitDone => continue,
                };

                // Phase A (under lock): revive grace-period backends, remove stale entries,
                // collect addrs that need new backends, mark evictions, reset GC timer.
                let to_create: Vec<(String, SocketAddr)> = {
                    let mut map = backends.lock().unwrap();

                    for addr_str in &new_endpoints {
                        if map.contains_key(addr_str) { continue; }
                        if let Some(ob) = owned.get_mut(addr_str) {
                            // Backend is in the GC grace period — revive it instead of allocating a new one.
                            ob.expiry = None;
                            map.insert(addr_str.clone(), SendableBackendRef(ob.backend.as_ref().clone()));
                        }
                    }

                    map.retain(|addr, _| new_endpoints.contains(addr));

                    let to_create = new_endpoints.iter()
                        .filter(|addr| !map.contains_key(*addr) && !owned.contains_key(*addr))
                        .filter_map(|addr| match addr.parse::<SocketAddr>() {
                            Ok(sa) => Some((addr.clone(), sa)),
                            Err(e) => {
                                vsl_log(LogTag::Error, format!("k8s-endpoint: bad endpoint address {addr}: {e}"));
                                None
                            }
                        })
                        .collect();

                    let expiry = Instant::now() + GC_GRACE;
                    for (addr, ob) in owned.iter_mut() {
                        if !new_endpoints.contains(addr) && ob.expiry.is_none() {
                            ob.expiry = Some(expiry);
                        }
                    }
                    if let Some(earliest) = owned.values().filter_map(|b| b.expiry).min() {
                        gc_timer.as_mut().reset(earliest);
                    }

                    to_create
                    // lock released here
                };

                // Phase B (no lock): create new backends via FFI without blocking VCL threads.
                let new_backends: Vec<(String, NativeBackend)> = to_create
                    .into_iter()
                    .filter_map(|(addr_str, sock_addr)| {
                        let name = CString::new(
                            format!("k8s_endpoint_{}", addr_str.replace(':', "_"))
                        ).unwrap();
                        let builder = NativeBackendBuilder::new_ip(&name, sock_addr);
                        #[cfg(varnishsys_90_sslflags)]
                        let builder = if use_tls { builder.tls(true, true) } else { builder };
                        match unsafe { builder.build_with_vcl(raw_vcl.0) } {
                            Ok(backend) => Some((addr_str, backend)),
                            Err(e) => {
                                vsl_log(LogTag::Error, format!("k8s-endpoint: failed to create backend for {addr_str}: {e}"));
                                None
                            }
                        }
                    })
                    .collect();

                // Phase C (under lock): insert newly built backends.
                if !new_backends.is_empty() {
                    let mut map = backends.lock().unwrap();
                    for (addr_str, backend) in new_backends {
                        map.insert(addr_str.clone(), SendableBackendRef(backend.as_ref().clone()));
                        owned.insert(addr_str, OwnedBackend { expiry: None, backend });
                    }
                }
            }

            // GC timer fired: drop backends whose grace period has elapsed.
            _ = &mut gc_timer, if owned.values().any(|b| b.expiry.is_some()) => {
                let now = Instant::now();
                owned.retain(|_, b| b.expiry.is_none_or(|e| e > now));
                if let Some(next) = owned.values().filter_map(|b| b.expiry).min() {
                    gc_timer.as_mut().reset(next);
                }
            }
        }
    }
}

pub struct VmodDirector {
    backends: Arc<Mutex<IndexMap<String, SendableBackendRef>>>,
    // Kept alive to prevent the watcher task from being cancelled.
    #[allow(unused)]
    runtime: Runtime,
}

#[varnish::vmod(docs = "API.md")]
mod k8s_endpoint {
    //! VMOD export wrapper used by varnish-rs to generate VMOD bindings.
    use super::*;

    impl VmodDirector {
        /// Construct a new director and start watching the given Kubernetes service.
        /// Creates a dedicated Tokio runtime and spawns a background watcher task on it.
        ///
        /// `namespace`: scope the watch to a specific namespace; omit to watch all namespaces.
        pub fn new(
            ctx: &mut Ctx,
            service_uri: &str,
            port_name: &str,
            namespace: Option<&str>,
        ) -> Result<Self, String> {
            let service = ServiceUri::parse(service_uri).map_err(|e| e.to_string())?;

            let backends = Arc::new(Mutex::new(IndexMap::new()));

            // Build a dedicated runtime to host background tasks for this director.
            let runtime = Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            let client = runtime
                .block_on(Client::try_default())
                .map_err(|e| e.to_string())?;

            let endpoints_api: Api<Endpoints> = match namespace {
                Some(ns) => Api::namespaced(client, ns),
                None => Api::all(client),
            };
            let watcher_config = Config {
                field_selector: Some(format!("metadata.name={}", service.service)),
                ..Default::default()
            };
            let stream = kube::runtime::watcher(endpoints_api, watcher_config);

            let vcl = super::RawVclPtr(ctx.raw.vcl.0);
            let port_name = port_name.to_string();
            let backends_for_task = backends.clone();
            runtime.spawn(async move {
                super::watch_endpoints(stream, port_name, service.use_tls, backends_for_task, vcl).await
            });

            Ok(VmodDirector { backends, runtime })
        }

        /// Return a randomly selected backend from the current pool, or `None` if empty.
        pub fn backend(&self) -> Option<BackendRef> {
            let map = self.backends.lock().unwrap();
            if map.is_empty() {
                return None;
            }
            let idx = rand::random_range(0..map.len());
            map.get_index(idx).map(|(_, v)| v.0.clone())
        }
    }
}
