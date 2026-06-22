use futures::{Stream, TryStreamExt};
use http::Uri;
use indexmap::IndexMap;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::runtime::watcher::{Config, Event};
use kube::{api::Api, Client};
use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::net::SocketAddr;
use std::sync::mpsc;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};
use tokio::time::{sleep_until, Instant};
use varnish::ffi;
use varnish::vcl::{log as vsl_log, BackendRef, Ctx, LogTag, NativeBackend, NativeBackendBuilder};

#[varnish::vmod(docs = "API.md")]
mod k8s_endpoint {
    //! VMOD export wrapper used by varnish-rs to generate VMOD bindings.
    use super::*;

    impl VmodDirector {
        /// Construct a new director and start watching the given Kubernetes service.
        /// Creates a dedicated Tokio runtime and spawns a background watcher task on it.
        ///
        /// `namespace`: Kubernetes namespace to watch. Defaults to the namespace from the active
        /// kubeconfig context or service-account (when running in-cluster).
        ///
        /// `wait_for_initial`: Block until the first endpoint list has been fetched from the
        /// Kubernetes API and all backends are registered. Defaults to `true`. Set to `false`
        /// to return immediately and discover backends in the background.
        pub fn new(
            ctx: &mut Ctx,
            service_uri: &str,
            port_name: &str,
            namespace: Option<&str>,
            wait_for_initial: Option<bool>,
        ) -> Result<Self, String> {
            let service = ServiceUri::parse(service_uri).map_err(|e| e.to_string())?;

            #[cfg(not(varnishsys_90_sslflags))]
            if service.use_tls {
                return Err("TLS requested but Varnish was built without SSL support".to_string());
            }

            let backends = Arc::new(RwLock::new(IndexMap::new()));

            // Build a dedicated runtime to host background tasks for this director.
            let runtime = Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            let client = runtime
                .block_on(Client::try_default())
                .map_err(|e| e.to_string())?;
            let effective_ns = namespace
                .map(str::to_string)
                .unwrap_or_else(|| client.default_namespace().to_string());

            let endpoints_api: Api<EndpointSlice> = Api::namespaced(client, &effective_ns);
            let watcher_config = Config {
                label_selector: Some(format!("kubernetes.io/service-name={}", service.service)),
                ..Default::default()
            };
            let stream = kube::runtime::watcher(endpoints_api, watcher_config);

            let vcl = super::RawVclPtr(ctx.raw.vcl.0);
            let port_name = port_name.to_string();
            let wait = wait_for_initial.unwrap_or(true);
            let (ready_tx, ready_rx) = if wait {
                let (tx, rx) = mpsc::sync_channel::<()>(1);
                (Some(tx), Some(rx))
            } else {
                (None, None)
            };
            runtime.spawn(super::watch_endpoints(
                stream,
                port_name,
                service.service,
                service.use_tls,
                backends.clone(),
                vcl,
                ready_tx,
            ));
            if let Some(rx) = ready_rx {
                rx.recv_timeout(Duration::from_secs(30))
                    .map_err(|e| match e {
                        mpsc::RecvTimeoutError::Timeout => {
                            "timed out waiting for initial endpoint discovery (30s)".to_string()
                        }
                        mpsc::RecvTimeoutError::Disconnected => {
                            "watcher exited before completing initial discovery".to_string()
                        }
                    })?;
            }

            Ok(VmodDirector { backends, runtime })
        }

        /// Return a randomly selected backend from the current pool, or `None` if empty.
        pub fn backend(&self) -> Option<BackendRef> {
            let map = self.backends.read().expect("backends lock poisoned");
            if map.is_empty() {
                return None;
            }
            let idx = rand::random_range(0..map.len());
            map.get_index(idx).map(|(_, v)| v.0.clone())
        }

        /// Return a pretty-printed JSON object listing all currently active backend addresses.
        pub fn dump(&self) -> String {
            let map = self.backends.read().expect("backends lock poisoned");
            if map.is_empty() {
                return "{\n  \"backends\": []\n}".to_string();
            }
            let entries: Vec<String> = map.keys().map(|addr| format!("    \"{}\"", addr)).collect();
            format!("{{\n  \"backends\": [\n{}\n  ]\n}}", entries.join(",\n"))
        }
    }
}

// Wraps `*mut ffi::vcl` to make it `Send`.
// Safety: the VCL pointer must remain valid for the entire lifetime of the Tokio runtime that owns this pointer.
struct RawVclPtr(*mut ffi::vcl);
unsafe impl Send for RawVclPtr {}

// Wraps `BackendRef` to make it `Send + Sync`.
// Safety: Varnish backends are designed for concurrent access by multiple request threads;
// the raw pointers inside BackendRef point to C structures protected by Varnish's own locking.
struct SendableBackendRef(BackendRef);
unsafe impl Send for SendableBackendRef {}
unsafe impl Sync for SendableBackendRef {}

struct ServiceUri {
    use_tls: bool,
    service: String,
}

impl ServiceUri {
    fn parse(uri: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let parsed: Uri = uri.parse()?;

        if parsed.scheme_str().is_none() && parsed.host().is_none() {
            let svc = parsed.path();
            if svc.is_empty() {
                return Err("service name must not be empty".into());
            }
            if svc.contains('/') || parsed.query().is_some() {
                return Err("invalid service name".into());
            }
            return Ok(ServiceUri {
                use_tls: false,
                service: svc.to_string(),
            });
        }

        let path = parsed.path();
        if !path.is_empty() && path != "/" {
            return Err("URI must not have a path".into());
        }
        if parsed.query().is_some() {
            return Err("URI must not have a query".into());
        }

        let use_tls = match parsed.scheme_str() {
            None | Some("http") => false,
            Some("https") => true,
            Some(s) => return Err(format!("unsupported scheme '{s}': use http or https").into()),
        };
        let host = parsed.host().ok_or("URI must have a host")?;

        Ok(ServiceUri {
            use_tls,
            service: host.to_string(),
        })
    }
}

fn extract_endpoints(ep: &EndpointSlice, port_name: &str) -> HashSet<String> {
    let Some(port) = ep
        .ports
        .iter()
        .flatten()
        .find(|p| p.name.as_deref() == Some(port_name))
        .and_then(|p| p.port)
    else {
        return HashSet::new();
    };

    ep.endpoints
        .iter()
        .filter(|e| e.conditions.as_ref().and_then(|c| c.ready).unwrap_or(true))
        .flat_map(|e| e.addresses.iter())
        .map(|ip| {
            if ip.contains(':') {
                format!("[{ip}]:{port}")
            } else {
                format!("{ip}:{port}")
            }
        })
        .collect()
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
    stream: impl Stream<Item = Result<Event<EndpointSlice>, kube::runtime::watcher::Error>>
        + Send
        + 'static,
    port_name: String,
    service_name: String,
    use_tls: bool,
    backends: Arc<RwLock<IndexMap<String, SendableBackendRef>>>,
    raw_vcl: RawVclPtr,
    mut ready_tx: Option<mpsc::SyncSender<()>>,
) {
    const GC_GRACE: Duration = Duration::from_secs(65);
    // Suppress unused-variable warnings on Varnish builds without SSL flags.
    #[cfg(not(varnishsys_90_sslflags))]
    let _ = (use_tls, service_name);

    tokio::pin!(stream);

    #[cfg(varnishsys_90_sslflags)]
    let sni_cstr = CString::new(service_name.as_str()).expect("service name contains null byte");

    // Per-slice endpoint tracking; unioned on each event to produce the desired backend set.
    let mut slices: HashMap<String, HashSet<String>> = HashMap::new();
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
                        vsl_log(LogTag::Error, format!("k8s_endpoint: watcher error: {e}"));
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
                let Some(status) = result else {
                    vsl_log(LogTag::Error, "k8s_endpoint: watcher stream ended; no further endpoint updates");
                    break;
                };
                let new_endpoints: HashSet<String> = match status {
                    Event::Apply(slice) | Event::InitApply(slice) => {
                        let Some(name) = slice.metadata.name.clone() else {
                            vsl_log(LogTag::Error, "k8s_endpoint: EndpointSlice has no metadata.name, skipping");
                            continue;
                        };
                        slices.insert(name, extract_endpoints(&slice, &port_name));
                        slices.values().flatten().cloned().collect()
                    }
                    Event::Delete(slice) => {
                        let Some(name) = slice.metadata.name.clone() else {
                            continue;
                        };
                        slices.remove(&name);
                        slices.values().flatten().cloned().collect()
                    }
                    Event::Init => { slices.clear(); continue; }
                    Event::InitDone => {
                        if let Some(tx) = ready_tx.take() {
                            let _ = tx.try_send(());
                        }
                        continue;
                    }
                };

                // Phase A (under lock): revive grace-period backends, remove stale entries,
                // collect addrs that need new backends, mark evictions, reset GC timer.
                let to_create: Vec<(String, SocketAddr)> = {
                    let mut map = backends.write().expect("backends lock poisoned");

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
                                vsl_log(LogTag::Error, format!("k8s_endpoint: bad endpoint address {addr}: {e}"));
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
                        let name = CString::new(format!("k8s_endpoint.ep({addr_str})"))
                            .expect("endpoint addr contains null byte");
                        let builder = NativeBackendBuilder::new_ip(&name, sock_addr);
                        #[cfg(varnishsys_90_sslflags)]
                        let builder = if use_tls {
                            let (verify_host, verify_peer) = (true, true);
                            builder.tls(verify_host, verify_peer).hosthdr(&sni_cstr)
                        } else {
                            builder
                        };
                        match unsafe { builder.build_with_vcl(raw_vcl.0) } {
                            Ok(backend) => Some((addr_str, backend)),
                            Err(e) => {
                                vsl_log(LogTag::Error, format!("k8s_endpoint: failed to create backend for {addr_str}: {e}"));
                                None
                            }
                        }
                    })
                    .collect();

                // Phase C (under lock): insert newly built backends.
                if !new_backends.is_empty() {
                    let mut map = backends.write().expect("backends lock poisoned");
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
    backends: Arc<RwLock<IndexMap<String, SendableBackendRef>>>,
    // Kept alive to prevent the watcher task from being cancelled.
    #[allow(unused)]
    runtime: Runtime,
}

#[cfg(test)]
mod tests {
    use k8s_openapi::api::discovery::v1::{
        Endpoint, EndpointConditions, EndpointPort, EndpointSlice,
    };

    varnish::run_vtc_tests!("tests/*.vtc");

    fn make_endpoint_slice(ips: &[&str], ports: &[(&str, i32)]) -> EndpointSlice {
        EndpointSlice {
            endpoints: ips
                .iter()
                .map(|ip| Endpoint {
                    addresses: vec![ip.to_string()],
                    conditions: Some(EndpointConditions {
                        ready: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .collect(),
            ports: Some(
                ports
                    .iter()
                    .map(|(name, port)| EndpointPort {
                        name: Some(name.to_string()),
                        port: Some(*port),
                        ..Default::default()
                    })
                    .collect(),
            ),
            ..Default::default()
        }
    }

    #[test]
    fn service_uri_plain_http() {
        let u = super::ServiceUri::parse("http://my-service").expect("valid URI");
        assert_eq!(u.service, "my-service");
        assert!(!u.use_tls);
    }

    #[test]
    fn service_uri_https() {
        let u = super::ServiceUri::parse("https://my-service").expect("valid URI");
        assert_eq!(u.service, "my-service");
        assert!(u.use_tls);
    }

    #[test]
    fn service_uri_trailing_slash() {
        let u = super::ServiceUri::parse("http://my-service/").expect("valid URI");
        assert_eq!(u.service, "my-service");
    }

    #[test]
    fn service_uri_no_scheme() {
        // bare hostname: no scheme, no host — http crate puts input in path segment
        let u = super::ServiceUri::parse("my-service").expect("valid URI");
        assert_eq!(u.service, "my-service");
        assert!(!u.use_tls);
    }

    #[test]
    fn service_uri_bare_with_hyphen() {
        let u = super::ServiceUri::parse("my-service-foo").expect("valid URI");
        assert_eq!(u.service, "my-service-foo");
        assert!(!u.use_tls);
    }

    #[test]
    fn service_uri_unknown_scheme_is_err() {
        assert!(super::ServiceUri::parse("ftp://my-service").is_err());
        assert!(super::ServiceUri::parse("grpc://my-service").is_err());
    }

    #[test]
    fn service_uri_with_path_is_err() {
        assert!(super::ServiceUri::parse("http://my-service/path").is_err());
        assert!(super::ServiceUri::parse("my-service/path").is_err());
    }

    #[test]
    fn service_uri_with_query_is_err() {
        assert!(super::ServiceUri::parse("http://my-service?q=1").is_err());
    }

    #[test]
    fn service_uri_empty_is_err() {
        assert!(super::ServiceUri::parse("").is_err());
    }

    #[test]
    fn service_uri_no_host_is_err() {
        assert!(super::ServiceUri::parse("http://").is_err());
    }

    #[test]
    fn extract_endpoints_no_ports() {
        let ep = EndpointSlice {
            ..Default::default()
        };
        assert!(super::extract_endpoints(&ep, "http").is_empty());
    }

    #[test]
    fn extract_endpoints_port_absent() {
        let ep = make_endpoint_slice(&["10.0.0.1"], &[("metrics", 9090)]);
        assert!(super::extract_endpoints(&ep, "http").is_empty());
    }

    #[test]
    fn extract_endpoints_multiple_ips() {
        let ep = make_endpoint_slice(&["10.0.0.1", "10.0.0.2"], &[("http", 8080)]);
        let result = super::extract_endpoints(&ep, "http");
        assert_eq!(result.len(), 2);
        assert!(result.contains("10.0.0.1:8080"));
        assert!(result.contains("10.0.0.2:8080"));
    }

    #[test]
    fn extract_endpoints_ipv6() {
        let ep = make_endpoint_slice(&["::1", "fe80::1"], &[("http", 8080)]);
        let result = super::extract_endpoints(&ep, "http");
        assert_eq!(result.len(), 2);
        assert!(result.contains("[::1]:8080"));
        assert!(result.contains("[fe80::1]:8080"));
    }

    #[test]
    fn extract_endpoints_mixed_ports() {
        let ep = make_endpoint_slice(&["10.0.0.1"], &[("http", 8080), ("grpc", 9000)]);
        let result = super::extract_endpoints(&ep, "http");
        assert_eq!(result.len(), 1);
        assert!(result.contains("10.0.0.1:8080"));
    }

    #[test]
    fn extract_endpoints_unready_excluded() {
        let mut ep = make_endpoint_slice(&["10.0.0.1"], &[("http", 8080)]);
        ep.endpoints[0].conditions = Some(EndpointConditions {
            ready: Some(false),
            ..Default::default()
        });
        assert!(super::extract_endpoints(&ep, "http").is_empty());
    }

    #[test]
    fn extract_endpoints_ready_none_included() {
        let mut ep = make_endpoint_slice(&["10.0.0.1"], &[("http", 8080)]);
        ep.endpoints[0].conditions = None;
        let result = super::extract_endpoints(&ep, "http");
        assert!(result.contains("10.0.0.1:8080"));
    }
}
