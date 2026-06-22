# vmod-k8s_endpoint

Varnish VMOD that watches a Kubernetes service's endpoints and exposes them as a randomly-selected director. Backends are added and removed automatically as pods come and go.

## Building

```bash
cargo build --release
```

## Usage

In VCL:

```vcl
import k8s_endpoint;

sub vcl_init {
    new director = k8s_endpoint.new(
        service_uri = "http://my-service",   // or https:// to enable TLS to backends
        port_name   = "http",                // must match a named port in the EndpointSlice
        namespace   = "production"           // omit to use namespace from kubeconfig/service account
    );
}

sub vcl_backend_fetch {
    set bereq.backend = director.backend();
}
```

### `new(service_uri, port_name [, namespace])`

- `service_uri`: Kubernetes service name, optionally prefixed with `http://` or `https://`. Only the hostname is used for service lookup; `https://` enables TLS to backends.
- `port_name`: Named port on the Kubernetes `EndpointSlice` to watch.
- `namespace` *(optional)*: Kubernetes namespace to scope the watch. Omit to use the namespace from the active kubeconfig context or in-cluster service account.

### `.backend()`

Returns a random backend from the current pool, or `0` (none) if the pool is empty.

### `.dump()`

Returns a JSON object listing the currently active backend endpoints:

```json
{
  "backends": [
    "10.0.0.1:8080",
    "10.0.0.2:8080",
    "10.0.0.3:8080"
  ]
}
```

Useful for synthetic diagnostic responses:

```vcl
sub vcl_recv {
    if (req.url == "/backends") {
        return(synth(200, ""));
    }
}

sub vcl_synth {
    set resp.http.Content-Type = "application/json";
    set resp.body = director.dump();
    return(deliver);
}
```

## RBAC

When running in-cluster, the Varnish pod's ServiceAccount needs read access to `EndpointSlices`:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
rules:
- apiGroups: ["discovery.k8s.io"]
  resources: ["endpointslices"]
  verbs: ["get", "list", "watch"]
```

## Testing with minikube

The `k8s/` directory contains manifests and a deploy script for a local test cluster:

```bash
k8s/deploy.sh
```

This starts minikube (if needed), deploys 3 backend pods and a Varnish pod, compiles and loads the VMOD, then prints the active backend list.
