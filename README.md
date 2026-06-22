# vmod-k8s-endpoint

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
