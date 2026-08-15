# mcpg-plugin-storage-s3

> S3-compatible content store for MCPG gateways: durable, shared across replicas, with native presigned download URLs.

MCPG gateways hand large tool results and generated media to a **content store**
rather than inlining them in an MCP response, then serve them back through
`mcpg-resource://` URIs. This crate implements that store on top of the S3 API,
so blobs survive restarts and are visible to every gateway replica behind a load
balancer. It targets AWS S3 by default and works against any service that speaks
the same wire protocol — MinIO, Cloudflare R2, Backblaze B2, Wasabi, Ceph — by
setting an endpoint URL and path-style addressing. Unlike the in-process and
filesystem stores, it can mint presigned URLs, letting clients download large
blobs directly instead of streaming them back through `resources/read`.

## What's here
- `S3StoragePlugin` — the `ContentStorePlugin` factory registered under
  `kind = "s3"`. `build_profile` validates the operator's `config:` object,
  translates it into an `S3ContentStoreConfig`, and opens the client.
- `S3ContentStore` — the `ContentStore` implementation: `put`, `get`, `delete`,
  `signed_url`, `stats`, and `sweep_expired`.
- `S3ContentStoreConfig` and `S3StaticCredentials` — the runtime configuration
  types, if you construct the store directly instead of going through the
  factory.
- The AWS SDK is pinned to the modern rustls / aws-lc-rs HTTPS client rather
  than the legacy hyper 0.14 stack, so the dependency tree carries no
  end-of-life TLS crates.

This crate is not a cdylib plugin. It has no `plugin.yaml`, exports no
`mcpg_plugin_register` symbol, and is never signed, packed, or pulled from a
registry — it is compiled into the gateway behind a Cargo feature.

## Used by
- The MCPG gateway, which registers this factory under `kind = "s3"` when built
  with `--features s3-content-store`. The feature is off by default so gateways
  that do not need object storage do not pay the `aws-sdk-s3` binary-size cost.
- Anything embedding the MCPG content-store traits that wants a durable,
  multi-writer blob store behind them.

## Usage

### As a Rust dependency

```toml
[dependencies]
mcpg-plugin-storage-s3 = "<version>"
mcpg-backend-llm-shared = "<version>"
serde_json = "1"
```

```rust
use std::sync::Arc;

use mcpg_backend_llm_shared::{ContentStore, ContentStoreError, ContentStorePlugin};
use mcpg_plugin_storage_s3::S3StoragePlugin;

async fn open_store() -> Result<Arc<dyn ContentStore>, ContentStoreError> {
    let plugin: Arc<dyn ContentStorePlugin> = Arc::new(S3StoragePlugin::new());
    let store = plugin
        .build_profile(
            "media",
            &serde_json::json!({
                "bucket": "mcpg-media",
                "region": "us-east-1",
                "prefix": "content/",
            }),
        )
        .await?;
    Ok(store)
}
```

### Operator configuration

The store is selected from the gateway's dedicated top-level `storage:` block,
by `kind: s3`. There is no `plugins:` entry to add. Bindings route to a provider
through their own `content_storage:` field, and `storage.default` names the
provider used by bindings that do not.

```yaml
storage:
  default: media
  providers:
    - id: media
      kind: s3
      config:
        bucket: mcpg-media
        region: us-east-1
        prefix: content/
        endpoint_url: http://minio.internal:9000
        force_path_style: true
        access_key_id: ${env.S3_ACCESS_KEY_ID}
        secret_access_key: ${env.S3_SECRET_ACCESS_KEY}
        max_bytes: 268435456          # 256 MiB per object
```

| Field | Type | Default | Description |
|---|---|---|---|
| `bucket` | string | *(required)* | Target bucket. Must already exist and be writable. |
| `region` | string | *(required)* | Region name. The SDK requires one even for non-AWS services; Cloudflare R2 uses `auto`. |
| `prefix` | string | `""` | Key prefix for every object. Empty writes at the bucket root. |
| `endpoint_url` | string | *(unset)* | Endpoint of an S3-compatible service. Omit for AWS. |
| `force_path_style` | bool | `false` | Path-style addressing, required by MinIO and most non-AWS services. AWS itself prefers the virtual-hosted default. |
| `access_key_id` | string | *(default credential chain)* | Static access key. Must be set together with `secret_access_key`. |
| `secret_access_key` | string | *(default credential chain)* | Static secret key. |
| `session_token` | string | *(unset)* | STS session token, for temporary credentials. |
| `max_bytes` | integer | `268435456` (256 MiB) | Maximum size of a single stored object; `0` removes the cap. |

Unknown fields are rejected, and an invalid spec aborts gateway boot rather than
starting with an unusable store.

## Security
Leave `access_key_id` and `secret_access_key` unset to use the default AWS
credential chain — environment variables, an EC2 or EKS instance role, an SSO
session, a credential process — which is the right choice on AWS because no
long-lived secret ends up in gateway config. When you do supply static
credentials, write them as `${env.NAME}` or `cred://…` references so the gateway
substitutes the value at config load. Supplying one half of the pair without the
other is rejected at profile build rather than silently falling back to the
ambient chain, so a half-configured provider cannot quietly pick up whatever
credentials the host happens to carry.

Presigned URLs are SigV4 GETs. A requested lifetime longer than the seven-day
SigV4 ceiling is rejected rather than clamped, so a caller cannot believe it
received a longer-lived link than it did.

## Object layout
Objects are content-addressed by BLAKE3 hash, which deduplicates identical
content automatically:

- `<prefix>blobs/<hash>` — the bytes.
- `<prefix>aliases/<base64url(alias_id)>` — a small JSON record redirecting a
  named alias to its target hash.

Per-object metadata travels in `x-amz-meta-mcpg-*` headers — `mime`,
`stored-at`, `expires-at`, `session`, `tenant`, and `size` — so it survives
copy and cross-region replication.

Expiry is lazy on read: `get` compares `expires-at` against the current time and
reports an expired object as not found. For reclaiming space, configure S3
lifecycle rules on the prefix, which cost nothing at request time. The
`sweep_expired` fallback walks the `blobs/` prefix and issues a `HEAD` per
object, which is fine at low volume and expensive at scale. `stats()` returns
zeros because S3 exposes no cheap bucket-level utilisation figure; use your
provider's own metrics for capacity monitoring.

## Build / test
```bash
cargo build -p mcpg-plugin-storage-s3
cargo test  -p mcpg-plugin-storage-s3
```

To build a gateway that registers this store:

```bash
cargo build -p mcpg --features s3-content-store --release
```

## See also
- Full gateway config schema, including the `storage:` block: <https://mcpg.dev/docs/reference/configuration>
- Plugin classes and the plugin ABI: <https://mcpg.dev/docs/plugins/plugins-and-protocol>
- `mcpg-plugin-storage-builtin` — the always-present in-memory and filesystem
  stores.
