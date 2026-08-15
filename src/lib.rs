//! `mcpg-plugin-storage-s3` — S3-compatible content-store plugin.
//!
//! Implements [`mcpg_backend_llm_shared::ContentStorePlugin`] with
//! `kind = "s3"`. Operators wire one or more named providers via
//! the gateway's top-level `storage:` config block:
//!
//! ```yaml
//! storage:
//!   providers:
//!     - id: media
//!       kind: s3
//!       config:
//!         bucket: mcpg-media
//!         region: us-east-1
//!         endpoint_url: http://minio.local:9000   # optional
//!         force_path_style: true                  # MinIO/R2/B2
//!         access_key_id: ${env.S3_KEY}
//!         secret_access_key: ${env.S3_SECRET}
//!         max_bytes: 268435456
//! ```
//!
//! The plugin is gated behind the gateway's `s3-content-store`
//! feature flag (path-dep) so non-S3 deployments don't pay the
//! aws-sdk-s3 binary size cost. See module `store` for the actual
//! `ContentStore` implementation; this module is the factory shell.

use std::sync::Arc;

use async_trait::async_trait;
use mcpg_backend_llm_shared::{ContentStore, ContentStoreError, ContentStorePlugin};
use mcpg_plugin_protocol::{PluginClass, PluginManifest};
use serde::{Deserialize, Serialize};

pub mod store;

pub use store::{S3ContentStore, S3ContentStoreConfig, S3StaticCredentials};

const PLUGIN_VERSION: &str = env!("CARGO_PKG_VERSION");
const PROTOCOL_VERSION: &str = "1.0";

fn s3_manifest() -> PluginManifest {
    PluginManifest {
        id: "dev.mcpg.storage.s3".into(),
        version: PLUGIN_VERSION.into(),
        name: "S3-Compatible Content Store".into(),
        plugin_class: PluginClass::ContentStore,
        protocol_version: PROTOCOL_VERSION.into(),
        license: None,
        required_capabilities: Vec::new(),
        tags: vec![
            "persistent".into(),
            "cross_replica".into(),
            "signed_urls".into(),
        ],
        provides: Vec::new(),
        provides_schemes: Vec::new(),
        module_path_prefix: ::std::module_path!()
            .split("::")
            .next()
            .unwrap_or("")
            .to_owned(),
        backend_profile: None,
    }
}

/// Operator-facing config shape. This is what the gateway sees in
/// the top-level `storage.providers[].config:` field; we translate
/// it into the runtime [`S3ContentStoreConfig`] inside
/// [`build_profile`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct S3Spec {
    /// Bucket name. Must already exist + be writable.
    bucket: String,
    /// Optional key prefix (e.g. `mcpg-content/`). Empty = bucket root.
    #[serde(default)]
    prefix: String,
    /// AWS region. Required by the SDK even for non-AWS providers
    /// (e.g. `auto` for Cloudflare R2).
    region: String,
    /// Custom endpoint URL for S3-compatible providers. Omit for AWS.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    endpoint_url: Option<String>,
    /// Path-style addressing. Required by MinIO + most non-AWS
    /// implementations. Default `false` (virtual-hosted style).
    #[serde(default)]
    force_path_style: bool,
    /// Static AWS access key. When unset, the default credential chain
    /// is used.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    access_key_id: Option<String>,
    /// Static AWS secret key. Required when `access_key_id` is set.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    secret_access_key: Option<String>,
    /// Optional STS session token (e.g. for temporary credentials).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    session_token: Option<String>,
    /// Maximum bytes per single `put`. `0` = uncapped. Default 256 MiB.
    #[serde(default = "S3Spec::default_max_bytes")]
    max_bytes: u64,
}

impl S3Spec {
    fn default_max_bytes() -> u64 {
        256 * 1024 * 1024
    }
}

/// Factory for `s3` content-store instances. Stateless — every call
/// to [`build_profile`] opens its own client; the returned
/// `Arc<dyn ContentStore>` is what the gateway holds long-term.
#[derive(Debug)]
pub struct S3StoragePlugin {
    manifest: PluginManifest,
}

impl Default for S3StoragePlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl S3StoragePlugin {
    pub fn new() -> Self {
        Self {
            manifest: s3_manifest(),
        }
    }
}

#[async_trait]
impl ContentStorePlugin for S3StoragePlugin {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn kind(&self) -> &str {
        "s3"
    }

    async fn build_profile(
        &self,
        _profile_name: &str,
        spec: &serde_json::Value,
    ) -> Result<Arc<dyn ContentStore>, ContentStoreError> {
        let parsed: S3Spec =
            serde_json::from_value(spec.clone()).map_err(|e| ContentStoreError::Storage {
                message: format!("invalid s3 spec: {e}"),
            })?;

        let credentials = match (
            parsed.access_key_id.as_deref(),
            parsed.secret_access_key.as_deref(),
        ) {
            (Some(ak), Some(sk)) => Some(S3StaticCredentials {
                access_key_id: ak.to_owned(),
                secret_access_key: sk.to_owned(),
                session_token: parsed.session_token.clone(),
            }),
            (None, None) => None,
            _ => {
                return Err(ContentStoreError::Storage {
                    message: "s3 spec: access_key_id and secret_access_key must be set together"
                        .into(),
                });
            }
        };

        let runtime_cfg = S3ContentStoreConfig {
            bucket: parsed.bucket,
            prefix: parsed.prefix,
            region: parsed.region,
            endpoint_url: parsed.endpoint_url,
            force_path_style: parsed.force_path_style,
            credentials,
            max_bytes: parsed.max_bytes,
        };

        let store = S3ContentStore::open(runtime_cfg).await?;
        Ok(store)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_is_content_store() {
        let m = s3_manifest();
        assert!(matches!(m.plugin_class, PluginClass::ContentStore));
        assert_eq!(m.id, "dev.mcpg.storage.s3");
        assert!(m.tags.contains(&"persistent".to_owned()));
        assert!(m.tags.contains(&"signed_urls".to_owned()));
    }

    #[test]
    fn kind_is_s3() {
        assert_eq!(S3StoragePlugin::new().kind(), "s3");
    }

    #[tokio::test]
    async fn build_profile_rejects_partial_credentials() {
        let plugin = S3StoragePlugin::new();
        let err = plugin
            .build_profile(
                "broken",
                &serde_json::json!({
                    "bucket": "x",
                    "region": "us-east-1",
                    "access_key_id": "AKIA..."   // missing secret
                }),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, ContentStoreError::Storage { .. }),
            "expected Storage error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn build_profile_rejects_unknown_field() {
        let plugin = S3StoragePlugin::new();
        let err = plugin
            .build_profile(
                "x",
                &serde_json::json!({
                    "bucket": "b",
                    "region": "r",
                    "bogus": true
                }),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ContentStoreError::Storage { .. }));
    }
}
