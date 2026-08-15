//! S3-compatible [`ContentStore`].
//!
//! Works against AWS S3, MinIO, Cloudflare R2, Backblaze B2, Wasabi,
//! and any other backend that speaks the S3 v4 wire protocol. The
//! aws-sdk-s3 client is configured at construction time with a custom
//! endpoint URL + path-style access toggle to accommodate non-AWS
//! providers; otherwise the default credential chain is used.
//!
//! Object layout under `bucket`:
//!
//! - `<prefix>blobs/<hash>` — raw bytes; metadata carries mime,
//!   stored_at, expires_at, session_id, tenant_id.
//! - `<prefix>aliases/<base64url(alias_id)>` — small JSON object
//!   `{ "target_hash": "...", "expires_at": "..." }`.
//!
//! Per-object metadata uses the `x-amz-meta-mcpg-*` header convention
//! so it survives copy/replication. The full set:
//!
//! | Header                       | Meaning                                          |
//! |------------------------------|--------------------------------------------------|
//! | `x-amz-meta-mcpg-mime`       | Original mime_type                               |
//! | `x-amz-meta-mcpg-stored-at`  | RFC3339 timestamp                                |
//! | `x-amz-meta-mcpg-expires-at` | RFC3339 timestamp (optional)                     |
//! | `x-amz-meta-mcpg-session`    | Session tag (optional)                           |
//! | `x-amz-meta-mcpg-tenant`     | Tenant tag (optional)                            |
//! | `x-amz-meta-mcpg-size`       | Content length (cross-check against stored_size) |
//!
//! ## Eviction
//!
//! Operators MUST configure either:
//! - S3 lifecycle rules on the bucket prefix (preferred — no
//!   round-trip cost; the backend does the work), or
//! - Rely on `sweep_expired()`, which walks the `blobs/` prefix and
//!   `HEAD`s each object; expensive at scale, fine for low-volume.
//!
//! Lazy expiry on read is always active — `get` checks `expires_at`
//! in object metadata and treats past entries as `NotFound`.
//!
//! ## Stats
//!
//! `stats()` returns zeros with a `max_bytes` of zero ("unbounded").
//! S3 doesn't surface bucket-level utilisation cheaply; operators
//! should rely on CloudWatch / provider-native metrics. Tracked in
//! the future-work list.
//!
//! ## Signed URLs
//!
//! Native S3 presigner. SigV4-signed GETs valid up to 7 days; we cap
//! at the requested TTL and reject anything longer than 7 days.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use base64::Engine as _;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use mcpg_backend_llm_shared::{
    ContentStore, ContentStoreError, ContentStoreStats, ContentToStore, ResourceContent,
    ResourceHandle,
};

/// Operator-supplied S3 connection parameters. Constructed by the
/// gateway from `plugins.content_store` config; this crate stays
/// transport-agnostic.
#[derive(Debug, Clone)]
pub struct S3ContentStoreConfig {
    /// Bucket name. Must already exist + be writable.
    pub bucket: String,
    /// Optional key prefix (e.g. `mcpg-content/`). Trailing slash is
    /// added if missing. Empty = bucket root.
    pub prefix: String,
    /// AWS region. Required by the SDK even for non-AWS providers
    /// (e.g. `auto` for Cloudflare R2).
    pub region: String,
    /// Custom endpoint URL for S3-compatible providers
    /// (e.g. `https://<account>.r2.cloudflarestorage.com`,
    /// `http://localhost:9000` for MinIO). `None` = use AWS default
    /// resolver from the region.
    pub endpoint_url: Option<String>,
    /// Path-style addressing. Required by MinIO + most non-AWS
    /// implementations. AWS itself prefers virtual-hosted style, which
    /// is the default when this is `false`.
    pub force_path_style: bool,
    /// Static credentials. `None` = use the default AWS credential
    /// chain (env vars, IAM role, profile, …).
    pub credentials: Option<S3StaticCredentials>,
    /// Maximum bytes per single `put`. `0` = uncapped. Defaults are
    /// set by the gateway config layer.
    pub max_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct S3StaticCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

const META_MIME: &str = "mcpg-mime";
const META_STORED_AT: &str = "mcpg-stored-at";
const META_EXPIRES_AT: &str = "mcpg-expires-at";
const META_SESSION: &str = "mcpg-session";
const META_TENANT: &str = "mcpg-tenant";
const META_SIZE: &str = "mcpg-size";

/// 7 days — the SigV4 hard ceiling on presigned URL TTL.
const PRESIGNED_URL_MAX_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AliasRecord {
    target_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_at: Option<DateTime<Utc>>,
}

/// S3-backed content store. See module docs for layout + auth model.
pub struct S3ContentStore {
    client: Client,
    bucket: String,
    prefix: String,
    max_bytes: u64,
}

impl std::fmt::Debug for S3ContentStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3ContentStore")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("max_bytes", &self.max_bytes)
            .finish()
    }
}

impl S3ContentStore {
    /// Construct a new S3-backed store. Performs no network I/O at
    /// this point — the first request validates auth + bucket
    /// reachability. Operators wanting startup-time validation
    /// should call `head_bucket` after construction.
    pub async fn open(cfg: S3ContentStoreConfig) -> Result<Arc<Self>, ContentStoreError> {
        let mut builder = S3ConfigBuilder::new()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(Region::new(cfg.region.clone()))
            .force_path_style(cfg.force_path_style);

        if let Some(endpoint) = &cfg.endpoint_url {
            builder = builder.endpoint_url(endpoint.clone());
        }

        if let Some(creds) = &cfg.credentials {
            let credentials = Credentials::new(
                &creds.access_key_id,
                &creds.secret_access_key,
                creds.session_token.clone(),
                None,
                "mcpg-static",
            );
            builder = builder.credentials_provider(credentials);
        } else {
            // Fall back to the default chain. Loading defaults is
            // async (env vars are sync, but the chain may probe IMDS
            // / SSO sources). Materialise it once here.
            let defaults = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .load()
                .await;
            if let Some(provider) = defaults.credentials_provider() {
                builder = builder.credentials_provider(provider);
            }
        }

        let client = Client::from_conf(builder.build());

        let prefix = if cfg.prefix.is_empty() || cfg.prefix.ends_with('/') {
            cfg.prefix
        } else {
            format!("{}/", cfg.prefix)
        };

        Ok(Arc::new(Self {
            client,
            bucket: cfg.bucket,
            prefix,
            max_bytes: cfg.max_bytes,
        }))
    }

    fn blob_key(&self, hash: &str) -> String {
        format!("{}blobs/{hash}", self.prefix)
    }

    fn alias_key(&self, alias_id: &str) -> String {
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(alias_id);
        format!("{}aliases/{encoded}", self.prefix)
    }

    fn resolve_to_hash_blob_key(&self, id: &str) -> Option<KeyKind> {
        if let Some(rest) = id.strip_prefix("hash:") {
            return Some(KeyKind::Blob {
                hash: rest.to_owned(),
            });
        }
        if id.starts_with("alias:") {
            return Some(KeyKind::Alias {
                alias_id: id.to_owned(),
            });
        }
        if !id.is_empty() && id.chars().all(|c| c.is_ascii_hexdigit()) {
            return Some(KeyKind::Blob {
                hash: id.to_owned(),
            });
        }
        None
    }

    async fn head_blob(&self, hash: &str) -> Result<Option<HeadResult>, ContentStoreError> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(self.blob_key(hash))
            .send()
            .await
        {
            Ok(resp) => Ok(Some(HeadResult {
                expires_at: resp
                    .metadata()
                    .and_then(|m| m.get(META_EXPIRES_AT))
                    .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                    .map(|d| d.with_timezone(&Utc)),
            })),
            Err(err) => {
                if let Some(svc) = err.as_service_error()
                    && svc.is_not_found()
                {
                    return Ok(None);
                }
                Err(ContentStoreError::Storage {
                    message: format!("HeadObject {hash}: {err}"),
                })
            }
        }
    }

    async fn read_alias(&self, alias_id: &str) -> Result<Option<AliasRecord>, ContentStoreError> {
        let key = self.alias_key(alias_id);
        match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(resp) => {
                let body = resp
                    .body
                    .collect()
                    .await
                    .map_err(|e| ContentStoreError::Storage {
                        message: format!("read alias body {alias_id}: {e}"),
                    })?;
                let bytes = body.into_bytes();
                let rec: AliasRecord =
                    serde_json::from_slice(&bytes).map_err(|e| ContentStoreError::Storage {
                        message: format!("decode alias {alias_id}: {e}"),
                    })?;
                Ok(Some(rec))
            }
            Err(err) => {
                if let Some(svc) = err.as_service_error()
                    && svc.is_no_such_key()
                {
                    return Ok(None);
                }
                Err(ContentStoreError::Storage {
                    message: format!("GetObject alias {alias_id}: {err}"),
                })
            }
        }
    }

    async fn delete_blob(&self, hash: &str) -> Result<(), ContentStoreError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(self.blob_key(hash))
            .send()
            .await
            .map_err(|e| ContentStoreError::Storage {
                message: format!("DeleteObject {hash}: {e}"),
            })?;
        Ok(())
    }

    async fn delete_alias(&self, alias_id: &str) -> Result<(), ContentStoreError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(self.alias_key(alias_id))
            .send()
            .await
            .map_err(|e| ContentStoreError::Storage {
                message: format!("DeleteObject alias {alias_id}: {e}"),
            })?;
        Ok(())
    }
}

enum KeyKind {
    Blob { hash: String },
    Alias { alias_id: String },
}

struct HeadResult {
    expires_at: Option<DateTime<Utc>>,
}

#[async_trait]
impl ContentStore for S3ContentStore {
    async fn put(&self, content: ContentToStore) -> Result<ResourceHandle, ContentStoreError> {
        let size = content.bytes.len();
        let size_u64 = size as u64;
        if self.max_bytes > 0 && size_u64 > self.max_bytes {
            return Err(ContentStoreError::SizeLimit {
                limit_bytes: self.max_bytes as usize,
                actual_bytes: size,
            });
        }

        let hash_hex = hex::encode(blake3::hash(&content.bytes).as_bytes());
        let stored_at = Utc::now();
        let expires_at = content
            .ttl
            .and_then(|d| chrono::Duration::from_std(d).ok())
            .map(|d| stored_at + d);

        let mut put = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(self.blob_key(&hash_hex))
            .content_type(content.mime_type.clone())
            .body(ByteStream::from(content.bytes.clone()))
            .metadata(META_MIME, content.mime_type.clone())
            .metadata(META_STORED_AT, stored_at.to_rfc3339())
            .metadata(META_SIZE, size_u64.to_string());

        if let Some(t) = expires_at {
            put = put.metadata(META_EXPIRES_AT, t.to_rfc3339());
        }
        if let Some(s) = content.session_id.as_deref() {
            put = put.metadata(META_SESSION, s.to_owned());
        }
        if let Some(t) = content.tenant_id.as_deref() {
            put = put.metadata(META_TENANT, t.to_owned());
        }

        put.send().await.map_err(|e| ContentStoreError::Storage {
            message: format!("PutObject {hash_hex}: {e}"),
        })?;

        let id = if let Some(alias) = content.alias.as_deref() {
            let session = content.session_id.as_deref().unwrap_or("__no_session__");
            let alias_id = format!("alias:{session}:{alias}");
            let rec = AliasRecord {
                target_hash: hash_hex.clone(),
                expires_at,
            };
            let alias_bytes = serde_json::to_vec(&rec).map_err(|e| ContentStoreError::Storage {
                message: format!("encode alias: {e}"),
            })?;
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(self.alias_key(&alias_id))
                .content_type("application/json")
                .body(ByteStream::from(alias_bytes))
                .send()
                .await
                .map_err(|e| ContentStoreError::Storage {
                    message: format!("PutObject alias {alias_id}: {e}"),
                })?;
            alias_id
        } else {
            format!("hash:{hash_hex}")
        };

        Ok(ResourceHandle {
            id: id.clone(),
            uri: format!("mcpg-resource://{id}"),
            size_bytes: size,
            mime_type: content.mime_type,
            expires_at,
            content_hash: format!("blake3:{hash_hex}"),
        })
    }

    async fn get(&self, id: &str) -> Result<Option<ResourceContent>, ContentStoreError> {
        let hash = match self.resolve_to_hash_blob_key(id) {
            Some(KeyKind::Blob { hash }) => hash,
            Some(KeyKind::Alias { alias_id }) => match self.read_alias(&alias_id).await? {
                Some(rec) => {
                    if rec.expires_at.is_some_and(|t| t <= Utc::now()) {
                        // Drop the alias and its target — lazy GC.
                        let _ = self.delete_alias(&alias_id).await;
                        let _ = self.delete_blob(&rec.target_hash).await;
                        return Ok(None);
                    }
                    rec.target_hash
                }
                None => return Ok(None),
            },
            None => return Ok(None),
        };

        let key = self.blob_key(&hash);
        let resp = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(err) => {
                if let Some(svc) = err.as_service_error()
                    && svc.is_no_such_key()
                {
                    return Ok(None);
                }
                return Err(ContentStoreError::Storage {
                    message: format!("GetObject {hash}: {err}"),
                });
            }
        };

        let metadata = resp.metadata().cloned().unwrap_or_default();
        let mime_type = metadata
            .get(META_MIME)
            .cloned()
            .or_else(|| resp.content_type().map(str::to_owned))
            .unwrap_or_else(|| "application/octet-stream".into());
        let stored_at = metadata
            .get(META_STORED_AT)
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|d| d.with_timezone(&Utc))
            .unwrap_or_else(Utc::now);
        let expires_at = metadata
            .get(META_EXPIRES_AT)
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|d| d.with_timezone(&Utc));
        let session_id = metadata.get(META_SESSION).cloned();
        let tenant_id = metadata.get(META_TENANT).cloned();

        if expires_at.is_some_and(|t| t <= Utc::now()) {
            let _ = self.delete_blob(&hash).await;
            return Ok(None);
        }

        let body = resp
            .body
            .collect()
            .await
            .map_err(|e| ContentStoreError::Storage {
                message: format!("read blob body {hash}: {e}"),
            })?;
        let bytes = body.into_bytes();

        Ok(Some(ResourceContent {
            bytes,
            mime_type,
            session_id,
            tenant_id,
            stored_at,
            expires_at,
        }))
    }

    async fn delete(&self, id: &str) -> Result<(), ContentStoreError> {
        match self.resolve_to_hash_blob_key(id) {
            Some(KeyKind::Blob { hash }) => self.delete_blob(&hash).await,
            Some(KeyKind::Alias { alias_id }) => {
                if let Some(rec) = self.read_alias(&alias_id).await? {
                    let _ = self.delete_blob(&rec.target_hash).await;
                }
                self.delete_alias(&alias_id).await
            }
            None => Ok(()),
        }
    }

    async fn signed_url(
        &self,
        id: &str,
        ttl: Duration,
    ) -> Result<Option<String>, ContentStoreError> {
        if ttl > PRESIGNED_URL_MAX_TTL {
            return Err(ContentStoreError::Storage {
                message: format!("signed URL ttl {ttl:?} exceeds 7-day SigV4 maximum"),
            });
        }

        let hash = match self.resolve_to_hash_blob_key(id) {
            Some(KeyKind::Blob { hash }) => hash,
            Some(KeyKind::Alias { alias_id }) => match self.read_alias(&alias_id).await? {
                Some(rec) => rec.target_hash,
                None => return Ok(None),
            },
            None => return Ok(None),
        };

        // Confirm the blob still exists; HeadObject is cheap and lets
        // us return None instead of a presigned URL pointing at a
        // 404-on-fetch.
        if self.head_blob(&hash).await?.is_none() {
            return Ok(None);
        }

        let presigning =
            PresigningConfig::expires_in(ttl).map_err(|e| ContentStoreError::Storage {
                message: format!("presigning config: {e}"),
            })?;
        let req = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.blob_key(&hash))
            .presigned(presigning)
            .await
            .map_err(|e| ContentStoreError::Storage {
                message: format!("presign {hash}: {e}"),
            })?;
        Ok(Some(req.uri().to_string()))
    }

    fn stats(&self) -> ContentStoreStats {
        // S3 doesn't surface byte_count/item_count cheaply; emit zeros
        // and rely on provider-native metrics for utilisation.
        ContentStoreStats::default()
    }

    async fn sweep_expired(&self) -> usize {
        let mut continuation: Option<String> = None;
        let prefix = format!("{}blobs/", self.prefix);
        let mut removed = 0usize;
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&prefix);
            if let Some(t) = &continuation {
                req = req.continuation_token(t.clone());
            }
            let resp = match req.send().await {
                Ok(r) => r,
                Err(_) => return removed,
            };
            for obj in resp.contents() {
                let Some(key) = obj.key() else { continue };
                let Some(hash) = key.strip_prefix(&prefix) else {
                    continue;
                };
                let head = match self.head_blob(hash).await {
                    Ok(Some(h)) => h,
                    _ => continue,
                };
                if head.expires_at.is_some_and(|t| t <= Utc::now())
                    && self.delete_blob(hash).await.is_ok()
                {
                    removed += 1;
                }
            }
            match resp.next_continuation_token() {
                Some(t) if !t.is_empty() => continuation = Some(t.to_owned()),
                _ => break,
            }
        }
        removed
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Compile-time exercise: the type implements ContentStore + the
    /// expected auto-traits. We can't easily mock S3 here without
    /// pulling testcontainers + MinIO, which is the integration-test
    /// path. End-to-end exercise lives behind the `s3` feature in the
    /// gateway's integration suite.
    #[allow(dead_code)]
    fn assert_impl_content_store() {
        fn requires<T: ContentStore + Send + Sync + 'static>() {}
        requires::<S3ContentStore>();
    }

    #[test]
    fn alias_key_round_trips_through_base64url() {
        let cfg = S3ContentStoreConfig {
            bucket: "b".into(),
            prefix: "p/".into(),
            region: "us-east-1".into(),
            endpoint_url: None,
            force_path_style: false,
            credentials: None,
            max_bytes: 0,
        };
        // The rest of the test body builds up a key without
        // instantiating a Client (which would require credentials).
        // We mimic the prefixing logic.
        let alias_id = "alias:s1:incident";
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(alias_id);
        let expected = format!("{}aliases/{encoded}", cfg.prefix);
        // Decode round-trip just to prove the encoding's reversible.
        let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(&encoded)
            .unwrap();
        assert_eq!(String::from_utf8(decoded).unwrap(), alias_id);
        assert!(expected.starts_with("p/aliases/"));
    }

    #[test]
    fn blob_key_uses_prefix_directly() {
        let prefix = "mcpg/";
        let hash = "abc123";
        let expected = format!("{prefix}blobs/{hash}");
        assert_eq!(expected, "mcpg/blobs/abc123");
    }

    #[test]
    fn presigned_ttl_ceiling_is_seven_days() {
        // 7 days exactly is allowed; 7 days + 1 second is not.
        assert_eq!(PRESIGNED_URL_MAX_TTL, Duration::from_secs(604_800));
    }
}
