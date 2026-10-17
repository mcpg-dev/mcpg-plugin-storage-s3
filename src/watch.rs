//! `watch_strategy` entity (`s3_objects`): the objects under a bucket
//! prefix, each one created, modified or deleted as the shared
//! `object.changed` event.
//!
//! Two modes. `list` (the default) lists the prefix every `interval_ms`
//! through the SDK's inventory helper, which diffs it against the listing
//! the gateway has stored and resumes exactly after a restart; an SQS queue
//! of the bucket's event notifications, when given, makes it list at once
//! rather than at the next interval. `notifications` reads only that queue,
//! for a bucket too large to list: each notification record is one event,
//! and its message is deleted once the gateway has stored every event of it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use aws_credential_types::Credentials;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::error::{DisplayErrorContext, ProvideErrorMetadata};
use aws_sdk_sqs::types::QueueAttributeName;
use mcpg_plugin_protocol::backend::{WatchError, WatchEvent};
use mcpg_plugin_protocol::{PluginManifest, firstparty_manifest};
use mcpg_plugin_sdk::HostHandle;
use mcpg_plugin_sdk::ffi::{SyncWatchStrategyPlugin, WatchHandleBox};
use mcpg_plugin_sdk::inventory::{
    self, InventoryOptions, InventoryStart, OBJECT_CHANGED_EVENT_TYPE, ObjectEntry, Pattern,
    acknowledge_inventory_watch, cancel_inventory_watch, matches_key,
    spawn_inventory_watch_with_waker,
};
use mcpg_plugin_sdk::queue::{
    Delivery, QueueOptions, QueueSource, acknowledge_queue_watch, cancel_queue_watch,
    spawn_queue_watch,
};
use serde::Deserialize;
use serde_json::{Value, json};

pub const PLUGIN_ID: &str = "dev.mcpg.storage.s3";

/// The strategy discriminator this entity handles.
pub const WATCH_KIND: &str = "s3_objects";

/// The `store` of every event.
const STORE: &str = "s3";

/// How much longer than `ack_timeout_ms` a notification stays invisible.
const VISIBILITY_MARGIN_SECS: u64 = 30;

/// Longest visibility timeout SQS allows: 12 hours.
const MAX_VISIBILITY_SECS: u64 = 43_200;

/// Most messages one `ReceiveMessage` returns.
const MAX_BATCH: i32 = 10;

fn default_true() -> bool {
    true
}

fn default_interval_ms() -> u64 {
    60_000
}

fn default_timeout_ms() -> u64 {
    30_000
}

fn default_wait_time_secs() -> u64 {
    10
}

/// How the watch learns of changes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    /// List the prefix; notifications, when given, only wake the listing.
    #[default]
    List,
    /// Read only the notification queue.
    Notifications,
}

/// The SQS queue the bucket sends its event notifications to.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NotificationsSpec {
    queue_url: String,
    /// Default: the bucket's region.
    #[serde(default)]
    region: Option<String>,
    /// Default: none, even when the bucket has one.
    #[serde(default)]
    endpoint_url: Option<String>,
    /// Long-poll wait of one receive, seconds (1 to 20).
    #[serde(default = "default_wait_time_secs")]
    wait_time_secs: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WatchSpec {
    bucket: String,
    /// The folder watched; a `/` is added when missing. Empty = the bucket.
    #[serde(default)]
    prefix: String,
    /// AWS region; `auto` for Cloudflare R2.
    region: String,
    /// Custom endpoint for an S3-compatible provider. Omit for AWS.
    #[serde(default)]
    endpoint_url: Option<String>,
    /// Path-style addressing (MinIO and most non-AWS providers).
    #[serde(default)]
    force_path_style: bool,
    /// Static credentials; without them the AWS default chain applies.
    #[serde(default)]
    access_key_id: Option<String>,
    #[serde(default)]
    secret_access_key: Option<String>,
    #[serde(default)]
    session_token: Option<String>,
    #[serde(default)]
    mode: Mode,
    /// Objects in "subfolders" of the prefix too.
    #[serde(default = "default_true")]
    recursive: bool,
    /// Keep only the objects whose key relative to the prefix matches.
    #[serde(default)]
    pattern: Option<String>,
    #[serde(default)]
    notifications: Option<NotificationsSpec>,
    /// Listing cadence (`list` mode).
    #[serde(default = "default_interval_ms")]
    interval_ms: u64,
    /// Budget of one listing.
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
    #[serde(default = "inventory::defaults::max_files")]
    max_files: usize,
    #[serde(default)]
    start_at: InventoryStart,
    #[serde(default = "inventory::defaults::snapshot_bytes")]
    snapshot_bytes: usize,
    #[serde(default = "inventory::defaults::max_in_flight")]
    max_in_flight: usize,
    #[serde(default = "inventory::defaults::ack_timeout_ms")]
    ack_timeout_ms: u64,
}

/// An SDK error with its code and message, without the request.
fn sdk_error<E, R>(service: &str, action: &str, error: aws_sdk_s3::error::SdkError<E, R>) -> String
where
    E: ProvideErrorMetadata + std::error::Error + 'static,
    R: std::fmt::Debug,
{
    match (error.code(), error.message()) {
        (Some(code), Some(message)) => format!("{service} {action} failed: {code}: {message}"),
        (Some(code), None) => format!("{service} {action} failed: {code}"),
        _ => format!("{service} {action} failed: {}", DisplayErrorContext(&error)),
    }
}

/// The objects of one listing, keyed relative to `prefix`; the first
/// `max_files` in key order when there are more.
async fn list_objects(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    recursive: bool,
    pattern: Option<&Pattern>,
    max_files: usize,
) -> Result<Vec<ObjectEntry>, String> {
    let mut entries = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut request = client.list_objects_v2().bucket(bucket).prefix(prefix);
        if !recursive {
            request = request.delimiter("/");
        }
        if let Some(token) = token.take() {
            request = request.continuation_token(token);
        }
        let page = request
            .send()
            .await
            .map_err(|e| sdk_error("S3", "ListObjectsV2", e))?;
        for object in page.contents() {
            let Some(key) = object.key() else { continue };
            let relative = key.strip_prefix(prefix).unwrap_or(key);
            // A zero-byte "folder" marker is not an object of the folder.
            if relative.is_empty() || relative.ends_with('/') {
                continue;
            }
            if pattern.is_some_and(|p| !matches_key(p, relative)) {
                continue;
            }
            // S3 lists in key order, so the first `max_files` are final.
            if entries.len() == max_files {
                tracing::warn!(
                    bucket,
                    prefix,
                    max_files,
                    "s3_objects: more objects than max_files; the listing is cut"
                );
                return Ok(entries);
            }
            entries.push(ObjectEntry {
                key: relative.to_owned(),
                size: object.size().and_then(|s| u64::try_from(s).ok()),
                modified_at_ms: object.last_modified().and_then(|t| t.to_millis().ok()),
                etag: object.e_tag().map(|e| e.trim_matches('"').to_owned()),
                version: None,
            });
        }
        match page.next_continuation_token() {
            Some(next) if page.is_truncated() == Some(true) => token = Some(next.to_owned()),
            _ => return Ok(entries),
        }
    }
}

/// What an S3 event notification record says happened.
#[derive(Debug, Clone, PartialEq)]
struct Record {
    bucket: String,
    key: String,
    created: bool,
    size: Option<u64>,
    etag: Option<String>,
    version: Option<String>,
    sequencer: Option<String>,
    event_time: Option<String>,
    event_name: String,
}

/// An object key as a notification carries it: URL-encoded, with `+` for
/// a space.
fn decode_key(encoded: &str) -> String {
    let bytes = encoded.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
                match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    Some(byte) => {
                        out.push(byte);
                        i += 2;
                    }
                    None => out.push(b'%'),
                }
            }
            byte => out.push(byte),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The object records of one queue message: an S3 notification, the same
/// wrapped by SNS, or an EventBridge event. The S3 test event and other
/// kinds of record have none.
fn parse_notification(body: &str) -> Vec<Record> {
    let Ok(value) = serde_json::from_str::<Value>(body) else {
        return Vec::new();
    };
    // SNS delivers the S3 notification as a JSON string in `Message`.
    if value.get("Type").and_then(Value::as_str) == Some("Notification")
        && let Some(inner) = value.get("Message").and_then(Value::as_str)
    {
        return parse_notification(inner);
    }
    if let Some(records) = value.get("Records").and_then(Value::as_array) {
        return records.iter().filter_map(s3_record).collect();
    }
    eventbridge_record(&value).into_iter().collect()
}

fn s3_record(record: &Value) -> Option<Record> {
    let event_name = record.get("eventName")?.as_str()?.to_owned();
    let created = if event_name.starts_with("ObjectCreated:") {
        true
    } else if event_name.starts_with("ObjectRemoved:")
        || event_name.starts_with("LifecycleExpiration:")
    {
        false
    } else {
        return None;
    };
    let s3 = record.get("s3")?;
    let object = s3.get("object")?;
    let text = |v: &Value, k: &str| v.get(k).and_then(Value::as_str).map(str::to_owned);
    Some(Record {
        bucket: s3.get("bucket")?.get("name")?.as_str()?.to_owned(),
        key: decode_key(object.get("key")?.as_str()?),
        created,
        size: object.get("size").and_then(Value::as_u64),
        etag: text(object, "eTag"),
        version: text(object, "versionId"),
        sequencer: text(object, "sequencer"),
        event_time: text(record, "eventTime"),
        event_name,
    })
}

fn eventbridge_record(event: &Value) -> Option<Record> {
    if event.get("source").and_then(Value::as_str) != Some("aws.s3") {
        return None;
    }
    let kind = event.get("detail-type")?.as_str()?;
    let created = match kind {
        "Object Created" => true,
        "Object Deleted" => false,
        _ => return None,
    };
    let detail = event.get("detail")?;
    let object = detail.get("object")?;
    let text = |v: &Value, k: &str| v.get(k).and_then(Value::as_str).map(str::to_owned);
    Some(Record {
        bucket: detail.get("bucket")?.get("name")?.as_str()?.to_owned(),
        key: object.get("key")?.as_str()?.to_owned(),
        created,
        size: object.get("size").and_then(Value::as_u64),
        etag: text(object, "etag"),
        version: text(object, "version-id"),
        sequencer: text(object, "sequencer"),
        event_time: text(event, "time"),
        event_name: format!("{kind}:{}", text(detail, "reason").unwrap_or_default()),
    })
}

/// Which records a watch reports: its bucket, under its prefix, matching
/// its pattern; and the key each is reported under.
struct Scope {
    bucket: String,
    prefix: String,
    recursive: bool,
    pattern: Option<Pattern>,
    location: String,
}

impl Scope {
    fn relative<'a>(&self, record: &'a Record) -> Option<&'a str> {
        if record.bucket != self.bucket {
            return None;
        }
        let relative = record.key.strip_prefix(&self.prefix)?;
        if relative.is_empty() || relative.ends_with('/') {
            return None;
        }
        if !self.recursive && relative.contains('/') {
            return None;
        }
        if self
            .pattern
            .as_ref()
            .is_some_and(|p| !matches_key(p, relative))
        {
            return None;
        }
        Some(relative)
    }

    /// The `object.changed` event of one record, its id kept across
    /// redeliveries of the notification.
    fn event(&self, record: &Record, key: &str) -> WatchEvent {
        let op = if record.created { "created" } else { "deleted" };
        let mut hasher = blake3::Hasher::new();
        let mark = record
            .sequencer
            .as_deref()
            .or(record.etag.as_deref())
            .or(record.event_time.as_deref())
            .unwrap_or("");
        for part in [STORE, self.location.as_str(), key, op, mark] {
            hasher.update(&(part.len() as u64).to_be_bytes());
            hasher.update(part.as_bytes());
        }
        let mut data = json!({
            "op": op,
            "store": STORE,
            "location": self.location,
            "key": key,
            "eventName": record.event_name,
        });
        if let Some(size) = record.size.filter(|_| record.created) {
            data["size"] = json!(size);
        }
        if let Some(etag) = &record.etag {
            data["etag"] = json!(etag.trim_matches('"'));
        }
        if let Some(version) = &record.version {
            data["version"] = json!(version);
        }
        if record.created
            && let Some(at) = &record.event_time
        {
            data["modifiedAt"] = json!(at);
        }
        WatchEvent {
            event_id: Some(format!("obj-{}", &hasher.finalize().to_hex()[..32])),
            event_type: Some(OBJECT_CHANGED_EVENT_TYPE.to_owned()),
            occurred_at: record.event_time.clone(),
            data: Some(data),
            ..WatchEvent::default()
        }
    }
}

/// The bucket's notification queue.
struct NotificationQueue {
    client: aws_sdk_sqs::Client,
    queue_url: String,
    wait_time_secs: i32,
    visibility_secs: i32,
}

impl NotificationQueue {
    async fn check(&self) -> Result<(), String> {
        self.client
            .get_queue_attributes()
            .queue_url(&self.queue_url)
            .attribute_names(QueueAttributeName::QueueArn)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| sdk_error("SQS", "GetQueueAttributes", e))
    }

    async fn receive(&self, max: i32) -> Result<Vec<aws_sdk_sqs::types::Message>, String> {
        self.client
            .receive_message()
            .queue_url(&self.queue_url)
            .max_number_of_messages(max.clamp(1, MAX_BATCH))
            .wait_time_seconds(self.wait_time_secs)
            .visibility_timeout(self.visibility_secs)
            .send()
            .await
            .map(|out| out.messages.unwrap_or_default())
            .map_err(|e| sdk_error("SQS", "ReceiveMessage", e))
    }

    async fn delete(&self, receipt: &str) -> Result<(), String> {
        self.client
            .delete_message()
            .queue_url(&self.queue_url)
            .receipt_handle(receipt)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| sdk_error("SQS", "DeleteMessage", e))
    }

    async fn release(&self, receipt: &str) -> Result<(), String> {
        self.client
            .change_message_visibility()
            .queue_url(&self.queue_url)
            .receipt_handle(receipt)
            .visibility_timeout(0)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| sdk_error("SQS", "ChangeMessageVisibility", e))
    }
}

/// `notifications` mode: the queue as a queue source, each relevant record
/// one delivery. A message is settled once every record of it is.
struct NotificationSource {
    queue: NotificationQueue,
    scope: Scope,
    rt: tokio::runtime::Runtime,
    /// Records of each message not yet acknowledged, by receipt handle.
    open: Mutex<HashMap<String, usize>>,
}

impl NotificationSource {
    fn handle(receipt: &Value) -> Option<&str> {
        receipt.get("h").and_then(Value::as_str)
    }
}

impl QueueSource for NotificationSource {
    fn connect(&self) -> Result<(), String> {
        self.rt.block_on(self.queue.check())
    }

    fn receive(&self, max: usize, _wait: Duration) -> Result<Vec<Delivery>, String> {
        let messages = self
            .rt
            .block_on(self.queue.receive(i32::try_from(max).unwrap_or(MAX_BATCH)))?;
        let mut deliveries = Vec::new();
        for message in messages {
            let Some(handle) = message.receipt_handle() else {
                continue;
            };
            let records = parse_notification(message.body().unwrap_or_default());
            let events: Vec<WatchEvent> = records
                .iter()
                .filter_map(|record| {
                    self.scope
                        .relative(record)
                        .map(|key| self.scope.event(record, key))
                })
                .collect();
            if events.is_empty() {
                // A test event, another bucket, or a key out of scope.
                if let Err(error) = self.rt.block_on(self.queue.delete(handle)) {
                    tracing::warn!(error = %error, "s3_objects: cannot delete a notification of nothing watched");
                }
                continue;
            }
            self.open
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(handle.to_owned(), events.len());
            deliveries.extend(events.into_iter().map(|event| Delivery {
                event,
                receipt: json!({ "h": handle }),
            }));
        }
        Ok(deliveries)
    }

    fn ack(&self, receipt: &Value) -> Result<(), String> {
        let Some(handle) = Self::handle(receipt) else {
            return Ok(());
        };
        let done = {
            let mut open = self.open.lock().unwrap_or_else(|e| e.into_inner());
            match open.get_mut(handle) {
                Some(left) if *left > 1 => {
                    *left -= 1;
                    false
                }
                Some(_) => {
                    open.remove(handle);
                    true
                }
                // Released already: the message comes again.
                None => false,
            }
        };
        if done {
            self.rt.block_on(self.queue.delete(handle))
        } else {
            Ok(())
        }
    }

    fn release(&self, receipt: &Value) -> Result<(), String> {
        let Some(handle) = Self::handle(receipt) else {
            return Ok(());
        };
        let held = self
            .open
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(handle)
            .is_some();
        if held {
            self.rt.block_on(self.queue.release(handle))
        } else {
            Ok(())
        }
    }
}

/// A `list` mode watch, with the thread that turns notifications into
/// early listings when it has one.
struct ListWatch {
    inventory: WatchHandleBox,
    waker: Option<(Arc<AtomicBool>, JoinHandle<()>)>,
}

/// Which strategy runs behind a handle.
enum WatchState {
    List(ListWatch),
    Notifications(WatchHandleBox),
}

/// `watch_strategy` entity. Stateless beyond its manifest — each watcher's
/// bucket and credentials arrive on the per-watch spec.
pub struct S3WatchCdylib {
    manifest: PluginManifest,
}

impl S3WatchCdylib {
    /// Infallible cdylib factory; the watch carries no plugin-level config.
    pub fn from_host_config(_config_json: &str, _host: HostHandle) -> Self {
        Self {
            manifest: firstparty_manifest! {
                id: "dev.mcpg.storage.s3",
                name: "S3 Objects Watch Strategy",
                class: WatchStrategy,
            },
        }
    }
}

fn check_secret(name: &str, value: Option<&String>) -> Result<(), String> {
    if value.is_some_and(|v| v.starts_with("cred://")) {
        return Err(format!(
            "{name} must not be a cred:// URI — per-caller credentials are unsupported; use \
             ${{env.X}} / ${{secret.X}} / vault:// (resolved before the watch starts)"
        ));
    }
    Ok(())
}

impl SyncWatchStrategyPlugin for S3WatchCdylib {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn kind(&self) -> &str {
        WATCH_KIND
    }

    fn watch(
        &self,
        resource_uri: &str,
        spec: &Value,
        emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
    ) -> Result<WatchHandleBox, WatchError> {
        let (spec, resume) = mcpg_plugin_protocol::take_watch_resume(spec);
        let parsed: WatchSpec =
            serde_json::from_value(spec).map_err(|e| WatchError::InvalidSpec {
                message: format!("invalid s3_objects watch spec: {e}"),
            })?;
        let invalid = |m: String| WatchError::InvalidSpec { message: m };
        if parsed.bucket.trim().is_empty() {
            return Err(invalid("bucket must not be empty".into()));
        }
        if parsed.region.trim().is_empty() {
            return Err(invalid("region must not be empty".into()));
        }
        check_secret("access_key_id", parsed.access_key_id.as_ref()).map_err(invalid)?;
        check_secret("secret_access_key", parsed.secret_access_key.as_ref()).map_err(invalid)?;
        check_secret("session_token", parsed.session_token.as_ref()).map_err(invalid)?;
        if parsed.access_key_id.is_some() != parsed.secret_access_key.is_some() {
            return Err(invalid(
                "access_key_id and secret_access_key go together".into(),
            ));
        }
        if !(1..=100_000).contains(&parsed.max_files) {
            return Err(invalid("max_files must be 1 to 100000".into()));
        }
        if parsed.timeout_ms == 0 {
            return Err(invalid("timeout_ms must be greater than 0".into()));
        }
        if parsed.mode == Mode::Notifications && parsed.notifications.is_none() {
            return Err(invalid(
                "mode notifications needs notifications.queue_url".into(),
            ));
        }
        if let Some(notifications) = &parsed.notifications {
            if !notifications.queue_url.starts_with("https://")
                && !notifications.queue_url.starts_with("http://")
            {
                return Err(invalid(
                    "notifications.queue_url must be an http(s) URL".into(),
                ));
            }
            if !(1..=20).contains(&notifications.wait_time_secs) {
                return Err(invalid(
                    "notifications.wait_time_secs must be 1 to 20".into(),
                ));
            }
        }
        let visibility_secs = parsed.ack_timeout_ms.div_ceil(1_000) + VISIBILITY_MARGIN_SECS;
        if visibility_secs > MAX_VISIBILITY_SECS {
            return Err(invalid(format!(
                "ack_timeout_ms must be at most {}",
                (MAX_VISIBILITY_SECS - VISIBILITY_MARGIN_SECS) * 1_000
            )));
        }
        let pattern = parsed
            .pattern
            .as_deref()
            .map(Pattern::new)
            .transpose()
            .map_err(|e| invalid(format!("pattern: {e}")))?;
        let prefix = if parsed.prefix.is_empty() || parsed.prefix.ends_with('/') {
            parsed.prefix.clone()
        } else {
            format!("{}/", parsed.prefix)
        };
        let location = format!("s3://{}/{prefix}", parsed.bucket);

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("mcpg-s3-watch")
            .enable_all()
            .build()
            .map_err(|e| WatchError::Subscribe {
                message: format!("s3_objects: tokio runtime init failed: {e}"),
            })?;
        let static_credentials = match (&parsed.access_key_id, &parsed.secret_access_key) {
            (Some(id), Some(secret)) => Some(Credentials::new(
                id,
                secret,
                parsed.session_token.clone(),
                None,
                "mcpg-s3-watch-static",
            )),
            _ => None,
        };
        let default_chain = || {
            rt.block_on(
                aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .region(Region::new(parsed.region.clone()))
                    .load(),
            )
            .credentials_provider()
        };
        let chain = static_credentials.is_none().then(default_chain).flatten();
        let sqs = parsed.notifications.as_ref().map(|notifications| {
            let mut config = aws_sdk_sqs::config::Builder::new()
                .behavior_version(aws_sdk_sqs::config::BehaviorVersion::latest())
                .region(aws_sdk_sqs::config::Region::new(
                    notifications
                        .region
                        .clone()
                        .unwrap_or_else(|| parsed.region.clone()),
                ));
            if let Some(endpoint) = &notifications.endpoint_url {
                config = config.endpoint_url(endpoint.clone());
            }
            if let Some(credentials) = &static_credentials {
                config = config.credentials_provider(credentials.clone());
            } else if let Some(chain) = &chain {
                config = config.credentials_provider(chain.clone());
            }
            NotificationQueue {
                client: aws_sdk_sqs::Client::from_conf(config.build()),
                queue_url: notifications.queue_url.clone(),
                wait_time_secs: i32::try_from(notifications.wait_time_secs).unwrap_or(10),
                visibility_secs: i32::try_from(visibility_secs).unwrap_or(i32::MAX),
            }
        });

        if parsed.mode == Mode::Notifications {
            let queue = sqs.ok_or_else(|| invalid("notifications.queue_url is missing".into()))?;
            let options = QueueOptions {
                source: format!("s3 notifications {}", queue.queue_url),
                max_in_flight: parsed.max_in_flight,
                ack_timeout: Duration::from_millis(parsed.ack_timeout_ms),
                receive_wait: Duration::from_secs(
                    u64::try_from(queue.wait_time_secs).unwrap_or(10),
                ),
            };
            options.validate().map_err(invalid)?;
            let source = NotificationSource {
                queue,
                scope: Scope {
                    bucket: parsed.bucket,
                    prefix,
                    recursive: parsed.recursive,
                    pattern,
                    location,
                },
                rt,
                open: Mutex::new(HashMap::new()),
            };
            let handle = spawn_queue_watch(resource_uri, options, emit_event, source)?;
            return Ok(boxed(WatchState::Notifications(handle)));
        }

        let mut config = aws_sdk_s3::config::Builder::new()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(Region::new(parsed.region.clone()))
            .force_path_style(parsed.force_path_style);
        if let Some(endpoint) = &parsed.endpoint_url {
            config = config.endpoint_url(endpoint.clone());
        }
        if let Some(credentials) = static_credentials {
            config = config.credentials_provider(credentials);
        } else if let Some(chain) = chain {
            config = config.credentials_provider(chain);
        }
        let client = aws_sdk_s3::Client::from_conf(config.build());
        let rt = Arc::new(rt);
        let listing_rt = Arc::clone(&rt);
        let timeout = Duration::from_millis(parsed.timeout_ms);
        let (bucket, recursive, max_files) =
            (parsed.bucket.clone(), parsed.recursive, parsed.max_files);
        let listing_prefix = prefix.clone();
        // A bucket that cannot be listed now is listed again at the next
        // interval: the source is not lost to a transient outage.
        let list = move || {
            listing_rt.block_on(async {
                tokio::time::timeout(
                    timeout,
                    list_objects(
                        &client,
                        &bucket,
                        &listing_prefix,
                        recursive,
                        pattern.as_ref(),
                        max_files,
                    ),
                )
                .await
                .map_err(|_| "S3 listing timed out".to_owned())?
            })
        };
        let (inventory, waker) = spawn_inventory_watch_with_waker(
            resource_uri,
            InventoryOptions {
                store: STORE.into(),
                location,
                interval: Duration::from_millis(parsed.interval_ms),
                start: parsed.start_at,
                snapshot_bytes: parsed.snapshot_bytes,
                max_in_flight: parsed.max_in_flight,
                ack_timeout: Duration::from_millis(parsed.ack_timeout_ms),
            },
            resume,
            emit_event,
            list,
        )?;
        let waker = sqs.map(|queue| {
            let stop = Arc::new(AtomicBool::new(false));
            let stopping = Arc::clone(&stop);
            let join = std::thread::Builder::new()
                .name("mcpg-s3-notify".to_owned())
                .spawn(move || {
                    let mut backoff = Duration::from_millis(500);
                    while !stopping.load(Ordering::Acquire) {
                        match rt.block_on(queue.receive(MAX_BATCH)) {
                            Ok(messages) => {
                                backoff = Duration::from_millis(500);
                                if messages.is_empty() {
                                    continue;
                                }
                                // A hint only: the listing is what reports.
                                waker.wake();
                                for message in &messages {
                                    if let Some(handle) = message.receipt_handle()
                                        && let Err(error) = rt.block_on(queue.delete(handle))
                                    {
                                        tracing::warn!(error = %error, "s3_objects: cannot delete a notification");
                                    }
                                }
                            }
                            Err(error) => {
                                tracing::warn!(error = %error, "s3_objects: notification receive failed; the listing goes on at its interval");
                                let until = std::time::Instant::now() + backoff;
                                while std::time::Instant::now() < until
                                    && !stopping.load(Ordering::Acquire)
                                {
                                    std::thread::sleep(Duration::from_millis(50));
                                }
                                backoff = (backoff * 2).min(Duration::from_secs(30));
                            }
                        }
                    }
                })
                .map_err(|e| WatchError::Subscribe {
                    message: format!("s3_objects: cannot start the notification thread: {e}"),
                });
            join.map(|join| (stop, join))
        });
        let waker = match waker.transpose() {
            Ok(waker) => waker,
            Err(error) => {
                cancel_inventory_watch(inventory);
                return Err(error);
            }
        };
        Ok(boxed(WatchState::List(ListWatch { inventory, waker })))
    }

    fn acknowledge(&self, watch_handle: &WatchHandleBox, position: &Value) {
        if watch_handle.0.is_null() {
            return;
        }
        // SAFETY: the pointer `watch` returned, still owned by the host,
        // which never acknowledges after cancel.
        #[allow(unsafe_code)]
        let state = unsafe { &*(watch_handle.0 as *const WatchState) };
        match state {
            WatchState::List(list) => acknowledge_inventory_watch(&list.inventory, position),
            WatchState::Notifications(handle) => acknowledge_queue_watch(handle, position),
        }
    }

    fn cancel(&self, watch_handle: WatchHandleBox) {
        if watch_handle.0.is_null() {
            return;
        }
        // SAFETY: produced by `Box::into_raw` in `boxed` and round-tripped by
        // the host exactly once.
        #[allow(unsafe_code)]
        let state = unsafe { Box::from_raw(watch_handle.0 as *mut WatchState) };
        match *state {
            WatchState::List(list) => {
                if let Some((stop, join)) = list.waker {
                    stop.store(true, Ordering::Release);
                    let _ = join.join();
                }
                cancel_inventory_watch(list.inventory);
            }
            WatchState::Notifications(handle) => cancel_queue_watch(handle),
        }
    }
}

fn boxed(state: WatchState) -> WatchHandleBox {
    WatchHandleBox(Box::into_raw(Box::new(state)) as *mut ())
}

#[cfg(test)]
#[path = "watch_tests.rs"]
mod tests;
