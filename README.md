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

- `watch::S3WatchCdylib` — the `s3_objects` watch strategy, shipped as the
  loadable `dev.mcpg.storage.s3` plugin: each object created, modified or
  deleted under a bucket prefix as an MCP Event. See
  [Watching a bucket](#watching-a-bucket-s3_objects).

The content store is compiled into the gateway behind a Cargo feature; the
watch strategy is the cdylib (`libmcpg_plugin_storage_s3.so`) the gateway
loads like any other plugin, from the `plugins/storage-s3` OCI artifact.

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

## Watching a bucket (`s3_objects`)

The `s3_objects` watch strategy reports each object created, modified or
deleted under a bucket prefix as the shared `object.changed` event, the
same shape a local folder or an SFTP directory gives:
`{op, store: "s3", location: "s3://bucket/prefix/", key, size, modifiedAt,
etag}`. Keys are relative to the prefix.

```yaml
mcp:
  capabilities:
    events:
      - name: partner.drops
        description: A CSV arrived in, changed in, or left the partner drop prefix.
        source:
          watch:
            strategy:
              type: plugin
              kind: s3_objects
              bucket: partner-drops
              prefix: incoming
              region: eu-west-1
              pattern: "**/*.csv"
              notifications:
                queue_url: https://sqs.eu-west-1.amazonaws.com/123456789012/partner-drops-events
            event_type: object.changed
```

It runs in one of two modes:

- **`list`** (default). The watch lists the prefix every `interval_ms` and
  reports what changed since the listing the gateway stored: an overwrite is
  a modification (its ETag or time moved), a deletion is seen as one. A
  restart resumes exactly, deletions included, while the listing fits
  `snapshot_bytes` (about 2 000 objects); past that it records a gap and takes
  the listing it finds as its baseline. With `notifications`, each message on
  the queue makes the watch list at once, and the message is deleted; the
  listing stays what reports, so a lost notification costs only latency.
- **`notifications`**. The watch reads only the queue, for a bucket too large
  to list: each `ObjectCreated:*` record is `created`, each `ObjectRemoved:*`
  or `LifecycleExpiration:*` record is `deleted`. S3 does not tell an
  overwrite from a creation, so an overwrite is `created` here. A message is
  deleted once the gateway has stored every event in it, and made visible
  again when it has not done so in `ack_timeout_ms`; the event id comes from
  the record's sequencer, so a redelivered record is stored once. The queue
  may carry S3's own notifications, the same wrapped by SNS, or EventBridge
  events; S3's test event and records outside the bucket, prefix or pattern
  are deleted without an event.

| Field | Type | Default | Notes |
|---|---|---|---|
| `bucket`, `region` | string (required) | — | `region: auto` for Cloudflare R2. |
| `prefix` | string | `""` | The folder watched; a `/` is added when missing. |
| `endpoint_url`, `force_path_style` | | — / `false` | For an S3-compatible provider, as for the content store. |
| `access_key_id`, `secret_access_key`, `session_token` | string | default chain | Static credentials, resolved before the watch starts; `cred://` is rejected. Without them the AWS default chain applies. |
| `mode` | `list` \| `notifications` | `list` | How the watch learns of changes. |
| `recursive` | bool | `true` | Objects in "subfolders" of the prefix too. |
| `pattern` | string | — | Keep only the keys, relative to the prefix, that match this glob: `*` stays in a folder, `**` crosses them. |
| `notifications.queue_url` | string | — | The SQS queue the bucket sends `s3:ObjectCreated:*` and `s3:ObjectRemoved:*` to. Required in `notifications` mode. |
| `notifications.region`, `notifications.endpoint_url` | string | the bucket's region / none | Where the queue is. |
| `notifications.wait_time_secs` | int | `10` | Long-poll wait of one receive (1 to 20). |
| `interval_ms` | int | `60000` | Listing cadence, at least 250. |
| `timeout_ms` | int | `30000` | Budget of one listing. |
| `max_files` | int | `10000` | Most objects listed (1 to 100000): the first in key order. |
| `start_at` | `now` \| `existing` | `now` | `existing` reports what is there at a first start as created. |
| `snapshot_bytes`, `max_in_flight`, `ack_timeout_ms` | int | `131072`, `256`, `60000` | As for every inventory and queue source. |

The watch needs `s3:ListBucket` on the bucket, and with a queue
`sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility` and
`sqs:GetQueueAttributes` on it. The bucket's notification configuration and
the queue policy that lets S3 send to the queue are set up on AWS, not here;
[Running against AWS](#running-against-aws) has a script for both.

## Build / test
```bash
cargo build  -p mcpg-plugin-storage-s3
cargo test   -p mcpg-plugin-storage-s3
cargo test   -p mcpg-plugin-storage-s3 --features integration-tests  # LocalStack S3 + SQS (docker), live AWS if set
cargo clippy -p mcpg-plugin-storage-s3 --all-targets -- -D warnings
```

The integration suite runs `s3_objects` against LocalStack with real bucket
notifications: a listing through creates, a modification, a deletion and an
exact restart; the notification queue alone through a create, a delete and
a redelivery; and a listing a notification wakes.

`tests/default_chain.rs` runs offline in the plain test run. It gives the
watch no static credentials, so the AWS default chain applies, and a
loopback mock serves the container credentials
(`AWS_CONTAINER_CREDENTIALS_FULL_URI`), a path-style S3 listing and the
notification queue. The test checks that the S3 and the SQS calls are
SigV4-signed with those credentials and their session token, each for its
own service and region.

`--features integration-tests` also builds `tests/s3_live.rs`, which runs
against real AWS when `S3_LIVE_BUCKET`, `S3_LIVE_REGION` and
`S3_LIVE_QUEUE_URL` are set in your shell and skips when they are not. See
[Running against AWS](#running-against-aws).

To build a gateway that registers this store:

```bash
cargo build -p mcpg --features s3-content-store --release
```

## Running against AWS

`tests/s3_live.rs` runs `s3_objects` against a real bucket and its real
event notifications on SQS, with the AWS default credential chain and no
`endpoint_url`. It uses two identities:

- **The watch**: the ambient chain, here the profile in `AWS_PROFILE`. It
  assumes a role that can only list the test prefix and read the queue.
- **The driver**: the profile in `AWS_LIVE_DRIVER_PROFILE`, an
  administrator. It writes and deletes the objects, puts the bucket's
  notification configuration back so that S3 sends `s3:TestEvent`, and
  reads the queue.

The suite skips unless `S3_LIVE_BUCKET`, `S3_LIVE_REGION` and
`S3_LIVE_QUEUE_URL` are set. Each run works under its own prefix,
`mcpg-live/<run>/`, and deletes it at the end. It checks `list` mode
(create, overwrite, delete, exact restart) and `notifications` mode (create,
delete, redelivery), and that `s3:TestEvent` gives no event. The queue is
for this suite only. The bucket is for this suite only too: the suite puts
the bucket's notification configuration back to make S3 send
`s3:TestEvent`, so it refuses a bucket whose configuration holds more than
one queue. While it waits for that `s3:TestEvent`, the suite deletes the
other messages on the queue.

### Set up

Run this once with the AWS CLI and `jq`. It makes the bucket, the queue,
the queue policy that lets S3 send this bucket's notifications, the
notification configuration, the watch role with its least-privilege policy,
and a profile that assumes the role.

```bash
#!/usr/bin/env bash
set -euo pipefail
export AWS_PROFILE=${AWS_LIVE_DRIVER_PROFILE:?set it to an administrator profile}
REGION=${AWS_REGION:-eu-west-1}
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET=mcpg-live-s3-$ACCOUNT-$REGION
QUEUE_ARN=arn:aws:sqs:$REGION:$ACCOUNT:mcpg-live-s3-events

if [ "$REGION" = us-east-1 ]; then
  aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" > /dev/null
else
  aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
    --create-bucket-configuration "LocationConstraint=$REGION" > /dev/null
fi
# What a failed run leaves expires after a day.
aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET" \
  --lifecycle-configuration '{"Rules":[{"ID":"mcpg-live","Status":"Enabled",
    "Filter":{"Prefix":"mcpg-live/"},"Expiration":{"Days":1}}]}'

# The queue must be in the bucket's region. S3 may send to it only for
# this bucket, in this account.
QUEUE=$(aws sqs create-queue --region "$REGION" --queue-name mcpg-live-s3-events \
  --query QueueUrl --output text)
queue_policy=$(cat <<EOF
{ "Version": "2012-10-17",
  "Statement": [{ "Effect": "Allow",
    "Principal": { "Service": "s3.amazonaws.com" },
    "Action": "sqs:SendMessage",
    "Resource": "$QUEUE_ARN",
    "Condition": {
      "ArnEquals": { "aws:SourceArn": "arn:aws:s3:::$BUCKET" },
      "StringEquals": { "aws:SourceAccount": "$ACCOUNT" } } }] }
EOF
)
aws sqs set-queue-attributes --region "$REGION" --queue-url "$QUEUE" \
  --attributes "$(jq -cn --arg policy "$queue_policy" '{Policy: $policy}')"

# S3 refuses this until the queue policy lets it send.
notifications=$(cat <<EOF
{ "QueueConfigurations": [{ "Id": "mcpg-live",
    "QueueArn": "$QUEUE_ARN",
    "Events": [ "s3:ObjectCreated:*", "s3:ObjectRemoved:*" ],
    "Filter": { "Key": { "FilterRules": [
      { "Name": "prefix", "Value": "mcpg-live/" } ] } } }] }
EOF
)
aws s3api put-bucket-notification-configuration --bucket "$BUCKET" \
  --notification-configuration "$notifications"

trust=$(cat <<EOF
{ "Version": "2012-10-17",
  "Statement": [{ "Effect": "Allow",
    "Principal": { "AWS": "arn:aws:iam::$ACCOUNT:root" },
    "Action": "sts:AssumeRole" }] }
EOF
)
policy=$(cat <<EOF
{ "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::$BUCKET",
      "Condition": { "StringLike": { "s3:prefix": "mcpg-live/*" } } },
    { "Effect": "Allow",
      "Action": [ "sqs:GetQueueAttributes", "sqs:ReceiveMessage",
                  "sqs:DeleteMessage", "sqs:ChangeMessageVisibility" ],
      "Resource": "$QUEUE_ARN" } ] }
EOF
)
aws iam create-role --role-name mcpg-live-s3-watch \
  --assume-role-policy-document "$trust" > /dev/null
aws iam put-role-policy --role-name mcpg-live-s3-watch \
  --policy-name s3-watch --policy-document "$policy"

aws configure set role_arn "arn:aws:iam::$ACCOUNT:role/mcpg-live-s3-watch" \
  --profile mcpg-live-s3-watch
aws configure set source_profile "$AWS_PROFILE" --profile mcpg-live-s3-watch
aws configure set region "$REGION" --profile mcpg-live-s3-watch

echo "export S3_LIVE_BUCKET=$BUCKET S3_LIVE_REGION=$REGION S3_LIVE_QUEUE_URL=$QUEUE"
```

The queue uses SQS-managed encryption, the default. A queue encrypted with
a customer-managed KMS key also needs a key policy that lets S3 use the key.
IAM can take some seconds before the new role can be assumed.

### Run

```bash
export AWS_PROFILE=mcpg-live-s3-watch
export AWS_LIVE_DRIVER_PROFILE=<your administrator profile>
export S3_LIVE_BUCKET=... S3_LIVE_REGION=... S3_LIVE_QUEUE_URL=...   # from the setup
cargo test -p mcpg-plugin-storage-s3 --features integration-tests --test s3_live -- --nocapture
```

The suite reads these variables from the environment and skips when one it
needs is not set. The SDK finds the profiles in `~/.aws/config` and the SSO
cache through `HOME`.

### Clean up

```bash
export AWS_PROFILE=$AWS_LIVE_DRIVER_PROFILE
aws s3 rm "s3://$S3_LIVE_BUCKET" --recursive --region "$S3_LIVE_REGION"
aws s3api delete-bucket --bucket "$S3_LIVE_BUCKET" --region "$S3_LIVE_REGION"
aws sqs delete-queue --queue-url "$S3_LIVE_QUEUE_URL" --region "$S3_LIVE_REGION"
aws iam delete-role-policy --role-name mcpg-live-s3-watch --policy-name s3-watch
aws iam delete-role --role-name mcpg-live-s3-watch
```

Then remove the `[profile mcpg-live-s3-watch]` section from
`~/.aws/config`.

## See also
- Full gateway config schema, including the `storage:` block: <https://mcpg.dev/docs/reference/configuration>
- Plugin classes and the plugin ABI: <https://mcpg.dev/docs/plugins/plugins-and-protocol>
- `mcpg-plugin-storage-builtin` — the always-present in-memory and filesystem
  stores.
