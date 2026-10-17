use super::*;

fn stub_host() -> HostHandle {
    // SAFETY: `stub_host_ref` returns a process-static no-op host ref; the
    // factory ignores the host entirely.
    #[allow(unsafe_code)]
    unsafe {
        HostHandle::from_ffi(mcpg_plugin_sdk::testing::stub_host_ref())
    }
}

fn plugin() -> S3WatchCdylib {
    S3WatchCdylib::from_host_config("", stub_host())
}

fn refused(spec: Value) -> String {
    match plugin().watch("mcpg-event://drops", &spec, Box::new(|_| {})) {
        Err(WatchError::InvalidSpec { message }) => message,
        Err(other) => panic!("not a spec error: {other:?}"),
        Ok(handle) => {
            plugin().cancel(handle);
            panic!("accepted {spec}");
        }
    }
}

fn scope(prefix: &str, recursive: bool, pattern: Option<&str>) -> Scope {
    Scope {
        bucket: "drops".into(),
        prefix: prefix.into(),
        recursive,
        pattern: pattern.map(|p| Pattern::new(p).unwrap()),
        location: format!("s3://drops/{prefix}"),
    }
}

fn record(key: &str, created: bool) -> Record {
    Record {
        bucket: "drops".into(),
        key: key.into(),
        created,
        size: Some(5),
        etag: Some("\"abc\"".into()),
        version: None,
        sequencer: Some("0055AED6DCD90281E5".into()),
        event_time: Some("2026-10-10T12:00:00.000Z".into()),
        event_name: if created {
            "ObjectCreated:Put".into()
        } else {
            "ObjectRemoved:Delete".into()
        },
    }
}

#[test]
fn manifest_and_kind_are_correct() {
    use mcpg_plugin_protocol::PluginClass;
    let p = plugin();
    assert_eq!(SyncWatchStrategyPlugin::manifest(&p).id, PLUGIN_ID);
    assert_eq!(
        SyncWatchStrategyPlugin::manifest(&p).plugin_class,
        PluginClass::WatchStrategy
    );
    assert_eq!(p.kind(), WATCH_KIND);
}

#[test]
fn spec_parses_with_defaults() {
    let parsed: WatchSpec =
        serde_json::from_value(json!({ "bucket": "drops", "region": "eu-west-1" })).unwrap();
    assert_eq!(parsed.mode, Mode::List);
    assert!(parsed.recursive);
    assert_eq!(parsed.interval_ms, 60_000);
    assert_eq!(parsed.timeout_ms, 30_000);
    assert_eq!(parsed.max_files, 10_000);
    assert_eq!(parsed.start_at, InventoryStart::Now);
    assert!(parsed.notifications.is_none());
}

#[test]
fn a_bad_spec_is_refused_before_anything_is_called() {
    let with = |extra: Value| {
        let mut spec = json!({ "bucket": "drops", "region": "eu-west-1" });
        for (k, v) in extra.as_object().unwrap() {
            spec[k] = v.clone();
        }
        spec
    };
    assert!(refused(with(json!({ "bogus": 1 }))).contains("unknown field"));
    assert!(refused(with(json!({ "bucket": " " }))).contains("bucket"));
    assert!(refused(with(json!({ "region": "" }))).contains("region"));
    assert!(refused(with(json!({ "access_key_id": "AKIA" }))).contains("go together"));
    assert!(
        refused(with(
            json!({ "access_key_id": "cred://vault/aws", "secret_access_key": "s" })
        ))
        .contains("cred://")
    );
    assert!(refused(with(json!({ "pattern": "[a" }))).contains("pattern"));
    assert!(refused(with(json!({ "max_files": 0 }))).contains("max_files"));
    assert!(refused(with(json!({ "timeout_ms": 0 }))).contains("timeout_ms"));
    assert!(refused(with(json!({ "mode": "notifications" }))).contains("notifications"));
    assert!(
        refused(with(json!({ "notifications": { "queue_url": "sqs://q" } }))).contains("queue_url")
    );
    assert!(
        refused(with(json!({ "notifications": {
            "queue_url": "https://sqs.eu-west-1.amazonaws.com/1/q", "wait_time_secs": 0 } })))
        .contains("wait_time_secs")
    );
    assert!(refused(with(json!({ "ack_timeout_ms": 50_000_000u64 }))).contains("ack_timeout_ms"));
    assert!(refused(with(json!({ "interval_ms": 10 }))).contains("interval_ms"));
}

#[test]
fn a_notification_key_is_url_decoded() {
    assert_eq!(
        decode_key("in%2F2026%2Freport+1.csv"),
        "in/2026/report 1.csv"
    );
    assert_eq!(decode_key("caf%C3%A9.txt"), "café.txt");
    assert_eq!(decode_key("100%"), "100%");
    assert_eq!(decode_key("%zz"), "%zz");
}

#[test]
fn an_s3_notification_becomes_records() {
    let body = json!({ "Records": [
        {
            "eventName": "ObjectCreated:Put",
            "eventTime": "2026-10-10T12:00:00.000Z",
            "s3": { "bucket": { "name": "drops" }, "object": {
                "key": "in/a+b.csv", "size": 7, "eTag": "e1", "sequencer": "01" } }
        },
        {
            "eventName": "ObjectRemoved:DeleteMarkerCreated",
            "s3": { "bucket": { "name": "drops" }, "object": { "key": "in/old.csv", "versionId": "v2" } }
        },
        {
            "eventName": "ObjectTagging:Put",
            "s3": { "bucket": { "name": "drops" }, "object": { "key": "in/tagged.csv" } }
        }
    ]})
    .to_string();
    let records = parse_notification(&body);
    assert_eq!(records.len(), 2, "a tagging change is not an object change");
    assert_eq!(records[0].key, "in/a b.csv");
    assert!(records[0].created);
    assert_eq!(records[0].size, Some(7));
    assert_eq!(records[0].sequencer.as_deref(), Some("01"));
    assert!(!records[1].created);
    assert_eq!(records[1].version.as_deref(), Some("v2"));
}

#[test]
fn the_test_event_sns_and_eventbridge_shapes_are_read() {
    let test_event = json!({ "Service": "Amazon S3", "Event": "s3:TestEvent", "Bucket": "drops" });
    assert!(parse_notification(&test_event.to_string()).is_empty());
    assert!(parse_notification("not json").is_empty());

    let inner = json!({ "Records": [{
        "eventName": "ObjectCreated:Copy",
        "s3": { "bucket": { "name": "drops" }, "object": { "key": "x.csv" } }
    }]});
    let sns = json!({ "Type": "Notification", "Message": inner.to_string() });
    assert_eq!(parse_notification(&sns.to_string())[0].key, "x.csv");

    let eventbridge = json!({
        "source": "aws.s3",
        "detail-type": "Object Deleted",
        "time": "2026-10-10T12:00:00Z",
        "detail": { "bucket": { "name": "drops" }, "object": {
            "key": "in/raw key.csv", "sequencer": "02" }, "reason": "DeleteObject" }
    });
    let records = parse_notification(&eventbridge.to_string());
    assert_eq!(
        records[0].key, "in/raw key.csv",
        "EventBridge keys are not encoded"
    );
    assert!(!records[0].created);
    assert_eq!(records[0].event_name, "Object Deleted:DeleteObject");
    let other =
        json!({ "source": "aws.s3", "detail-type": "Object Restore Completed", "detail": {} });
    assert!(parse_notification(&other.to_string()).is_empty());
}

#[test]
fn the_scope_keeps_its_bucket_prefix_depth_and_pattern() {
    let all = scope("in/", true, None);
    assert_eq!(all.relative(&record("in/a.csv", true)), Some("a.csv"));
    assert_eq!(
        all.relative(&record("in/sub/b.csv", true)),
        Some("sub/b.csv")
    );
    assert_eq!(all.relative(&record("out/a.csv", true)), None);
    assert_eq!(
        all.relative(&record("in/sub/", true)),
        None,
        "a folder marker"
    );
    let other_bucket = Record {
        bucket: "elsewhere".into(),
        ..record("in/a.csv", true)
    };
    assert_eq!(all.relative(&other_bucket), None);
    let flat = scope("in/", false, None);
    assert_eq!(flat.relative(&record("in/sub/b.csv", true)), None);
    let csv = scope("in/", true, Some("*.csv"));
    assert_eq!(csv.relative(&record("in/a.csv", true)), Some("a.csv"));
    assert_eq!(
        csv.relative(&record("in/sub/b.csv", true)),
        None,
        "`*` stays in a folder"
    );
    assert_eq!(csv.relative(&record("in/a.txt", true)), None);
}

#[test]
fn a_record_is_an_object_changed_event_with_a_stable_id() {
    let scope = scope("in/", true, None);
    let created = scope.event(&record("in/a.csv", true), "a.csv");
    assert_eq!(
        created.event_type.as_deref(),
        Some(OBJECT_CHANGED_EVENT_TYPE)
    );
    let data = created.data.as_ref().unwrap();
    assert_eq!(data["op"], json!("created"));
    assert_eq!(data["store"], json!("s3"));
    assert_eq!(data["location"], json!("s3://drops/in/"));
    assert_eq!(data["key"], json!("a.csv"));
    assert_eq!(data["size"], json!(5));
    assert_eq!(data["etag"], json!("abc"));
    assert_eq!(data["modifiedAt"], json!("2026-10-10T12:00:00.000Z"));
    assert_eq!(
        scope.event(&record("in/a.csv", true), "a.csv").event_id,
        created.event_id,
        "a redelivered record keeps its id"
    );
    let later = Record {
        sequencer: Some("0055AED6DCD90281FF".into()),
        ..record("in/a.csv", true)
    };
    assert_ne!(scope.event(&later, "a.csv").event_id, created.event_id);

    let deleted = scope.event(&record("in/a.csv", false), "a.csv");
    let data = deleted.data.unwrap();
    assert_eq!(data["op"], json!("deleted"));
    assert!(data.get("size").is_none() && data.get("modifiedAt").is_none());
}
