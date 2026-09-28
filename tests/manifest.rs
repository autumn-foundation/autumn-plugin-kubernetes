//! Manifest generator (AC11).
#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default,
    clippy::similar_names
)]

use autumn_plugin_kubernetes::KubernetesConfig;
use autumn_plugin_kubernetes::manifest::ManifestSpec;
use autumn_web::ProcessRole;
use autumn_web::config::AutumnConfig;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::rbac::v1::Role;
use serde_json::{Value, json};

fn kinds(objs: &[Value]) -> Vec<&str> {
    objs.iter().map(|o| o["kind"].as_str().unwrap()).collect()
}

fn find<'a>(objs: &'a [Value], kind: &str) -> &'a Value {
    objs.iter().find(|o| o["kind"] == kind).unwrap()
}

fn full_spec() -> ManifestSpec {
    let mut s = ManifestSpec::new("shop", "ghcr.io/acme/shop:1.2.3");
    s.namespace = "prod".into();
    s.lease_name = Some("shop-leader".into());
    s.config_maps = vec!["flags".into(), "limits".into()];
    s.events = true;
    s
}

#[test]
fn defaults_follow_autumn() {
    let s = ManifestSpec::new("shop", "img");
    assert_eq!(s.port, 3000);
    assert_eq!(s.replicas, 2);
    assert_eq!(s.probes.live, "/live");
    assert_eq!(s.grace_period_secs(), 50, "5 + 5 + 30 + 10");
    assert_eq!(s.namespace, "default");
}

#[test]
fn from_config_reads_autumn_and_kubernetes_sections() {
    let mut autumn = AutumnConfig::default();
    autumn.server.port = 8080;
    autumn.server.shutdown_timeout_secs = 20;
    autumn.server.prestop_grace_secs = 3;
    autumn.health.ready_path = "/readyz".into();
    autumn.role = ProcessRole::Worker;
    let mut kube = KubernetesConfig::default();
    kube.namespace = "shop".into();
    kube.leader_election.enabled = true;
    kube.leader_election.lease_name = "l".into();
    kube.config_maps.watch = vec!["flags".into()];
    let s = ManifestSpec::from_config("shop", "img", &autumn, &kube);
    assert_eq!(s.port, 8080);
    assert_eq!(s.shutdown_timeout_secs, 20);
    assert_eq!(s.prestop_grace_secs, 3);
    assert_eq!(s.probes.ready, "/readyz");
    assert_eq!(s.role, Some(ProcessRole::Worker));
    assert_eq!(s.namespace, "shop");
    assert_eq!(s.lease_name.as_deref(), Some("l"));
    assert_eq!(s.config_maps, vec!["flags".to_owned()]);
    assert!(s.events);
    assert_eq!(s.grace_period_secs(), 5 + 3 + 20 + 10);

    assert!(s.api_access);
    kube.enabled = false;
    let off = ManifestSpec::from_config("shop", "img", &autumn, &kube);
    assert!(!off.api_access, "plugin off: no token");
    assert_eq!(off.lease_name, None, "plugin off: no RBAC");
    assert!(off.config_maps.is_empty());
    assert!(!off.events);
    assert_eq!(
        ManifestSpec::from_config(
            "shop",
            "img",
            &AutumnConfig::default(),
            &KubernetesConfig::default()
        )
        .role,
        None
    );
}

#[test]
fn full_spec_has_all_objects_in_apply_order() {
    let objs = full_spec().objects().unwrap();
    assert_eq!(
        kinds(&objs),
        vec![
            "ServiceAccount",
            "Role",
            "RoleBinding",
            "Deployment",
            "Service",
            "PodDisruptionBudget"
        ]
    );
    for o in &objs {
        assert_eq!(o["metadata"]["name"], "shop");
        assert_eq!(o["metadata"]["namespace"], "prod");
        assert_eq!(o["metadata"]["labels"]["app.kubernetes.io/name"], "shop");
        assert_eq!(
            o["metadata"]["labels"]["app.kubernetes.io/managed-by"],
            "autumn-plugin-kubernetes"
        );
    }
}

#[test]
fn role_has_only_needed_rules() {
    let objs = full_spec().objects().unwrap();
    let role: Role = serde_json::from_value(find(&objs, "Role").clone()).unwrap();
    let rules = role.rules.unwrap();
    let rule = |group: &str, res: &str| {
        rules
            .iter()
            .filter(|r| {
                r.api_groups.as_deref() == Some(&[group.to_owned()][..])
                    && r.resources.as_deref() == Some(&[res.to_owned()][..])
            })
            .collect::<Vec<_>>()
    };
    let leases = rule("coordination.k8s.io", "leases");
    assert_eq!(leases.len(), 1, "one rule, limited by name");
    assert_eq!(leases[0].verbs, ["create", "get", "patch", "update"]);
    assert_eq!(
        leases[0].resource_names.as_deref(),
        Some(&["shop-leader".to_owned()][..])
    );
    let cms = rule("", "configmaps");
    assert_eq!(cms.len(), 1);
    assert_eq!(cms[0].verbs, ["list", "watch"], "the watch needs no get");
    assert_eq!(
        cms[0].resource_names.as_deref(),
        Some(&["flags".to_owned(), "limits".to_owned()][..])
    );
    let events = rule("events.k8s.io", "events");
    assert_eq!(events[0].verbs, ["create", "patch"]);
    assert_eq!(rules.len(), 3);
}

#[test]
fn no_api_access_gives_no_role_and_no_token() {
    let mut s = ManifestSpec::new("shop", "img");
    s.events = false;
    s.api_access = false;
    s.replicas = 1;
    let objs = s.objects().unwrap();
    assert_eq!(
        kinds(&objs),
        vec!["ServiceAccount", "Deployment", "Service"]
    );
    let d = find(&objs, "Deployment");
    assert_eq!(
        d["spec"]["template"]["spec"]["automountServiceAccountToken"],
        false
    );
}

#[test]
fn api_access_mounts_the_token_even_with_no_rules() {
    let mut s = ManifestSpec::new("shop", "img");
    s.events = false;
    s.replicas = 1;
    assert!(s.api_access, "default: the plugin talks to the API");
    let objs = s.objects().unwrap();
    assert_eq!(
        kinds(&objs),
        vec!["ServiceAccount", "Deployment", "Service"]
    );
    let d = find(&objs, "Deployment");
    assert_eq!(
        d["spec"]["template"]["spec"]["automountServiceAccountToken"],
        true
    );
}

#[test]
fn deployment_wires_probes_env_prestop_and_grace() {
    let mut s = full_spec();
    s.role = Some(ProcessRole::Web);
    s.env.insert("RUST_LOG".into(), "info".into());
    let objs = s.objects().unwrap();
    let d: Deployment = serde_json::from_value(find(&objs, "Deployment").clone()).unwrap();
    let spec = d.spec.unwrap();
    assert_eq!(spec.replicas, Some(2));
    let pod = spec.template.spec.unwrap();
    assert_eq!(pod.termination_grace_period_seconds, Some(50));
    assert_eq!(pod.service_account_name.as_deref(), Some("shop"));
    assert_eq!(pod.automount_service_account_token, Some(true));
    let c = &pod.containers[0];
    assert_eq!(c.image.as_deref(), Some("ghcr.io/acme/shop:1.2.3"));
    assert_eq!(c.ports.as_ref().unwrap()[0].container_port, 3000);
    let path = |p: &Option<k8s_openapi::api::core::v1::Probe>| {
        p.as_ref()
            .unwrap()
            .http_get
            .as_ref()
            .unwrap()
            .path
            .clone()
            .unwrap()
    };
    assert_eq!(path(&c.liveness_probe), "/live");
    assert_eq!(path(&c.readiness_probe), "/ready");
    assert_eq!(path(&c.startup_probe), "/startup");
    assert_eq!(
        c.readiness_probe.as_ref().unwrap().failure_threshold,
        Some(1)
    );
    let pre = c.lifecycle.as_ref().unwrap().pre_stop.as_ref().unwrap();
    assert_eq!(pre.sleep.as_ref().unwrap().seconds, 5);
    let env: std::collections::BTreeMap<_, _> = c
        .env
        .as_ref()
        .unwrap()
        .iter()
        .map(|e| (e.name.clone(), e))
        .collect();
    for (name, field) in [
        ("POD_NAME", "metadata.name"),
        ("POD_NAMESPACE", "metadata.namespace"),
        ("POD_UID", "metadata.uid"),
        ("NODE_NAME", "spec.nodeName"),
        ("POD_IP", "status.podIP"),
        ("POD_SERVICE_ACCOUNT", "spec.serviceAccountName"),
    ] {
        let from = env[name].value_from.as_ref().unwrap();
        assert_eq!(from.field_ref.as_ref().unwrap().field_path, field, "{name}");
    }
    assert_eq!(
        env["AUTUMN_SERVER__HOST"].value.as_deref(),
        Some("0.0.0.0"),
        "bind all"
    );
    assert_eq!(env["AUTUMN_SERVER__PORT"].value.as_deref(), Some("3000"));
    assert_eq!(env["AUTUMN_ROLE"].value.as_deref(), Some("web"));
    assert_eq!(env["RUST_LOG"].value.as_deref(), Some("info"));
    let mount = &c.volume_mounts.as_ref().unwrap()[0];
    assert_eq!(mount.mount_path, "/etc/podinfo");
    let vol = &pod.volumes.as_ref().unwrap()[0];
    let items = vol.downward_api.as_ref().unwrap().items.as_ref().unwrap();
    assert_eq!(items[0].path, "labels");
    let sc = c.security_context.as_ref().unwrap();
    assert_eq!(sc.allow_privilege_escalation, Some(false));
    assert_eq!(sc.run_as_non_root, Some(true));
}

#[test]
fn no_prestop_hook_when_zero() {
    let mut s = full_spec();
    s.prestop_hook_secs = 0;
    let objs = s.objects().unwrap();
    let d = find(&objs, "Deployment");
    assert!(d["spec"]["template"]["spec"]["containers"][0]["lifecycle"].is_null());
    assert_eq!(
        d["spec"]["template"]["spec"]["terminationGracePeriodSeconds"],
        45
    );
}

#[test]
fn service_and_pdb() {
    let objs = full_spec().objects().unwrap();
    let svc = find(&objs, "Service");
    assert_eq!(svc["spec"]["ports"][0]["port"], 80);
    assert_eq!(svc["spec"]["ports"][0]["targetPort"], "http");
    assert_eq!(
        svc["spec"]["selector"],
        json!({"app.kubernetes.io/name": "shop"})
    );
    let pdb = find(&objs, "PodDisruptionBudget");
    assert_eq!(pdb["apiVersion"], "policy/v1");
    assert_eq!(pdb["spec"]["minAvailable"], 1);
}

#[test]
fn yaml_has_one_document_per_object_and_parses() {
    let yaml = full_spec().render_yaml().unwrap();
    let docs: Vec<&str> = yaml
        .split("\n---\n")
        .map(str::trim)
        .filter(|d| !d.is_empty())
        .collect();
    assert_eq!(docs.len(), 6, "{yaml}");
    let d: Deployment = serde_saphyr::from_str(docs[3]).unwrap();
    assert_eq!(d.metadata.name.as_deref(), Some("shop"));
    assert!(yaml.contains("terminationGracePeriodSeconds: 50"), "{yaml}");
}

#[test]
fn bad_specs_are_rejected() {
    let bad = |f: fn(&mut ManifestSpec), what: &str| {
        let mut s = full_spec();
        f(&mut s);
        let err = s.render_yaml().unwrap_err().to_string();
        assert!(err.contains(what), "{what}: {err}");
    };
    bad(|s| s.name = "1shop".into(), "name");
    bad(|s| s.name = "Shop".into(), "name");
    bad(|s| s.namespace = "a.b".into(), "namespace");
    bad(|s| s.image = " ".into(), "image");
    bad(|s| s.port = 0, "port");
    bad(|s| s.replicas = -1, "replicas");
    bad(|s| s.probes.live = "live".into(), "live");
    bad(|s| s.lease_name = Some("Bad".into()), "lease");
    bad(|s| s.config_maps = vec!["x_y".into()], "config map");
    bad(
        |s| {
            s.env.insert("1BAD".into(), "v".into());
        },
        "env",
    );
    bad(
        |s| {
            s.env.insert("POD_NAME".into(), "v".into());
        },
        "POD_NAME",
    );
    bad(|s| s.prestop_hook_secs = u64::MAX, "grace");
}

#[test]
fn role_binding_binds_the_service_account() {
    let objs = full_spec().objects().unwrap();
    let rb: k8s_openapi::api::rbac::v1::RoleBinding =
        serde_json::from_value(find(&objs, "RoleBinding").clone()).unwrap();
    assert_eq!(rb.role_ref.kind, "Role");
    assert_eq!(rb.role_ref.name, "shop");
    let subject = &rb.subjects.unwrap()[0];
    assert_eq!(
        (subject.kind.as_str(), subject.name.as_str()),
        ("ServiceAccount", "shop")
    );
    assert_eq!(subject.namespace.as_deref(), Some("prod"));
}
