//! Config loading end to end, through `examples/manifests` (AC11, AC12).
//!
//! Like a Docker image: `AUTUMN_MANIFEST_DIR` names a build path that does
//! not exist, and the config files are in the working directory.
#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;
use std::process::Command;

fn example(name: &str) -> PathBuf {
    // target/<profile>/deps/load-<hash> -> target/<profile>/examples/<name>
    let exe = std::env::current_exe().unwrap();
    let dir = exe.parent().unwrap().parent().unwrap().join("examples");
    let path = dir.join(name);
    assert!(
        path.exists(),
        "build the examples first: {}",
        path.display()
    );
    path
}

fn temp_dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("akp-load-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn manifests_use_the_real_config_profile_and_dotenv() {
    let dir = temp_dir("full");
    std::fs::write(
        dir.join("autumn.toml"),
        r#"
[server]
port = 8080
shutdown_timeout_secs = 60

[health]
ready_path = "/readyz"

[kubernetes]
namespace = "base"

[profile.production.kubernetes]
events = false
"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("autumn-prod.toml"),
        "[kubernetes.leader_election]\nenabled = true\nlease_name = \"shop-leader\"\n",
    )
    .unwrap();
    std::fs::write(
        dir.join(".env"),
        "AUTUMN_KUBERNETES__CONFIG_MAPS__WATCH=flags\n",
    )
    .unwrap();
    let out = Command::new(example("manifests"))
        .args(["shop", "ghcr.io/acme/shop:1"])
        .current_dir(&dir)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("AUTUMN_ENV", "prod")
        .env("AUTUMN_MANIFEST_DIR", "/nonexistent/build/path")
        .output()
        .unwrap();
    let yaml = String::from_utf8_lossy(&out.stdout);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "{err}");
    assert!(yaml.contains("path: /readyz"), "health.ready_path:\n{yaml}");
    assert!(yaml.contains("containerPort: 8080"), "server.port:\n{yaml}");
    assert!(
        yaml.contains("namespace: base"),
        "[kubernetes] namespace:\n{yaml}"
    );
    assert!(yaml.contains("shop-leader"), "autumn-prod.toml:\n{yaml}");
    // Like autumn-web: `.env` is not read in prod (no AUTUMN_DOTENV=1), and
    // only from AUTUMN_MANIFEST_DIR when that is set.
    assert!(!yaml.contains("- flags"), ".env must not apply:\n{yaml}");
    assert!(
        !yaml.contains("events.k8s.io"),
        "[profile.production]:\n{yaml}"
    );
    // 5 (preStop) + 5 (prestop grace) + 60 (shutdown) + 10 (buffer).
    assert!(yaml.contains("terminationGracePeriodSeconds: 80"), "{yaml}");
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn dotenv_applies_in_dev_like_autumn() {
    let dir = temp_dir("dotenv");
    std::fs::write(
        dir.join("autumn.toml"),
        "[kubernetes]\nnamespace = \"dev-ns\"\n",
    )
    .unwrap();
    std::fs::write(
        dir.join(".env"),
        "AUTUMN_KUBERNETES__CONFIG_MAPS__WATCH=flags\nAUTUMN_KUBERNETES__NAMESPACE=from-dotenv\n",
    )
    .unwrap();
    let out = Command::new(example("manifests"))
        .args(["shop", "img", "--profile", "dev"])
        .current_dir(std::env::temp_dir())
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("AUTUMN_MANIFEST_DIR", &dir)
        // The process env wins over `.env`.
        .env("AUTUMN_KUBERNETES__NAMESPACE", "from-env")
        .output()
        .unwrap();
    let yaml = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(yaml.contains("- flags"), ".env applies in dev:\n{yaml}");
    assert!(yaml.contains("namespace: from-env"), "env wins:\n{yaml}");
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn manifests_usage_error_without_args() {
    let out = Command::new(example("manifests")).output().unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("usage"));
}
