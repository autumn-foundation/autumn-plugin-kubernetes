//! Prints Kubernetes YAML for an Autumn app.
//!
//! `cargo run --example manifests -- <name> <image> [namespace] --profile prod`
//!
//! It loads the app config like autumn-web: `autumn.toml`, the profile
//! (`AUTUMN_ENV`, `AUTUMN_PROFILE`, or `--profile`), `autumn-<profile>.toml`,
//! `.env`, and env vars. So probes, ports, grace period, and the Role match
//! the app.

use autumn_plugin_kubernetes::KubernetesConfig;
use autumn_plugin_kubernetes::manifest::ManifestSpec;
use autumn_web::config::AutumnConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Positional args. `--profile <name>` and `--profile=<name>` are for
    // autumn's config loader, so skip them here.
    let mut args: Vec<String> = Vec::new();
    let mut raw = std::env::args().skip(1);
    while let Some(a) = raw.next() {
        if a == "--profile" {
            raw.next();
        } else if !a.starts_with("--profile=") {
            args.push(a);
        }
    }
    let (Some(name), Some(image)) = (args.first(), args.get(1)) else {
        eprintln!("usage: manifests <name> <image> [namespace] --profile <name>");
        std::process::exit(2);
    };
    // With no profile, autumn uses dev (1 s shutdown timeout). That gives a
    // grace period far too short for the pods. Ask for the pod profile.
    // Like autumn: an empty value selects no profile.
    let raw: Vec<String> = std::env::args().collect();
    let flag_value = raw.iter().enumerate().find_map(|(i, a)| {
        a.strip_prefix("--profile=").map(str::to_owned).or_else(|| {
            (a == "--profile")
                .then(|| raw.get(i + 1).cloned())
                .flatten()
        })
    });
    let profile_set = ["AUTUMN_ENV", "AUTUMN_PROFILE"]
        .iter()
        .any(|k| std::env::var(k).is_ok_and(|v| !v.trim().is_empty()))
        || flag_value.is_some_and(|v| !v.trim().is_empty());
    if !profile_set {
        eprintln!(
            "manifests: choose the profile that the pods run with: --profile prod \
             (or AUTUMN_ENV). The Deployment sets AUTUMN_ENV to it."
        );
        std::process::exit(2);
    }
    let autumn = AutumnConfig::load_lenient_unknown_roots()?;
    let kube = KubernetesConfig::load(autumn.profile_name())?;
    let mut spec = ManifestSpec::from_config(name.as_str(), image.as_str(), &autumn, &kube);
    if let Some(ns) = args.get(2) {
        spec.namespace.clone_from(ns);
    }
    print!("{}", spec.render_yaml()?);
    Ok(())
}
