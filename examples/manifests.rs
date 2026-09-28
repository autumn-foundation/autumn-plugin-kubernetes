//! Prints Kubernetes YAML for an Autumn app.
//!
//! `cargo run --example manifests -- <name> <image> [namespace]`
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
        eprintln!("usage: manifests <name> <image> [namespace] [--profile <name>]");
        std::process::exit(2);
    };
    let autumn = AutumnConfig::load_lenient_unknown_roots()?;
    let kube = KubernetesConfig::load(autumn.profile_name())?;
    let mut spec = ManifestSpec::from_config(name.as_str(), image.as_str(), &autumn, &kube);
    if let Some(ns) = args.get(2) {
        spec.namespace.clone_from(ns);
    }
    print!("{}", spec.render_yaml()?);
    Ok(())
}
