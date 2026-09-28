//! Prints Kubernetes YAML for an Autumn app.
//!
//! `cargo run --example manifests -- <name> <image> [namespace]`
//!
//! It reads `[kubernetes]` from `autumn.toml` (leader election, ConfigMaps,
//! events) so the Role has the right verbs.

use autumn_plugin_kubernetes::KubernetesConfig;
use autumn_plugin_kubernetes::manifest::ManifestSpec;
use autumn_web::config::AutumnConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let (Some(name), Some(image)) = (args.next(), args.next()) else {
        eprintln!("usage: manifests <name> <image> [namespace]");
        std::process::exit(2);
    };
    let kube = KubernetesConfig::load(None)?;
    let mut spec = ManifestSpec::from_config(name, image, &AutumnConfig::default(), &kube);
    if let Some(ns) = args.next() {
        spec.namespace = ns;
    }
    print!("{}", spec.render_yaml()?);
    Ok(())
}
