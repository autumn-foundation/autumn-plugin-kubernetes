//! `[kubernetes]` config.
//!
//! Layers, from low to high:
//! 1. `autumn.toml` `[kubernetes]`.
//! 2. `autumn.toml` `[profile.<name>.kubernetes]`. For `prod`, the
//!    `production` section applies first, then `prod` (like autumn-web).
//! 3. `autumn-<profile>.toml` `[kubernetes]`.
//! 4. `.env` values, then env vars: `AUTUMN_KUBERNETES__<PATH>`, for example
//!    `AUTUMN_KUBERNETES__LEADER_ELECTION__ENABLED=true`. A list is comma
//!    separated.
//!
//! Files: each file is read from `$AUTUMN_MANIFEST_DIR` if it is there,
//! else from the working directory (like autumn-web).
//!
//! With `server.strict_config`, autumn 0.7 accepts `[kubernetes]` only at the
//! top level. Put profile values in `autumn-<profile>.toml`, not in
//! `[profile.<name>.kubernetes]`.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::KubeError;

/// The config section name.
pub const SECTION: &str = "kubernetes";
/// The env var prefix.
pub const ENV_PREFIX: &str = "AUTUMN_KUBERNETES__";

/// `[kubernetes]` config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KubernetesConfig {
    /// Master switch. When `false`, the plugin gives only `PodInfo`.
    pub enabled: bool,
    /// Stop startup when no cluster is found. Default: `false` (run detached).
    pub required: bool,
    /// Namespace for leases and ConfigMaps. Empty: the pod namespace, then
    /// the client default.
    pub namespace: String,
    /// Write Kubernetes Events on the pod.
    pub events: bool,
    /// Lease leader election.
    pub leader_election: LeaderElectionConfig,
    /// ConfigMaps to watch.
    pub config_maps: ConfigMapsConfig,
}

/// `[kubernetes.leader_election]` config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LeaderElectionConfig {
    /// Run leader election.
    pub enabled: bool,
    /// Lease name. Necessary when enabled.
    pub lease_name: String,
    /// Holder identity. Empty: `<name>-<8 random hex digits>`, where `<name>`
    /// is `POD_NAME`, else `HOSTNAME`, else `autumn`. Each process needs its
    /// own identity. Do not give one fixed value to many replicas.
    pub identity: String,
    /// Lease duration. Other replicas take over after this time.
    pub lease_duration_secs: u64,
    /// The leader stops after this time with no good renew.
    pub renew_deadline_secs: u64,
    /// Time between tries.
    pub retry_period_secs: u64,
    /// Clear the holder on shutdown, so another replica takes over at once.
    pub release_on_shutdown: bool,
    /// With no cluster (detached), act as leader so leader tasks run. For
    /// local development only. Default: `false`.
    pub lead_when_detached: bool,
    /// Process roles that take part (`combined`, `web`, `worker`). Empty: all.
    /// A leader task runs only on a replica that takes part.
    pub roles: Vec<String>,
}

/// `[kubernetes.config_maps]` config.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigMapsConfig {
    /// ConfigMap names to watch in the namespace.
    pub watch: Vec<String>,
}

impl Default for KubernetesConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            required: false,
            namespace: String::new(),
            events: true,
            leader_election: LeaderElectionConfig::default(),
            config_maps: ConfigMapsConfig::default(),
        }
    }
}

impl Default for LeaderElectionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            lease_name: String::new(),
            identity: String::new(),
            lease_duration_secs: 15,
            renew_deadline_secs: 10,
            retry_period_secs: 2,
            release_on_shutdown: true,
            lead_when_detached: false,
            roles: Vec::new(),
        }
    }
}

impl LeaderElectionConfig {
    /// Returns `true` when a process with `role` takes part in the election.
    #[must_use]
    pub fn campaigns_for(&self, role: autumn_web::ProcessRole) -> bool {
        self.roles.is_empty()
            || self
                .roles
                .iter()
                .any(|r| autumn_web::ProcessRole::from_env_value(r) == Some(role))
    }

    /// Lease duration in milliseconds.
    #[must_use]
    pub const fn lease_duration_ms(&self) -> u64 {
        self.lease_duration_secs.saturating_mul(1000)
    }

    /// Renew deadline in milliseconds.
    #[must_use]
    pub const fn renew_deadline_ms(&self) -> u64 {
        self.renew_deadline_secs.saturating_mul(1000)
    }

    /// Retry period in milliseconds.
    #[must_use]
    pub const fn retry_period_ms(&self) -> u64 {
        self.retry_period_secs.saturating_mul(1000)
    }
}

fn config_err(msg: impl std::fmt::Display) -> KubeError {
    KubeError::Config(msg.to_string())
}

fn parse_toml(text: &str, what: &str) -> Result<toml::Table, KubeError> {
    text.parse::<toml::Table>()
        .map_err(|e| config_err(format!("{what}: {e}")))
}

/// Returns the `[kubernetes]` table at `path` in `table`, if any.
fn section<'a>(table: &'a toml::Table, path: &[&str]) -> Option<&'a toml::Table> {
    let mut t = table;
    for key in path {
        t = t.get(*key)?.as_table()?;
    }
    Some(t)
}

/// Deep merge: `over` wins. Tables merge key by key.
fn merge(base: &mut toml::Table, over: &toml::Table) {
    for (k, v) in over {
        match (base.get_mut(k), v) {
            (Some(toml::Value::Table(b)), toml::Value::Table(o)) => merge(b, o),
            _ => {
                base.insert(k.clone(), v.clone());
            }
        }
    }
}

/// Sets one env override. The default value at the path gives the type.
fn apply_env(
    table: &mut toml::Table,
    defaults: &toml::Table,
    key: &str,
    value: &str,
) -> Result<(), KubeError> {
    let path: Vec<String> = key[ENV_PREFIX.len()..]
        .split("__")
        .map(str::to_ascii_lowercase)
        .collect();
    let bad = |why: &str| config_err(format!("{key}: {why}"));
    let (leaf, parents) = path.split_last().ok_or_else(|| bad("empty path"))?;
    let mut shape = defaults;
    let mut target = table;
    for p in parents {
        shape = shape
            .get(p)
            .and_then(toml::Value::as_table)
            .ok_or_else(|| bad("unknown key"))?;
        target = target
            .entry(p.clone())
            .or_insert_with(|| toml::Value::Table(toml::Table::new()))
            .as_table_mut()
            .ok_or_else(|| bad("not a table"))?;
    }
    let typed = match shape.get(leaf).ok_or_else(|| bad("unknown key"))? {
        toml::Value::Boolean(_) => match value.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => toml::Value::Boolean(true),
            "false" | "0" | "no" | "off" => toml::Value::Boolean(false),
            _ => return Err(bad("expected true or false")),
        },
        toml::Value::Integer(_) => {
            let n: u64 = value
                .trim()
                .parse()
                .map_err(|_| bad("expected a whole number of 0 or more"))?;
            toml::Value::Integer(i64::try_from(n).map_err(|_| bad("number too large"))?)
        }
        toml::Value::String(_) => toml::Value::String(value.to_owned()),
        toml::Value::Array(_) => toml::Value::Array(
            value
                .split(',')
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(|v| toml::Value::String(v.to_owned()))
                .collect(),
        ),
        _ => return Err(bad("a table cannot be set from one env var")),
    };
    target.insert(leaf.clone(), typed);
    Ok(())
}

/// A `.env` error. A parse error can repeat the line (a secret), so it shows
/// only the file and the line number. A read error (line 0) shows its text.
fn dotenv_error(e: &autumn_web::dotenv::DotenvError) -> KubeError {
    if e.line == 0 {
        config_err(format!(".env: {}: {}", e.path.display(), e.message))
    } else {
        config_err(format!(".env: {}:{}: not valid", e.path.display(), e.line))
    }
}

/// `dir/file` if it exists, else `file` in the working directory. autumn-web
/// does the same. Thus a build path in the binary does not stop the plugin
/// from finding files.
pub(crate) fn find_file(dir: &Path, file: &str) -> std::path::PathBuf {
    let candidate = dir.join(file);
    if candidate.exists() {
        candidate
    } else {
        std::path::PathBuf::from(file)
    }
}

/// The value of `--profile <name>` or `--profile=<name>` in the process args.
fn profile_flag() -> Option<String> {
    let args: Vec<String> = std::env::args_os()
        .filter_map(|a| a.into_string().ok())
        .collect();
    profile_flag_in(&args)
}

fn profile_flag_in(args: &[String]) -> Option<String> {
    args.iter()
        .enumerate()
        .find_map(|(i, a)| {
            a.strip_prefix("--profile=").map(str::to_owned).or_else(|| {
                (a == "--profile")
                    .then(|| args.get(i + 1).cloned())
                    .flatten()
            })
        })
        // Like autumn-web: an empty value selects nothing.
        .filter(|p| !p.trim().is_empty())
}

/// Inline `[profile.<name>]` merge order of autumn-web: the long alias first,
/// so the short name wins.
fn inline_profile_order(profile_names: &[String]) -> Vec<String> {
    let mut names = profile_names.to_vec();
    names.sort_by_key(|n| match n.as_str() {
        "production" | "development" => 0,
        _ => 1,
    });
    names
}

fn read_optional(path: &Path) -> Result<Option<String>, KubeError> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(config_err(format!("{}: {e}", path.display()))),
    }
}

impl KubernetesConfig {
    /// Reads `[kubernetes]` from one `autumn.toml` text.
    ///
    /// # Errors
    /// Returns [`KubeError::Config`] for bad TOML or unknown keys.
    pub fn from_toml_str(autumn_toml: &str) -> Result<Self, KubeError> {
        Self::from_sources(Some(autumn_toml), None, None, std::iter::empty())
    }

    /// Merges the layers. See the module docs.
    ///
    /// # Errors
    /// Returns [`KubeError::Config`] for bad TOML, unknown keys, or bad env
    /// values.
    pub fn from_sources(
        base: Option<&str>,
        profile: Option<&str>,
        profile_file: Option<&str>,
        env: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, KubeError> {
        let inline: Vec<String> = profile.map(str::to_owned).into_iter().collect();
        Self::from_layers(base, &inline, profile_file, env)
    }

    /// Like [`Self::from_sources`], with several inline profile sections
    /// merged in order (the last wins).
    ///
    /// # Errors
    /// Same as [`Self::from_sources`].
    pub fn from_layers(
        base: Option<&str>,
        inline_profiles: &[String],
        profile_file: Option<&str>,
        env: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, KubeError> {
        let mut merged = toml::Table::new();
        if let Some(text) = base {
            let base = parse_toml(text, "autumn.toml")?;
            if let Some(t) = section(&base, &[SECTION]) {
                merge(&mut merged, t);
            }
            for name in inline_profiles {
                if let Some(t) = section(&base, &["profile", name, SECTION]) {
                    merge(&mut merged, t);
                }
            }
        }
        if let Some(text) = profile_file {
            let file = parse_toml(text, "profile file")?;
            if let Some(t) = section(&file, &[SECTION]) {
                merge(&mut merged, t);
            }
        }
        let defaults = toml::Table::try_from(Self::default()).map_err(config_err)?;
        let mut vars: Vec<(String, String)> = env
            .into_iter()
            .filter(|(k, _)| k.starts_with(ENV_PREFIX))
            .collect();
        // Stable order, so one error message is always the same.
        vars.sort();
        for (k, v) in &vars {
            apply_env(&mut merged, &defaults, k, v)?;
        }
        toml::Value::Table(merged)
            .try_into()
            .map_err(|e| config_err(format!("[{SECTION}]: {e}")))
    }

    /// Reads `autumn.toml` and the first `autumn-<name>.toml` in
    /// `profile_names` from `dir`.
    ///
    /// # Errors
    /// Same as [`Self::from_sources`], and file read errors.
    pub fn load_from_dir(
        dir: &Path,
        profile_names: &[String],
        env: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, KubeError> {
        let base = read_optional(&find_file(dir, "autumn.toml"))?;
        let mut profile_file = None;
        for name in profile_names {
            if let Some(text) = read_optional(&find_file(dir, &format!("autumn-{name}.toml")))? {
                profile_file = Some(text);
                break;
            }
        }
        Self::from_layers(
            base.as_deref(),
            &inline_profile_order(profile_names),
            profile_file.as_deref(),
            env,
        )
    }

    /// Reads the config like autumn-web does: files in
    /// `$AUTUMN_MANIFEST_DIR` (or `.`), the app profile, and the process env.
    ///
    /// # Errors
    /// Same as [`Self::load_from_dir`].
    pub fn load(profile: Option<&str>) -> Result<Self, KubeError> {
        use autumn_web::config::Env as _;
        let os = autumn_web::config::OsEnv;
        let names = profile
            .map(|p| {
                // Same selector order as autumn-web.
                let selector = ["AUTUMN_ENV", "AUTUMN_PROFILE"]
                    .iter()
                    .find_map(|k| os.var(k).ok().filter(|v| !v.trim().is_empty()))
                    .or_else(profile_flag)
                    .map_or_else(|| p.to_owned(), |v| v.trim().to_owned());
                autumn_web::config::profile_override_file_lookup_names(p, &selector)
            })
            .unwrap_or_default();
        let dir = os
            .var("AUTUMN_MANIFEST_DIR")
            .map_or_else(|_| std::path::PathBuf::from("."), std::path::PathBuf::from);
        // `.env` first, then the process env, so the process env wins.
        let mut env = autumn_web::dotenv::resolve_process_dotenv().map_err(|e| dotenv_error(&e))?;
        env.extend(
            std::env::vars_os()
                .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?))),
        );
        let env = env.into_iter().filter(|(k, _)| k.starts_with(ENV_PREFIX));
        Self::load_from_dir(&dir, &names, env)
    }

    /// Checks names and lease timing.
    ///
    /// # Errors
    /// Returns [`KubeError::Config`] with the first problem.
    pub fn validate(&self) -> Result<(), KubeError> {
        if !self.namespace.is_empty() && !is_dns_label(&self.namespace) {
            return Err(config_err(format!(
                "namespace {:?} is not a DNS-1123 label",
                self.namespace
            )));
        }
        let le = &self.leader_election;
        if le.enabled {
            if le.lease_name.is_empty() {
                return Err(config_err(
                    "leader_election.lease_name is necessary when leader_election.enabled = true",
                ));
            }
            if !is_dns_subdomain(&le.lease_name) {
                return Err(config_err(format!(
                    "leader_election.lease_name {:?} is not a DNS-1123 subdomain",
                    le.lease_name
                )));
            }
            if !crate::policy::timing_valid(
                le.lease_duration_ms(),
                le.renew_deadline_ms(),
                le.retry_period_ms(),
            ) {
                return Err(config_err(format!(
                    "leader_election timing is not safe: need retry_period_secs > 0, \
                     renew_deadline_secs > 1.2 * retry_period_secs, \
                     lease_duration_secs > renew_deadline_secs, and \
                     lease_duration_secs <= {} (got {}, {}, {})",
                    crate::policy::MAX_LEASE_MS / 1000,
                    le.lease_duration_secs,
                    le.renew_deadline_secs,
                    le.retry_period_secs
                )));
            }
            if let Some(bad) = le
                .roles
                .iter()
                .find(|r| autumn_web::ProcessRole::from_env_value(r).is_none())
            {
                return Err(config_err(format!(
                    "leader_election.roles: {bad:?} is not combined, web, or worker"
                )));
            }
        }
        let mut seen = std::collections::BTreeSet::new();
        for name in &self.config_maps.watch {
            if !is_dns_subdomain(name) {
                return Err(config_err(format!(
                    "config_maps.watch: {name:?} is not a DNS-1123 subdomain"
                )));
            }
            if !seen.insert(name) {
                return Err(config_err(format!(
                    "config_maps.watch: {name:?} is listed twice"
                )));
            }
        }
        Ok(())
    }
}

/// Returns `true` for a valid DNS-1123 subdomain (most object names).
#[must_use]
pub fn is_dns_subdomain(name: &str) -> bool {
    !name.is_empty() && name.len() <= 253 && name.split('.').all(is_dns_label)
}

/// Returns `true` for a valid DNS-1123 label (namespaces).
#[must_use]
pub fn is_dns_label(name: &str) -> bool {
    let bytes = name.as_bytes();
    let ok = |b: &u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    match (bytes.first(), bytes.last()) {
        (Some(first), Some(last)) => {
            bytes.len() <= 63 && ok(first) && ok(last) && bytes.iter().all(|b| ok(b) || *b == b'-')
        }
        _ => false,
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    fn leader(name: &str) -> KubernetesConfig {
        let mut c = KubernetesConfig::default();
        c.leader_election.enabled = true;
        c.leader_election.lease_name = name.to_owned();
        c
    }

    #[test]
    fn defaults_are_safe() {
        let c = KubernetesConfig::default();
        assert!(c.enabled);
        assert!(!c.required);
        assert!(c.events);
        assert!(c.namespace.is_empty());
        assert!(!c.leader_election.enabled);
        assert!(c.leader_election.release_on_shutdown);
        assert_eq!(c.leader_election.lease_duration_ms(), 15_000);
        assert_eq!(c.leader_election.renew_deadline_ms(), 10_000);
        assert_eq!(c.leader_election.retry_period_ms(), 2_000);
        assert!(c.config_maps.watch.is_empty());
        c.validate().unwrap();
    }

    #[test]
    fn reads_section_and_ignores_other_sections() {
        let c = KubernetesConfig::from_toml_str(
            r#"
            [server]
            port = 3000
            [kubernetes]
            namespace = "shop"
            [kubernetes.leader_election]
            enabled = true
            lease_name = "shop-leader"
            [kubernetes.config_maps]
            watch = ["flags"]
            "#,
        )
        .unwrap();
        assert_eq!(c.namespace, "shop");
        assert!(c.leader_election.enabled);
        assert_eq!(c.leader_election.lease_name, "shop-leader");
        assert_eq!(c.config_maps.watch, vec!["flags".to_owned()]);
    }

    #[test]
    fn missing_section_gives_defaults() {
        assert_eq!(
            KubernetesConfig::from_toml_str("[server]\nport = 1").unwrap(),
            KubernetesConfig::default()
        );
    }

    #[test]
    fn unknown_key_is_an_error() {
        let err = KubernetesConfig::from_toml_str("[kubernetes]\nnamspace = \"x\"").unwrap_err();
        assert!(
            matches!(err, KubeError::Config(ref m) if m.contains("namspace")),
            "{err}"
        );
    }

    #[test]
    fn bad_toml_is_an_error() {
        assert!(matches!(
            KubernetesConfig::from_toml_str("[kubernetes"),
            Err(KubeError::Config(_))
        ));
    }

    #[test]
    fn layers_apply_in_order() {
        let base = r#"
            [kubernetes]
            namespace = "base"
            events = false
            [profile.prod.kubernetes]
            namespace = "inline"
            required = true
        "#;
        let file = "[kubernetes]\nnamespace = \"file\"";
        let c =
            KubernetesConfig::from_sources(Some(base), Some("prod"), Some(file), env(&[])).unwrap();
        assert_eq!(c.namespace, "file");
        assert!(c.required, "inline profile applies");
        assert!(!c.events, "base applies");

        let c = KubernetesConfig::from_sources(
            Some(base),
            Some("prod"),
            Some(file),
            env(&[("AUTUMN_KUBERNETES__NAMESPACE", "env")]),
        )
        .unwrap();
        assert_eq!(c.namespace, "env");
    }

    #[test]
    fn inline_profile_needs_matching_name() {
        let base = "[profile.prod.kubernetes]\nnamespace = \"inline\"";
        let c = KubernetesConfig::from_sources(Some(base), Some("dev"), None, env(&[])).unwrap();
        assert_eq!(c.namespace, "");
    }

    #[test]
    fn env_values_take_the_type_of_the_field() {
        let c = KubernetesConfig::from_sources(
            None,
            None,
            None,
            env(&[
                ("AUTUMN_KUBERNETES__LEADER_ELECTION__ENABLED", "true"),
                ("AUTUMN_KUBERNETES__LEADER_ELECTION__LEASE_NAME", "l"),
                (
                    "AUTUMN_KUBERNETES__LEADER_ELECTION__LEASE_DURATION_SECS",
                    "30",
                ),
                ("AUTUMN_KUBERNETES__CONFIG_MAPS__WATCH", "a, b"),
                ("AUTUMN_KUBERNETES__EVENTS", "FALSE"),
                ("OTHER", "x"),
            ]),
        )
        .unwrap();
        assert!(c.leader_election.enabled);
        assert_eq!(c.leader_election.lease_name, "l");
        assert_eq!(c.leader_election.lease_duration_secs, 30);
        assert_eq!(c.config_maps.watch, vec!["a".to_owned(), "b".to_owned()]);
        assert!(!c.events);
    }

    #[test]
    fn empty_env_list_is_empty() {
        let c = KubernetesConfig::from_sources(
            Some("[kubernetes.config_maps]\nwatch = [\"a\"]"),
            None,
            None,
            env(&[("AUTUMN_KUBERNETES__CONFIG_MAPS__WATCH", "")]),
        )
        .unwrap();
        assert!(c.config_maps.watch.is_empty());
    }

    #[test]
    fn bad_env_values_are_errors() {
        for (k, v) in [
            ("AUTUMN_KUBERNETES__EVENTS", "maybe"),
            (
                "AUTUMN_KUBERNETES__LEADER_ELECTION__LEASE_DURATION_SECS",
                "ten",
            ),
            (
                "AUTUMN_KUBERNETES__LEADER_ELECTION__LEASE_DURATION_SECS",
                "-1",
            ),
            ("AUTUMN_KUBERNETES__NO_SUCH_KEY", "1"),
            ("AUTUMN_KUBERNETES__LEADER_ELECTION", "1"),
        ] {
            let err = KubernetesConfig::from_sources(None, None, None, env(&[(k, v)])).unwrap_err();
            assert!(
                matches!(err, KubeError::Config(ref m) if m.contains(k)),
                "{k}: {err}"
            );
        }
    }

    #[test]
    fn validate_needs_lease_name() {
        let err = leader("").validate().unwrap_err();
        assert!(err.to_string().contains("lease_name"), "{err}");
        leader("app-leader").validate().unwrap();
    }

    #[test]
    fn validate_checks_timing() {
        let mut c = leader("l");
        c.leader_election.renew_deadline_secs = 15;
        let err = c.validate().unwrap_err();
        assert!(err.to_string().contains("renew_deadline_secs"), "{err}");
        c.leader_election.renew_deadline_secs = 10;
        c.leader_election.retry_period_secs = 9;
        assert!(c.validate().is_err());
        c.leader_election.retry_period_secs = 0;
        assert!(c.validate().is_err());
        c.leader_election.retry_period_secs = 2;
        c.leader_election.lease_duration_secs = u64::MAX;
        assert!(c.validate().is_err(), "overflow must not wrap");
    }

    #[test]
    fn disabled_leader_skips_timing_rules() {
        let mut c = KubernetesConfig::default();
        c.leader_election.renew_deadline_secs = 99;
        c.validate().unwrap();
    }

    #[test]
    fn roles_gate_the_campaign() {
        use autumn_web::ProcessRole;
        let mut le = LeaderElectionConfig::default();
        assert!(le.campaigns_for(ProcessRole::Web), "empty list: all roles");
        le.roles = vec!["worker".into(), "Combined".into()];
        assert!(le.campaigns_for(ProcessRole::Worker));
        assert!(le.campaigns_for(ProcessRole::Combined));
        assert!(!le.campaigns_for(ProcessRole::Web));
    }

    #[test]
    fn validate_checks_roles() {
        let mut c = leader("l");
        c.leader_election.roles = vec!["worker".into(), "cron".into()];
        let err = c.validate().unwrap_err();
        assert!(err.to_string().contains("cron"), "{err}");
    }

    #[test]
    fn env_sets_roles() {
        let c = KubernetesConfig::from_sources(
            None,
            None,
            None,
            env(&[(
                "AUTUMN_KUBERNETES__LEADER_ELECTION__ROLES",
                "worker,combined",
            )]),
        )
        .unwrap();
        assert_eq!(c.leader_election.roles, vec!["worker", "combined"]);
    }

    #[test]
    fn validate_checks_names() {
        assert!(leader("Bad_Name").validate().is_err());
        let mut c = KubernetesConfig::default();
        c.namespace = "a.b".to_owned();
        assert!(c.validate().unwrap_err().to_string().contains("namespace"));
        let mut c = KubernetesConfig::default();
        c.config_maps.watch = vec!["ok".to_owned(), "NO".to_owned()];
        assert!(c.validate().unwrap_err().to_string().contains("NO"));
        let mut c = KubernetesConfig::default();
        c.config_maps.watch = vec!["dup".to_owned(), "dup".to_owned()];
        assert!(c.validate().unwrap_err().to_string().contains("twice"));
    }

    #[test]
    fn dns_rules() {
        assert!(is_dns_label("shop-1"));
        assert!(!is_dns_label(""));
        assert!(!is_dns_label("-a"));
        assert!(!is_dns_label("a-"));
        assert!(!is_dns_label("a.b"));
        assert!(!is_dns_label(&"a".repeat(64)));
        assert!(is_dns_subdomain("a.b-c.d"));
        assert!(!is_dns_subdomain("a..b"));
        assert!(!is_dns_subdomain("A"));
        assert!(!is_dns_subdomain(&"a".repeat(254)));
        assert!(is_dns_subdomain(&"a".repeat(63)));
    }

    #[test]
    fn inline_profiles_merge_long_alias_first() {
        let base = r#"
            [profile.production.kubernetes]
            events = false
            namespace = "long"
            [profile.prod.kubernetes]
            namespace = "short"
        "#;
        let names = inline_profile_order(&["prod".to_owned(), "production".to_owned()]);
        assert_eq!(names, vec!["production", "prod"]);
        let c = KubernetesConfig::from_layers(Some(base), &names, None, env(&[])).unwrap();
        assert!(!c.events, "production applies");
        assert_eq!(c.namespace, "short", "prod wins");
    }

    #[test]
    fn profile_flag_forms() {
        let a = |v: &[&str]| v.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
        assert_eq!(
            profile_flag_in(&a(&["app", "--profile", "prod"])).as_deref(),
            Some("prod")
        );
        assert_eq!(
            profile_flag_in(&a(&["app", "--profile=dev"])).as_deref(),
            Some("dev")
        );
        assert_eq!(profile_flag_in(&a(&["app", "--profile="])), None);
        assert_eq!(profile_flag_in(&a(&["app", "--profile"])), None);
        assert_eq!(profile_flag_in(&a(&["app"])), None);
    }

    #[test]
    fn find_file_falls_back_to_the_working_directory() {
        let dir = std::env::temp_dir().join(format!("akp-find-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("autumn.toml"), "").unwrap();
        assert_eq!(find_file(&dir, "autumn.toml"), dir.join("autumn.toml"));
        assert_eq!(
            find_file(&dir, "autumn-prod.toml"),
            std::path::PathBuf::from("autumn-prod.toml")
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn dotenv_error_text_hides_the_line_but_not_io_errors() {
        let e = autumn_web::dotenv::DotenvError {
            path: "/app/.env".into(),
            line: 3,
            message: "bad line: SECRET=abc".into(),
        };
        let text = dotenv_error(&e).to_string();
        assert!(
            text.contains("/app/.env:3") && !text.contains("SECRET"),
            "{text}"
        );
        let io = autumn_web::dotenv::DotenvError {
            path: "/app/.env".into(),
            line: 0,
            message: "failed to read: Permission denied".into(),
        };
        assert!(dotenv_error(&io).to_string().contains("Permission denied"));
    }

    #[test]
    fn load_from_dir_reads_files() {
        let dir = std::env::temp_dir().join(format!("akp-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("autumn.toml"),
            "[kubernetes]\nnamespace = \"base\"",
        )
        .unwrap();
        std::fs::write(
            dir.join("autumn-production.toml"),
            "[kubernetes]\nrequired = true",
        )
        .unwrap();
        let names = vec!["prod".to_owned(), "production".to_owned()];
        let c = KubernetesConfig::load_from_dir(&dir, &names, env(&[])).unwrap();
        assert_eq!(c.namespace, "base");
        assert!(c.required);
        let none = KubernetesConfig::load_from_dir(&dir.join("missing"), &[], env(&[])).unwrap();
        assert_eq!(none, KubernetesConfig::default());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
