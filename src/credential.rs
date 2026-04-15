//! Configuration resolution for `LabKey` connections.
//!
//! Resolves connection parameters from a precedence chain:
//! 1. Named SQL parameters (`base_url`, `container_path`, `api_key`)
//! 2. Environment variables (`LABKEY_BASE_URL`, `LABKEY_CONTAINER`, `LABKEY_API_KEY`)
//!    with `LABKEY_CONTAINER_PATH` accepted as a legacy fallback
//! 3. `.netrc` file (looked up by hostname extracted from the resolved base URL)
//! 4. Guest (no credentials)

use std::{env, fmt};

use labkey_rs::Credential;
use url::Url;

/// Resolved configuration from the precedence chain.
pub(crate) struct ResolvedConfig {
    pub(crate) base_url: String,
    pub(crate) container_path: String,
    pub(crate) credential: Credential,
}

// Manual `Debug` impl that redacts the credential to avoid leaking API keys
// or passwords in log output.
impl fmt::Debug for ResolvedConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedConfig")
            .field("base_url", &self.base_url)
            .field("container_path", &self.container_path)
            .field("credential", &"<redacted>")
            .finish()
    }
}

/// Resolves `LabKey` connection configuration from the precedence chain.
///
/// `param_base_url`, `param_container_path`, and `param_api_key` are the values
/// extracted from SQL named parameters by the caller. This function is
/// independent of `duckdb` types so it can be unit-tested without a `BindInfo`.
///
/// # Errors
///
/// Returns an error if no base URL is available from either the parameter or
/// the `LABKEY_BASE_URL` environment variable.
pub(crate) fn resolve_config(
    param_base_url: Option<String>,
    param_container_path: Option<String>,
    param_api_key: Option<String>,
) -> Result<ResolvedConfig, Box<dyn std::error::Error>> {
    // --- base_url: param → env → error ---
    let base_url = param_base_url
        .or_else(|| env::var("LABKEY_BASE_URL").ok())
        .ok_or(
            "No LabKey base URL configured. \
             Set the base_url parameter, or the LABKEY_BASE_URL environment variable.",
        )?;

    // --- container_path: param → env → "/" ---
    let container_path = param_container_path
        .or_else(|| env::var("LABKEY_CONTAINER").ok())
        .or_else(|| env::var("LABKEY_CONTAINER_PATH").ok())
        .unwrap_or_else(|| "/".to_owned());

    // --- credential: param api_key → env api_key → .netrc → Guest ---
    let credential = param_api_key
        .or_else(|| env::var("LABKEY_API_KEY").ok())
        .map_or_else(
            || {
                Url::parse(&base_url)
                    .ok()
                    .and_then(|u| u.host_str().map(str::to_owned))
                    .and_then(|host| Credential::from_netrc(&host).ok())
                    .unwrap_or(Credential::Guest)
            },
            Credential::ApiKey,
        );

    Ok(ResolvedConfig {
        base_url,
        container_path,
        credential,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Env-var tests must run serially because they mutate process-global state.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// RAII guard that restores env vars when dropped, even on panic.
    struct EnvRestore<'a>(Vec<(&'a str, Option<String>)>);

    impl Drop for EnvRestore<'_> {
        fn drop(&mut self) {
            for (k, v) in &self.0 {
                match v {
                    Some(val) => env::set_var(k, val),
                    None => env::remove_var(k),
                }
            }
        }
    }

    /// Helper: run a closure with specific env vars set, restoring originals
    /// afterward regardless of panics.
    fn with_env<F: FnOnce()>(vars: &[(&str, Option<&str>)], f: F) {
        const MANAGED_VARS: [&str; 4] = [
            "LABKEY_BASE_URL",
            "LABKEY_API_KEY",
            "LABKEY_CONTAINER",
            "LABKEY_CONTAINER_PATH",
        ];
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let mut managed_keys: Vec<&str> = vars.iter().map(|(k, _)| *k).collect();
        for key in MANAGED_VARS {
            if !managed_keys.contains(&key) {
                managed_keys.push(key);
            }
        }

        let originals: Vec<(&str, Option<String>)> = managed_keys
            .iter()
            .map(|k| (*k, env::var(k).ok()))
            .collect();

        for key in &managed_keys {
            env::remove_var(key);
        }

        for (k, v) in vars {
            match v {
                Some(val) => env::set_var(k, val),
                None => env::remove_var(k),
            }
        }

        let _restore = EnvRestore(originals);

        f();
    }

    // -----------------------------------------------------------------------
    // base_url precedence
    // -----------------------------------------------------------------------

    #[test]
    fn base_url_from_param() {
        with_env(
            &[
                ("LABKEY_BASE_URL", Some("https://env.example.com")),
                ("LABKEY_API_KEY", None),
                ("LABKEY_CONTAINER_PATH", None),
            ],
            || {
                let cfg = resolve_config(
                    Some("https://param.example.com".into()),
                    None,
                    Some("key".into()),
                )
                .expect("should succeed");
                assert_eq!(cfg.base_url, "https://param.example.com");
            },
        );
    }

    #[test]
    fn base_url_from_env() {
        with_env(
            &[
                ("LABKEY_BASE_URL", Some("https://env.example.com")),
                ("LABKEY_API_KEY", Some("envkey")),
                ("LABKEY_CONTAINER_PATH", None),
            ],
            || {
                let cfg = resolve_config(None, None, None).expect("should succeed");
                assert_eq!(cfg.base_url, "https://env.example.com");
            },
        );
    }

    #[test]
    fn base_url_missing_is_error() {
        with_env(
            &[
                ("LABKEY_BASE_URL", None),
                ("LABKEY_API_KEY", None),
                ("LABKEY_CONTAINER_PATH", None),
            ],
            || {
                let err = resolve_config(None, None, None).unwrap_err();
                let msg = err.to_string();
                assert!(
                    msg.contains("No LabKey base URL configured"),
                    "unexpected error message: {msg}"
                );
            },
        );
    }

    // -----------------------------------------------------------------------
    // container_path precedence
    // -----------------------------------------------------------------------

    #[test]
    fn container_path_from_param() {
        with_env(
            &[
                ("LABKEY_BASE_URL", None),
                ("LABKEY_API_KEY", None),
                ("LABKEY_CONTAINER_PATH", Some("/EnvProject")),
            ],
            || {
                let cfg = resolve_config(
                    Some("https://lk.example.com".into()),
                    Some("/ParamProject".into()),
                    Some("key".into()),
                )
                .expect("should succeed");
                assert_eq!(cfg.container_path, "/ParamProject");
            },
        );
    }

    #[test]
    fn container_path_from_env() {
        with_env(
            &[
                ("LABKEY_BASE_URL", None),
                ("LABKEY_API_KEY", None),
                ("LABKEY_CONTAINER", Some("/EnvProject")),
            ],
            || {
                let cfg = resolve_config(
                    Some("https://lk.example.com".into()),
                    None,
                    Some("key".into()),
                )
                .expect("should succeed");
                assert_eq!(cfg.container_path, "/EnvProject");
            },
        );
    }

    #[test]
    fn container_path_prefers_labkey_container_over_legacy_name() {
        with_env(
            &[
                ("LABKEY_BASE_URL", None),
                ("LABKEY_API_KEY", None),
                ("LABKEY_CONTAINER", Some("/PreferredProject")),
                ("LABKEY_CONTAINER_PATH", Some("/LegacyProject")),
            ],
            || {
                let cfg = resolve_config(
                    Some("https://lk.example.com".into()),
                    None,
                    Some("key".into()),
                )
                .expect("should succeed");
                assert_eq!(cfg.container_path, "/PreferredProject");
            },
        );
    }

    #[test]
    fn container_path_defaults_to_root() {
        with_env(
            &[
                ("LABKEY_BASE_URL", None),
                ("LABKEY_API_KEY", None),
                ("LABKEY_CONTAINER_PATH", None),
            ],
            || {
                let cfg = resolve_config(
                    Some("https://lk.example.com".into()),
                    None,
                    Some("key".into()),
                )
                .expect("should succeed");
                assert_eq!(cfg.container_path, "/");
            },
        );
    }

    // -----------------------------------------------------------------------
    // credential precedence
    // -----------------------------------------------------------------------

    #[test]
    fn credential_api_key_from_param() {
        with_env(
            &[
                ("LABKEY_BASE_URL", None),
                ("LABKEY_API_KEY", Some("env_key")),
                ("LABKEY_CONTAINER_PATH", None),
            ],
            || {
                let cfg = resolve_config(
                    Some("https://lk.example.com".into()),
                    None,
                    Some("param_key".into()),
                )
                .expect("should succeed");
                assert!(
                    matches!(&cfg.credential, Credential::ApiKey(k) if k == "param_key"),
                    "expected ApiKey(\"param_key\"), got {:?}",
                    cfg.credential
                );
            },
        );
    }

    #[test]
    fn credential_api_key_from_env() {
        with_env(
            &[
                ("LABKEY_BASE_URL", None),
                ("LABKEY_API_KEY", Some("env_key")),
                ("LABKEY_CONTAINER_PATH", None),
            ],
            || {
                let cfg = resolve_config(Some("https://lk.example.com".into()), None, None)
                    .expect("should succeed");
                assert!(
                    matches!(&cfg.credential, Credential::ApiKey(k) if k == "env_key"),
                    "expected ApiKey(\"env_key\"), got {:?}",
                    cfg.credential
                );
            },
        );
    }

    #[test]
    fn credential_falls_back_to_guest_when_no_netrc() {
        with_env(
            &[
                ("LABKEY_BASE_URL", None),
                ("LABKEY_API_KEY", None),
                ("LABKEY_CONTAINER_PATH", None),
            ],
            || {
                // No param api_key, no env api_key, and .netrc won't have an
                // entry for this host.
                let cfg = resolve_config(
                    Some("https://no-netrc-entry.example.com".into()),
                    None,
                    None,
                )
                .expect("should succeed");
                assert!(
                    matches!(cfg.credential, Credential::Guest),
                    "expected Guest, got {:?}",
                    cfg.credential
                );
            },
        );
    }

    #[test]
    fn credential_guest_on_url_without_host() {
        with_env(
            &[
                ("LABKEY_BASE_URL", None),
                ("LABKEY_API_KEY", None),
                ("LABKEY_CONTAINER_PATH", None),
            ],
            || {
                // A valid URL that has no host component exercises the
                // `host_str() => None` branch (distinct from parse failure).
                let cfg = resolve_config(Some("data:text/plain,hello".into()), None, None)
                    .expect("should succeed with Guest credential");
                assert!(
                    matches!(cfg.credential, Credential::Guest),
                    "expected Guest for host-less URL, got {:?}",
                    cfg.credential
                );
            },
        );
    }

    #[test]
    fn credential_guest_on_unparseable_url() {
        with_env(
            &[
                ("LABKEY_BASE_URL", None),
                ("LABKEY_API_KEY", None),
                ("LABKEY_CONTAINER_PATH", None),
            ],
            || {
                // A base_url that isn't a valid URL should still resolve
                // (Guest fallback), not error. The URL parsing failure only
                // affects credential resolution, not base_url validation.
                let cfg = resolve_config(Some("not-a-url".into()), None, None)
                    .expect("should succeed with Guest credential");
                assert!(
                    matches!(cfg.credential, Credential::Guest),
                    "expected Guest, got {:?}",
                    cfg.credential
                );
            },
        );
    }

    // -----------------------------------------------------------------------
    // combined / edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn all_from_params() {
        with_env(
            &[
                ("LABKEY_BASE_URL", None),
                ("LABKEY_API_KEY", None),
                ("LABKEY_CONTAINER_PATH", None),
            ],
            || {
                let cfg = resolve_config(
                    Some("https://lk.example.com".into()),
                    Some("/MyProject".into()),
                    Some("my_key".into()),
                )
                .expect("should succeed");
                assert_eq!(cfg.base_url, "https://lk.example.com");
                assert_eq!(cfg.container_path, "/MyProject");
                assert!(matches!(
                    &cfg.credential,
                    Credential::ApiKey(k) if k == "my_key"
                ));
            },
        );
    }

    #[test]
    fn all_from_env() {
        with_env(
            &[
                ("LABKEY_BASE_URL", Some("https://env.example.com")),
                ("LABKEY_API_KEY", Some("env_key")),
                ("LABKEY_CONTAINER_PATH", Some("/EnvProject")),
            ],
            || {
                let cfg = resolve_config(None, None, None).expect("should succeed");
                assert_eq!(cfg.base_url, "https://env.example.com");
                assert_eq!(cfg.container_path, "/EnvProject");
                assert!(matches!(
                    &cfg.credential,
                    Credential::ApiKey(k) if k == "env_key"
                ));
            },
        );
    }

    #[test]
    fn params_override_env() {
        with_env(
            &[
                ("LABKEY_BASE_URL", Some("https://env.example.com")),
                ("LABKEY_API_KEY", Some("env_key")),
                ("LABKEY_CONTAINER_PATH", Some("/EnvProject")),
            ],
            || {
                let cfg = resolve_config(
                    Some("https://param.example.com".into()),
                    Some("/ParamProject".into()),
                    Some("param_key".into()),
                )
                .expect("should succeed");
                assert_eq!(cfg.base_url, "https://param.example.com");
                assert_eq!(cfg.container_path, "/ParamProject");
                assert!(matches!(
                    &cfg.credential,
                    Credential::ApiKey(k) if k == "param_key"
                ));
            },
        );
    }

    #[test]
    fn base_url_from_env_rest_from_params() {
        with_env(
            &[
                ("LABKEY_BASE_URL", Some("https://env.example.com")),
                ("LABKEY_API_KEY", None),
                ("LABKEY_CONTAINER_PATH", None),
            ],
            || {
                let cfg =
                    resolve_config(None, Some("/ParamProject".into()), Some("param_key".into()))
                        .expect("should succeed");
                assert_eq!(cfg.base_url, "https://env.example.com");
                assert_eq!(cfg.container_path, "/ParamProject");
                assert!(matches!(
                    &cfg.credential,
                    Credential::ApiKey(k) if k == "param_key"
                ));
            },
        );
    }

    #[test]
    fn empty_string_param_is_treated_as_some() {
        // Empty strings are valid Option<String> values. They are not None.
        // An empty api_key param means the user explicitly set it to "".
        with_env(
            &[
                ("LABKEY_BASE_URL", None),
                ("LABKEY_API_KEY", Some("env_key")),
                ("LABKEY_CONTAINER_PATH", None),
            ],
            || {
                let cfg = resolve_config(
                    Some("https://lk.example.com".into()),
                    None,
                    Some(String::new()),
                )
                .expect("should succeed");
                // Empty string is still ApiKey, param takes precedence over env
                assert!(
                    matches!(&cfg.credential, Credential::ApiKey(k) if k.is_empty()),
                    "expected ApiKey(\"\"), got {:?}",
                    cfg.credential
                );
            },
        );
    }

    #[test]
    fn empty_env_api_key_takes_precedence_over_netrc() {
        // An empty LABKEY_API_KEY env var is NOT treated as unset.
        // It produces ApiKey(""), skipping .netrc lookup entirely.
        with_env(
            &[
                ("LABKEY_BASE_URL", None),
                ("LABKEY_API_KEY", Some("")),
                ("LABKEY_CONTAINER_PATH", None),
            ],
            || {
                let cfg = resolve_config(Some("https://lk.example.com".into()), None, None)
                    .expect("should succeed");
                assert!(
                    matches!(&cfg.credential, Credential::ApiKey(k) if k.is_empty()),
                    "expected ApiKey(\"\") from empty env var, got {:?}",
                    cfg.credential
                );
            },
        );
    }

    #[test]
    fn empty_base_url_param_is_not_treated_as_missing() {
        // An empty string base_url param is Some, not None. The function
        // succeeds with base_url = "". Downstream consumers will fail when
        // they try to use this as a URL, but resolve_config itself does not
        // validate URL format — only presence.
        with_env(
            &[
                ("LABKEY_BASE_URL", Some("https://env.example.com")),
                ("LABKEY_API_KEY", None),
                ("LABKEY_CONTAINER_PATH", None),
            ],
            || {
                let cfg = resolve_config(Some(String::new()), None, Some("key".into()))
                    .expect("empty string param is Some, not None");
                // Empty string wins over env — intentional: resolve_config
                // checks presence, not validity.
                assert_eq!(cfg.base_url, "");
            },
        );
    }
}
