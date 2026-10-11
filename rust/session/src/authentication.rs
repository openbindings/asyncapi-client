//! Bounded per-session credentials and portable query placement. No I/O.
use crate::{ConnectionPlan, RuntimeCode, RuntimeError, SessionPlan};
use dynamic_asyncapi_client::HttpApiKeyLocation;
use serde::{
    Deserialize, Deserializer,
    de::{MapAccess, Visitor},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

/// Values keyed by the declared HTTP query parameter name, not a component alias.
/// No ambient lookup. At most 32 names, 256 bytes per name, 16 KiB per value,
/// and 64 KiB total raw UTF-8. Values are not serialized or printed by Debug.
#[derive(Clone, Default)]
pub struct QueryCredentials(BTreeMap<String, String>);
impl fmt::Debug for QueryCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("QueryCredentials([redacted])")
    }
}
fn invalid(detail: &'static str) -> RuntimeError {
    RuntimeError::new(RuntimeCode::InvalidConfiguration, detail)
}
impl QueryCredentials {
    pub fn new<K: Into<String>, V: Into<String>>(
        pairs: impl IntoIterator<Item = (K, V)>,
    ) -> Result<Self, RuntimeError> {
        let mut result = Self::default();
        for (name, value) in pairs {
            result.insert(name.into(), value.into())?;
        }
        Ok(result)
    }
    fn insert(&mut self, name: String, value: String) -> Result<(), RuntimeError> {
        if self.0.len() >= 32 || name.len() > 256 || value.len() > 16 * 1024 {
            return Err(invalid(
                "query credentials exceed the entry or field limits",
            ));
        }
        let existing: usize = self.0.iter().map(|(k, v)| k.len() + v.len()).sum();
        if existing + name.len() + value.len() > 64 * 1024 {
            return Err(invalid("query credentials exceed the aggregate byte limit"));
        }
        if self.0.contains_key(&name) {
            return Err(invalid("query credential name is duplicated"));
        }
        self.0.insert(name, value);
        Ok(())
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
impl<'de> Deserialize<'de> for QueryCredentials {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct CredentialsVisitor;
        impl<'de> Visitor<'de> for CredentialsVisitor {
            type Value = QueryCredentials;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a bounded query credential map")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut result = QueryCredentials::default();
                while let Some((name, value)) = map.next_entry::<String, String>()? {
                    result
                        .insert(name, value)
                        .map_err(serde::de::Error::custom)?;
                }
                Ok(result)
            }
        }
        deserializer
            .deserialize_map(CredentialsVisitor)
            .map_err(|_| {
                serde::de::Error::custom(
                    "query credentials have invalid structure, duplicate names or exceeded limits",
                )
            })
    }
}

/// Runtime-only endpoint containing credential material. Plan metadata is unchanged.
/// `as_str` is an explicit secret exposure for the concrete transport driver.
pub struct AuthenticatedEndpoint(String);
impl AuthenticatedEndpoint {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl fmt::Debug for AuthenticatedEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AuthenticatedEndpoint([redacted])")
    }
}
fn encode(text: &str, output: &mut String) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            output.push(byte as char);
        } else {
            output.push('%');
            output.push(HEX[(byte >> 4) as usize] as char);
            output.push(HEX[(byte & 15) as usize] as char);
        }
    }
}
impl SessionPlan {
    /// Resolve only declared query API keys. Missing and unused values refuse
    /// before I/O, so a shared credential dictionary cannot leak extra fields.
    /// Other mechanisms remain the concrete driver's responsibility.
    pub fn websocket_endpoint(
        &self,
        credentials: &QueryCredentials,
    ) -> Result<AuthenticatedEndpoint, RuntimeError> {
        let ConnectionPlan::WebSocket(endpoint) = self.connection() else {
            return Err(RuntimeError::new(
                RuntimeCode::Unsupported,
                "query authentication requires a WebSocket connection",
            ));
        };
        let mut names = BTreeSet::new();
        for scheme in self
            .plans()
            .iter()
            .flat_map(|p| p.describe().authentication.schemes())
        {
            if scheme.scheme_type != "httpApiKey" {
                continue;
            }
            let key = scheme
                .http_api_key
                .as_ref()
                .filter(|k| k.location == HttpApiKeyLocation::Query)
                .ok_or_else(|| {
                    RuntimeError::new(
                        RuntimeCode::Unsupported,
                        "HTTP API key placement is unsupported",
                    )
                })?;
            names.insert(key.name.as_str());
        }
        if names.iter().any(|name| !credentials.0.contains_key(*name)) {
            return Err(invalid(
                "declared query authentication requires a credential value",
            ));
        }
        if credentials
            .0
            .keys()
            .any(|name| !names.contains(name.as_str()))
        {
            return Err(invalid("query credential has no selected declaration"));
        }
        // Core plans currently exclude authored query strings and fragments.
        // Refuse if that changes; never append ambiguously or override a value.
        let uri = fluent_uri::Uri::parse(endpoint.as_str())
            .map_err(|_| invalid("invalid prepared endpoint"))?;
        if uri.query().is_some() || uri.fragment().is_some() {
            return Err(invalid(
                "query authentication cannot overwrite an authored query or fragment",
            ));
        }
        let mut result = endpoint.clone();
        for (index, name) in names.into_iter().enumerate() {
            result.push(if index == 0 { '?' } else { '&' });
            encode(name, &mut result);
            result.push('=');
            encode(&credentials.0[name], &mut result);
        }
        Ok(AuthenticatedEndpoint(result))
    }
}
