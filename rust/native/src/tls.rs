use crate::{RuntimeCode, RuntimeError};
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
use std::sync::Arc;

/// Reusable per-session trust and optional client identity. Server certificate
/// chains, validity and endpoint names are always verified. No insecure mode.
#[derive(Clone)]
pub struct TlsConfig {
    roots: Arc<RootCertStore>,
    client: Arc<ClientConfig>,
}
impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TlsConfig([redacted])")
    }
}
fn invalid(detail: &'static str) -> RuntimeError {
    RuntimeError::new(RuntimeCode::InvalidConfiguration, detail)
}
fn certificates(pem: &[u8], maximum: usize) -> Result<Vec<CertificateDer<'static>>, RuntimeError> {
    if pem.len() > 4 * 1024 * 1024 {
        return Err(invalid("TLS certificate material exceeds 4 MiB"));
    }
    let mut certificates = Vec::new();
    for entry in CertificateDer::pem_slice_iter(pem) {
        if certificates.len() == maximum {
            return Err(invalid(
                "TLS certificate count exceeds the configured profile",
            ));
        }
        certificates.push(entry.map_err(|_| invalid("TLS certificate PEM is invalid"))?);
    }
    if certificates.is_empty() {
        return Err(invalid("TLS certificate material contains no certificates"));
    }
    Ok(certificates)
}
impl TlsConfig {
    /// Loads platform roots now, including the native loader's SSL_CERT_FILE
    /// and SSL_CERT_DIR overrides. This is blocking setup work; reuse the result.
    /// A partial/failed root load refuses instead of silently changing trust.
    pub fn system_roots() -> Result<Self, RuntimeError> {
        let loaded = rustls_native_certs::load_native_certs();
        if !loaded.errors.is_empty() {
            return Err(invalid("system TLS roots could not be loaded completely"));
        }
        Self::from_certificates(loaded.certs)
    }
    /// Uses only this PEM CA bundle, without merging ambient system roots.
    /// Admission is bounded to 4 MiB and 512 certificates.
    pub fn from_ca_pem(pem: &[u8]) -> Result<Self, RuntimeError> {
        Self::from_certificates(certificates(pem, 512)?)
    }
    fn from_certificates(certificates: Vec<CertificateDer<'static>>) -> Result<Self, RuntimeError> {
        if certificates.is_empty() {
            return Err(invalid("TLS trust store is empty"));
        }
        let mut roots = RootCertStore::empty();
        for certificate in certificates {
            roots
                .add(certificate)
                .map_err(|_| invalid("TLS trust store contains an invalid certificate"))?;
        }
        let roots = Arc::new(roots);
        let client = Self::builder()?
            .with_root_certificates(roots.clone())
            .with_no_client_auth();
        Ok(Self {
            roots,
            client: Arc::new(client),
        })
    }
    /// Returns a new configuration with a PEM client chain (leaf first, at most
    /// 16 certificates) and exactly one unencrypted PEM private key (64 KiB max).
    /// Existing configurations and sessions retain their original identity.
    pub fn with_client_identity(
        &self,
        chain: &[u8],
        private_key: &[u8],
    ) -> Result<Self, RuntimeError> {
        let chain = certificates(chain, 16)?;
        if private_key.len() > 64 * 1024 {
            return Err(invalid("TLS private key material exceeds 64 KiB"));
        }
        let mut keys = PrivateKeyDer::pem_slice_iter(private_key);
        let key = keys
            .next()
            .ok_or_else(|| invalid("TLS private key is absent"))?
            .map_err(|_| invalid("TLS private key PEM is invalid"))?;
        if keys.next().is_some() {
            return Err(invalid(
                "TLS client identity requires exactly one private key",
            ));
        }
        let client = Self::builder()?
            .with_root_certificates(self.roots.clone())
            .with_client_auth_cert(chain, key)
            .map_err(|_| {
                invalid("TLS client certificate and private key are invalid or do not match")
            })?;
        Ok(Self {
            roots: self.roots.clone(),
            client: Arc::new(client),
        })
    }
    fn builder() -> Result<rustls::ConfigBuilder<ClientConfig, rustls::WantsVerifier>, RuntimeError>
    {
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|_| invalid("TLS protocol configuration is unavailable"))
    }
    pub(crate) fn config(&self) -> Arc<ClientConfig> {
        self.client.clone()
    }
}
