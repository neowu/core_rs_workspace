use std::sync::Arc;

use rustls_platform_verifier::BuilderVerifierExt as _;
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::rustls::DigitallySignedStruct;
use tokio_rustls::rustls::Error as TlsError;
use tokio_rustls::rustls::RootCertStore;
use tokio_rustls::rustls::SignatureScheme;
use tokio_rustls::rustls::client::danger::HandshakeSignatureValid;
use tokio_rustls::rustls::client::danger::ServerCertVerified;
use tokio_rustls::rustls::client::danger::ServerCertVerifier;
use tokio_rustls::rustls::crypto::CryptoProvider;
use tokio_rustls::rustls::crypto::aws_lc_rs;
use tokio_rustls::rustls::pki_types::CertificateDer;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::pki_types::UnixTime;

use super::HttpClientConfig;

pub(super) fn tls_config(config: &HttpClientConfig) -> ClientConfig {
    let provider = Arc::new(aws_lc_rs::default_provider());
    let builder = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .expect("default tls versions must be supported");
    let mut tls = if config.accept_invalid_cert {
        builder.dangerous().with_custom_certificate_verifier(Arc::new(AcceptAnyCert(provider))).with_no_client_auth()
    } else if let Some(certs) = &config.accept_certs {
        let mut roots = RootCertStore::empty();
        roots.add_parsable_certificates(certs.iter().cloned());
        builder.with_root_certificates(roots).with_no_client_auth()
    } else {
        builder.with_platform_verifier().expect("failed to load platform cert verifier").with_no_client_auth()
    };
    tls.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    tls
}

#[derive(Debug)]
struct AcceptAnyCert(Arc<CryptoProvider>);

impl ServerCertVerifier for AcceptAnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
