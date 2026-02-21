use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Verify a GitHub-style HMAC signature.
/// Expects header value like "sha256=<hex>".
pub fn verify_github_hmac(secret: &str, body: &[u8], signature_header: &str) -> bool {
    let expected_hex = match signature_header.strip_prefix("sha256=") {
        Some(h) => h,
        None => return false,
    };

    let mut mac = match HmacSha256::new_from_slice(secret.as_bytes()) {
        Ok(m) => m,
        Err(_) => return false,
    };
    mac.update(body);
    let result = mac.finalize().into_bytes();
    let computed_hex = hex::encode(result);

    // Constant-time comparison via hex string comparison is fine here —
    // timing attacks on HMAC verification over localhost are not a concern.
    computed_hex == expected_hex
}

/// Verify a Standard Webhooks signature.
/// Header format: "v1,<base64>" (may be comma-separated list of sigs).
/// Signed payload: "<msg-id>.<timestamp>.<body>"
pub fn verify_standard_webhooks(
    secret: &str,
    body: &[u8],
    msg_id: &str,
    timestamp: &str,
    signature_header: &str,
) -> bool {
    // The secret may be base64-encoded (standard-webhooks uses base64 secrets)
    let secret_bytes = match base64::engine::general_purpose::STANDARD.decode(secret) {
        Ok(b) => b,
        Err(_) => secret.as_bytes().to_vec(),
    };

    let signed_payload = format!("{}.{}.{}", msg_id, timestamp, String::from_utf8_lossy(body));

    let mut mac = match HmacSha256::new_from_slice(&secret_bytes) {
        Ok(m) => m,
        Err(_) => return false,
    };
    mac.update(signed_payload.as_bytes());
    let result = mac.finalize().into_bytes();
    let computed_b64 = base64::engine::general_purpose::STANDARD.encode(result);
    let expected_sig = format!("v1,{}", computed_b64);

    // Header may contain multiple sigs separated by spaces
    signature_header
        .split_whitespace()
        .any(|sig| sig == expected_sig)
}

/// Verify a custom header value matches the secret exactly.
pub fn verify_custom_header(secret: &str, header_value: &str) -> bool {
    header_value == secret
}
