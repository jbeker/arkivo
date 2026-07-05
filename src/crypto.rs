use anyhow::{Context, Result, bail};
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use sha2::{Digest, Sha256};

const NONCE_LEN: usize = 24;

/// Seals and unseals per-account Fastmail tokens with XChaCha20-Poly1305.
/// Stored form is `nonce || ciphertext`; the key id travels alongside in
/// its own column so the master key can be rotated later.
#[derive(Clone)]
pub struct Sealer {
    cipher: XChaCha20Poly1305,
    key_id: String,
}

impl Sealer {
    /// `key` must be exactly 32 bytes; `key_id` identifies it for rotation.
    pub fn new(key: &[u8], key_id: impl Into<String>) -> Result<Self> {
        if key.len() != 32 {
            bail!("master key must be 32 bytes, got {}", key.len());
        }
        let key = Key::try_from(key).expect("length checked above");
        Ok(Self {
            cipher: XChaCha20Poly1305::new(&key),
            key_id: key_id.into(),
        })
    }

    /// Load the master key from a hex-encoded file (spec §13: the config
    /// holds a key *reference*, never the key itself).
    pub fn from_key_file(path: &std::path::Path, key_id: impl Into<String>) -> Result<Self> {
        let hex_key = std::fs::read_to_string(path)
            .with_context(|| format!("reading master key file {}", path.display()))?;
        let key = hex::decode(hex_key.trim()).context("master key file is not valid hex")?;
        Self::new(&key, key_id)
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub fn seal(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        let nonce: [u8; NONCE_LEN] = rand::random();
        let ciphertext = self
            .cipher
            .encrypt(&XNonce::from(nonce), plaintext)
            .map_err(|_| anyhow::anyhow!("sealing failed"))?;
        let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    pub fn unseal(&self, sealed: &[u8]) -> Result<Vec<u8>> {
        if sealed.len() < NONCE_LEN {
            bail!("sealed blob too short");
        }
        let (nonce, ciphertext) = sealed.split_at(NONCE_LEN);
        let nonce = XNonce::try_from(nonce).expect("length checked above");
        self.cipher
            .decrypt(&nonce, ciphertext)
            .map_err(|_| anyhow::anyhow!("unsealing failed: wrong key or tampered data"))
    }
}

/// Random 32-byte bearer token, presented base64url and stored only as a
/// SHA-256 hash. High-entropy input makes a KDF unnecessary.
pub struct GeneratedToken {
    /// The secret, shown to the user exactly once.
    pub token: String,
    /// SHA-256 of the secret, for storage.
    pub hash: Vec<u8>,
}

pub fn generate_token(prefix: &str) -> GeneratedToken {
    let raw: [u8; 32] = rand::random();
    let token = format!("{prefix}_{}", hex::encode(raw));
    let hash = hash_token(&token);
    GeneratedToken { token, hash }
}

pub fn hash_token(token: &str) -> Vec<u8> {
    Sha256::digest(token.as_bytes()).to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sealer(byte: u8) -> Sealer {
        Sealer::new(&[byte; 32], "k1").unwrap()
    }

    #[test]
    fn seal_unseal_round_trip() {
        let s = sealer(1);
        let sealed = s.seal(b"fmu1-secret-token").unwrap();
        assert_eq!(s.unseal(&sealed).unwrap(), b"fmu1-secret-token");
    }

    #[test]
    fn sealing_is_randomized() {
        let s = sealer(1);
        assert_ne!(s.seal(b"x").unwrap(), s.seal(b"x").unwrap());
    }

    #[test]
    fn unseal_rejects_tampered_ciphertext() {
        let s = sealer(1);
        let mut sealed = s.seal(b"secret").unwrap();
        let last = sealed.len() - 1;
        sealed[last] ^= 0x01;
        assert!(s.unseal(&sealed).is_err());
    }

    #[test]
    fn unseal_rejects_wrong_key() {
        let sealed = sealer(1).seal(b"secret").unwrap();
        assert!(sealer(2).unseal(&sealed).is_err());
    }

    #[test]
    fn rejects_bad_key_length() {
        assert!(Sealer::new(&[0u8; 16], "k1").is_err());
    }

    #[test]
    fn generated_tokens_hash_consistently() {
        let t = generate_token("mcp");
        assert!(t.token.starts_with("mcp_"));
        assert_eq!(hash_token(&t.token), t.hash);
        assert_ne!(generate_token("mcp").token, t.token);
    }
}
