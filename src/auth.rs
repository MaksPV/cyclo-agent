use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use rand::RngCore;
use sha2::{Digest, Sha256};

pub fn hash_password(password: &str) -> Result<String, String> {
    let salt = SaltString::generate(&mut rand::thread_rng());
    argon2::Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| e.to_string())
}

pub fn verify_password(password: &str, hash: &str) -> bool {
    let Ok(parsed) = PasswordHash::new(hash) else {
        return false;
    };
    argon2::Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

/// Raw session token (hex) + its sha256 for DB storage.
pub fn new_session_token() -> (String, String) {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    let raw = hex::encode(bytes);
    let digest = Sha256::digest(raw.as_bytes());
    (raw, hex::encode(digest))
}

pub fn hash_token(raw: &str) -> String {
    hex::encode(Sha256::digest(raw.as_bytes()))
}

pub fn valid_login(login: &str) -> bool {
    (3..=32).contains(&login.len())
        && login
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
}
