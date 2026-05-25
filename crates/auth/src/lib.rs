//! Authentication, authorization, and RBAC for copperdb.
//!
//! Equivalent to Go's `pkg/auth` in NornicDB.
//! Provides JWT authentication, OAuth2 integration, RBAC roles/privileges,
//! allowlists, and per-database access control.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("invalid credentials")]
    InvalidCredentials,
    #[error("token expired")]
    TokenExpired,
    #[error("token invalid: {0}")]
    TokenInvalid(String),
    #[error("permission denied: {action} on {resource}")]
    PermissionDenied { action: String, resource: String },
    #[error("user not found: {0}")]
    UserNotFound(String),
    #[error("role not found: {0}")]
    RoleNotFound(String),
}

/// Database privilege flags (bitmask).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Privilege(pub u32);

impl Privilege {
    pub const READ: Privilege = Privilege(0b0001);
    pub const WRITE: Privilege = Privilege(0b0010);
    pub const CREATE_DB: Privilege = Privilege(0b0100);
    pub const DROP_DB: Privilege = Privilege(0b1000);
    pub const ADMIN: Privilege = Privilege(0b1111);

    pub fn has(self, other: Privilege) -> bool {
        (self.0 & other.0) == other.0
    }
}

/// A named role with associated privileges.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    pub name: String,
    pub privileges: Privilege,
    pub databases: Vec<String>,
}

/// Authenticated user session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub username: String,
    pub roles: Vec<Role>,
    pub metadata: std::collections::HashMap<String, String>,
}

impl User {
    /// Check if the user has a specific privilege on a database.
    pub fn has_privilege(&self, database: &str, privilege: Privilege) -> bool {
        self.roles.iter().any(|role| {
            (role.databases.contains(&database.to_string())
                || role.databases.contains(&"*".to_string()))
                && role.privileges.has(privilege)
        })
    }
}

/// JWT claims for copperdb tokens.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub exp: u64,
    pub iat: u64,
    pub roles: Vec<String>,
}

/// JWT token manager.
pub struct TokenManager {
    secret: String,
}

impl TokenManager {
    pub fn new(secret: impl Into<String>) -> Self {
        Self {
            secret: secret.into(),
        }
    }

    /// Issue a signed JWT token for a user.
    pub fn issue(
        &self,
        username: &str,
        roles: Vec<String>,
        expiry_secs: u64,
    ) -> Result<String, AuthError> {
        use jsonwebtoken::{encode, EncodingKey, Header};
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let claims = Claims {
            sub: username.to_owned(),
            exp: now + expiry_secs,
            iat: now,
            roles,
        };
        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(self.secret.as_bytes()),
        )
        .map_err(|e| AuthError::TokenInvalid(e.to_string()))
    }

    /// Validate and decode a JWT token.
    pub fn verify(&self, token: &str) -> Result<Claims, AuthError> {
        use jsonwebtoken::{decode, DecodingKey, Validation};
        let data = decode::<Claims>(
            token,
            &DecodingKey::from_secret(self.secret.as_bytes()),
            &Validation::default(),
        )
        .map_err(|e| match e.kind() {
            jsonwebtoken::errors::ErrorKind::ExpiredSignature => AuthError::TokenExpired,
            _ => AuthError::TokenInvalid(e.to_string()),
        })?;
        Ok(data.claims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_privilege_flags() {
        let admin = Privilege::ADMIN;
        assert!(admin.has(Privilege::READ));
        assert!(admin.has(Privilege::WRITE));
        assert!(!Privilege::READ.has(Privilege::WRITE));
    }

    #[test]
    fn test_jwt_issue_and_verify() {
        let mgr = TokenManager::new("test-secret");
        let token = mgr.issue("alice", vec!["reader".into()], 3600).unwrap();
        let claims = mgr.verify(&token).unwrap();
        assert_eq!(claims.sub, "alice");
        assert!(claims.roles.contains(&"reader".to_string()));
    }

    #[test]
    fn test_user_privilege_check() {
        let user = User {
            username: "alice".into(),
            roles: vec![Role {
                name: "reader".into(),
                privileges: Privilege::READ,
                databases: vec!["mydb".into()],
            }],
            metadata: Default::default(),
        };
        assert!(user.has_privilege("mydb", Privilege::READ));
        assert!(!user.has_privilege("mydb", Privilege::WRITE));
    }
}
