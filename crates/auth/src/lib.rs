//! Persistent authentication, authorization, and RBAC for copperDB.
//!
//! This crate owns the security-layer identity model. Users, custom roles,
//! database allowlists, per-database privileges, and role entitlements are stored
//! as system `NodeRecord`s in `copperdb-storage`; in-memory maps are only caches
//! rebuilt from durable state.

use copperdb_storage::{NodeRecord, StorageEngine, StorageError};
use getrandom::fill as fill_random;
use hmac::{Hmac, KeyInit, Mac};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use uuid::Uuid;

const USER_LABEL: &str = "_User";
const ROLE_LABEL: &str = "_Role";
const ROLE_DB_ACCESS_LABEL: &str = "_RoleDbAccess";
const DB_PRIVILEGE_LABEL: &str = "_DbPrivilege";
const ROLE_ENTITLEMENT_LABEL: &str = "_RoleEntitlement";
const SYSTEM_LABEL: &str = "_System";
const USER_PREFIX: &str = "auth:user:";
const ROLE_PREFIX: &str = "auth:role:";
const ROLE_DB_ACCESS_PREFIX: &str = "auth:role_db_access:";
const DB_PRIVILEGE_PREFIX: &str = "auth:db_priv:";
const ROLE_ENTITLEMENT_PREFIX: &str = "auth:role_entitlement:";
const PASSWORD_HASH_VERSION: &str = "copperdb-pbkdf2-sha256-v1";
const PASSWORD_ITERATIONS: u32 = 120_000;
const DEFAULT_TOKEN_CACHE_ENTRIES: usize = 1024;
const DEFAULT_TOKEN_CACHE_TTL: Duration = Duration::from_secs(30);

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
    #[error("user already exists: {0}")]
    UserExists(String),
    #[error("role not found: {0}")]
    RoleNotFound(String),
    #[error("role already exists: {0}")]
    RoleExists(String),
    #[error("cannot delete or rename built-in role: {0}")]
    BuiltinRole(String),
    #[error("invalid role name: {0}")]
    InvalidRoleName(String),
    #[error("account locked")]
    AccountLocked,
    #[error("password does not meet minimum length requirement")]
    PasswordTooShort,
    #[error("user disabled")]
    UserDisabled,
    #[error("JWT secret not configured")]
    MissingSecret,
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Privilege(pub u32);

impl Privilege {
    pub const READ: Privilege = Privilege(0b000001);
    pub const WRITE: Privilege = Privilege(0b000010);
    pub const CREATE: Privilege = Privilege(0b000100);
    pub const DELETE: Privilege = Privilege(0b001000);
    pub const SCHEMA: Privilege = Privilege(0b010000);
    pub const USER_MANAGE: Privilege = Privilege(0b100000);
    pub const CREATE_DB: Privilege = Privilege::CREATE;
    pub const DROP_DB: Privilege = Privilege::DELETE;
    pub const ADMIN: Privilege = Privilege(
        Self::READ.0
            | Self::WRITE.0
            | Self::CREATE.0
            | Self::DELETE.0
            | Self::SCHEMA.0
            | Self::USER_MANAGE.0,
    );

    pub fn has(self, other: Privilege) -> bool {
        (self.0 & other.0) == other.0
    }

    pub fn union(self, other: Privilege) -> Privilege {
        Privilege(self.0 | other.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    Read,
    Write,
    Create,
    Delete,
    Admin,
    Schema,
    UserManage,
}

impl Permission {
    pub fn id(self) -> &'static str {
        match self {
            Permission::Read => "read",
            Permission::Write => "write",
            Permission::Create => "create",
            Permission::Delete => "delete",
            Permission::Admin => "admin",
            Permission::Schema => "schema",
            Permission::UserManage => "user_manage",
        }
    }

    pub fn from_id(id: &str) -> Option<Self> {
        match normalize_name(id).as_str() {
            "read" => Some(Self::Read),
            "write" => Some(Self::Write),
            "create" => Some(Self::Create),
            "delete" => Some(Self::Delete),
            "admin" => Some(Self::Admin),
            "schema" => Some(Self::Schema),
            "user_manage" => Some(Self::UserManage),
            _ => None,
        }
    }

    pub fn privilege(self) -> Privilege {
        match self {
            Permission::Read => Privilege::READ,
            Permission::Write => Privilege::WRITE,
            Permission::Create => Privilege::CREATE,
            Permission::Delete => Privilege::DELETE,
            Permission::Admin => Privilege::ADMIN,
            Permission::Schema => Privilege::SCHEMA,
            Permission::UserManage => Privilege::USER_MANAGE,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Role {
    pub name: String,
    pub privileges: Privilege,
    pub databases: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct User {
    pub id: String,
    pub username: String,
    pub email: Option<String>,
    pub roles: Vec<Role>,
    pub metadata: HashMap<String, String>,
    pub disabled: bool,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    pub last_login_unix_ms: Option<i64>,
}

impl User {
    pub fn has_role(&self, role: &str) -> bool {
        let role = normalize_name(role);
        self.roles
            .iter()
            .any(|candidate| normalize_name(&candidate.name) == role)
    }

    pub fn has_permission(&self, permission: Permission) -> bool {
        self.roles
            .iter()
            .any(|role| role.privileges.has(permission.privilege()))
    }

    pub fn has_privilege(&self, database: &str, privilege: Privilege) -> bool {
        self.roles.iter().any(|role| {
            (role.databases.is_empty()
                || role.databases.iter().any(|db| db == "*" || db == database))
                && role.privileges.has(privilege)
        })
    }

    pub fn role_names(&self) -> Vec<String> {
        self.roles.iter().map(|role| role.name.clone()).collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredUser {
    id: String,
    username: String,
    email: Option<String>,
    password_hash: String,
    roles: Vec<String>,
    metadata: HashMap<String, String>,
    disabled: bool,
    failed_logins: u32,
    locked_until_unix_ms: Option<i64>,
    created_at_unix_ms: i64,
    updated_at_unix_ms: i64,
    last_login_unix_ms: Option<i64>,
}

impl StoredUser {
    fn public_user(&self, role_resolver: &RoleResolver<'_>) -> User {
        User {
            id: self.id.clone(),
            username: self.username.clone(),
            email: self.email.clone(),
            roles: self
                .roles
                .iter()
                .map(|role| role_resolver.role(role))
                .collect(),
            metadata: self.metadata.clone(),
            disabled: self.disabled,
            created_at_unix_ms: self.created_at_unix_ms,
            updated_at_unix_ms: self.updated_at_unix_ms,
            last_login_unix_ms: self.last_login_unix_ms,
        }
    }
}

struct RoleResolver<'a> {
    entitlements: &'a RoleEntitlementsStore,
    allowlist: &'a AllowlistStore,
}

impl RoleResolver<'_> {
    fn role(&self, name: &str) -> Role {
        let name = normalize_name(name);
        Role {
            privileges: privilege_for_permissions(&self.entitlements.permissions_for_role(&name)),
            databases: self.allowlist.databases_for_role(&name).unwrap_or_default(),
            name,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub exp: u64,
    pub iat: u64,
    pub roles: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: Option<u64>,
    pub scope: Option<String>,
}

pub struct TokenManager {
    secret: String,
    cache: Mutex<TokenCache>,
}

impl TokenManager {
    pub fn new(secret: impl Into<String>) -> Self {
        Self {
            secret: secret.into(),
            cache: Mutex::new(TokenCache::new(
                DEFAULT_TOKEN_CACHE_ENTRIES,
                DEFAULT_TOKEN_CACHE_TTL,
            )),
        }
    }

    pub fn issue(
        &self,
        username: &str,
        roles: Vec<String>,
        expiry_secs: u64,
    ) -> Result<String, AuthError> {
        let now = now_secs();
        let claims = Claims {
            sub: username.to_owned(),
            exp: if expiry_secs == 0 {
                0
            } else {
                now + expiry_secs
            },
            iat: now,
            roles,
        };
        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(self.secret.as_bytes()),
        )
        .map_err(|err| AuthError::TokenInvalid(err.to_string()))
    }

    pub fn verify(&self, token: &str) -> Result<Claims, AuthError> {
        if let Some(claims) = self.cache.lock().get(token) {
            return Ok(claims);
        }
        let mut validation = Validation::default();
        validation.validate_exp = true;
        let data = decode::<Claims>(
            token,
            &DecodingKey::from_secret(self.secret.as_bytes()),
            &validation,
        )
        .map_err(|err| match err.kind() {
            jsonwebtoken::errors::ErrorKind::ExpiredSignature => AuthError::TokenExpired,
            _ => AuthError::TokenInvalid(err.to_string()),
        })?;
        if data.claims.exp != 0 {
            self.cache.lock().set(token, &data.claims);
        }
        Ok(data.claims)
    }
}

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub min_password_length: usize,
    pub jwt_secret: Vec<u8>,
    pub token_expiry: Option<Duration>,
    pub max_failed_logins: u32,
    pub lockout_duration: Duration,
    pub default_admin_username: String,
    pub security_enabled: bool,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            min_password_length: 8,
            jwt_secret: Vec::new(),
            token_expiry: None,
            max_failed_logins: 5,
            lockout_duration: Duration::from_secs(15 * 60),
            default_admin_username: "admin".into(),
            security_enabled: true,
        }
    }
}

pub struct Authenticator {
    storage: Arc<StorageEngine>,
    config: AuthConfig,
    token_manager: TokenManager,
    users: RwLock<HashMap<String, StoredUser>>,
    pub roles: RoleStore,
    pub allowlist: AllowlistStore,
    pub privileges: PrivilegesStore,
    pub entitlements: RoleEntitlementsStore,
}

impl Authenticator {
    pub fn new(config: AuthConfig, storage: Arc<StorageEngine>) -> Result<Self, AuthError> {
        if config.security_enabled && config.jwt_secret.is_empty() {
            return Err(AuthError::MissingSecret);
        }
        let jwt_secret = String::from_utf8_lossy(&config.jwt_secret).to_string();
        let roles = RoleStore::new(Arc::clone(&storage));
        let allowlist = AllowlistStore::new(Arc::clone(&storage));
        let privileges = PrivilegesStore::new(Arc::clone(&storage));
        let entitlements = RoleEntitlementsStore::new(Arc::clone(&storage));
        let auth = Self {
            storage,
            config,
            token_manager: TokenManager::new(jwt_secret),
            users: RwLock::new(HashMap::new()),
            roles,
            allowlist,
            privileges,
            entitlements,
        };
        auth.reload()?;
        Ok(auth)
    }

    pub fn reload(&self) -> Result<(), AuthError> {
        self.roles.load()?;
        self.allowlist.load()?;
        self.privileges.load()?;
        self.entitlements.load()?;
        let mut users = HashMap::new();
        for node in self.storage.get_nodes_by_label(USER_LABEL)? {
            if let Some(user) = stored_user_from_node(&node)? {
                users.insert(normalize_name(&user.username), user);
            }
        }
        *self.users.write() = users;
        Ok(())
    }

    pub fn seed_builtin_access_if_empty(&self) -> Result<(), AuthError> {
        self.allowlist.seed_builtin_if_empty()?;
        Ok(())
    }

    pub fn create_user(
        &self,
        username: &str,
        password: &str,
        roles: Vec<String>,
    ) -> Result<User, AuthError> {
        let username = normalize_username(username)?;
        if password.len() < self.config.min_password_length {
            return Err(AuthError::PasswordTooShort);
        }
        if self.users.read().contains_key(&username) {
            return Err(AuthError::UserExists(username));
        }
        let role_names = if roles.is_empty() {
            vec![ROLE_VIEWER.into()]
        } else {
            roles
                .into_iter()
                .map(|role| normalize_name(&role))
                .collect()
        };
        for role in &role_names {
            if !self.roles.exists(role) {
                return Err(AuthError::RoleNotFound(role.clone()));
            }
        }
        let now = now_unix_ms();
        let user = StoredUser {
            id: Uuid::new_v4().to_string(),
            email: Some(format!("{username}@localhost")),
            username: username.clone(),
            password_hash: hash_password(password)?,
            roles: role_names,
            metadata: HashMap::new(),
            disabled: false,
            failed_logins: 0,
            locked_until_unix_ms: None,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
            last_login_unix_ms: None,
        };
        self.persist_user(&user)?;
        self.users.write().insert(username, user.clone());
        Ok(user.public_user(&self.role_resolver()))
    }

    pub fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> Result<(TokenResponse, User), AuthError> {
        if !self.config.security_enabled {
            let user = User {
                id: username.to_string(),
                username: username.to_string(),
                email: None,
                roles: vec![self.role_resolver().role(ROLE_ADMIN)],
                metadata: HashMap::new(),
                disabled: false,
                created_at_unix_ms: now_unix_ms(),
                updated_at_unix_ms: now_unix_ms(),
                last_login_unix_ms: None,
            };
            let token = self.issue_user_token(&user)?;
            return Ok((token, user));
        }

        let key = normalize_name(username);
        let mut user = self
            .users
            .read()
            .get(&key)
            .cloned()
            .ok_or(AuthError::InvalidCredentials)?;
        if user.disabled {
            return Err(AuthError::UserDisabled);
        }
        let now = now_unix_ms();
        if user
            .locked_until_unix_ms
            .map(|until| now < until)
            .unwrap_or(false)
        {
            return Err(AuthError::AccountLocked);
        }
        if !verify_password(password, &user.password_hash) {
            user.failed_logins = user.failed_logins.saturating_add(1);
            if user.failed_logins >= self.config.max_failed_logins
                && user.username != self.config.default_admin_username
            {
                user.locked_until_unix_ms =
                    Some(now + self.config.lockout_duration.as_millis() as i64);
            }
            user.updated_at_unix_ms = now;
            self.persist_user(&user)?;
            self.users.write().insert(key, user);
            return Err(AuthError::InvalidCredentials);
        }

        user.failed_logins = 0;
        user.locked_until_unix_ms = None;
        user.last_login_unix_ms = Some(now);
        user.updated_at_unix_ms = now;
        self.persist_user(&user)?;
        self.users.write().insert(key, user.clone());
        let public = user.public_user(&self.role_resolver());
        let token = self.issue_user_token(&public)?;
        Ok((token, public))
    }

    pub fn get_user(&self, username: &str) -> Result<User, AuthError> {
        let key = normalize_name(username);
        self.users
            .read()
            .get(&key)
            .map(|user| user.public_user(&self.role_resolver()))
            .ok_or(AuthError::UserNotFound(key))
    }

    pub fn update_user_metadata(
        &self,
        username: &str,
        metadata: HashMap<String, String>,
    ) -> Result<User, AuthError> {
        let key = normalize_name(username);
        let mut user = self
            .users
            .read()
            .get(&key)
            .cloned()
            .ok_or_else(|| AuthError::UserNotFound(key.clone()))?;
        user.metadata = metadata;
        user.updated_at_unix_ms = now_unix_ms();
        self.persist_user(&user)?;
        self.users.write().insert(key, user.clone());
        Ok(user.public_user(&self.role_resolver()))
    }

    pub fn set_user_disabled(&self, username: &str, disabled: bool) -> Result<User, AuthError> {
        let key = normalize_name(username);
        let mut user = self
            .users
            .read()
            .get(&key)
            .cloned()
            .ok_or_else(|| AuthError::UserNotFound(key.clone()))?;
        user.disabled = disabled;
        user.updated_at_unix_ms = now_unix_ms();
        self.persist_user(&user)?;
        self.users.write().insert(key, user.clone());
        Ok(user.public_user(&self.role_resolver()))
    }

    pub fn unlock_user(&self, username: &str) -> Result<User, AuthError> {
        let key = normalize_name(username);
        let mut user = self
            .users
            .read()
            .get(&key)
            .cloned()
            .ok_or_else(|| AuthError::UserNotFound(key.clone()))?;
        user.failed_logins = 0;
        user.locked_until_unix_ms = None;
        user.updated_at_unix_ms = now_unix_ms();
        self.persist_user(&user)?;
        self.users.write().insert(key, user.clone());
        Ok(user.public_user(&self.role_resolver()))
    }

    pub fn validate_token(&self, token: &str) -> Result<Claims, AuthError> {
        self.token_manager.verify(token)
    }

    fn issue_user_token(&self, user: &User) -> Result<TokenResponse, AuthError> {
        let expiry_secs = self
            .config
            .token_expiry
            .map(|ttl| ttl.as_secs())
            .unwrap_or(0);
        let token = self
            .token_manager
            .issue(&user.username, user.role_names(), expiry_secs)?;
        Ok(TokenResponse {
            access_token: token,
            token_type: "Bearer".into(),
            expires_in: self.config.token_expiry.map(|ttl| ttl.as_secs()),
            scope: Some(user.role_names().join(" ")),
        })
    }

    fn persist_user(&self, user: &StoredUser) -> Result<(), AuthError> {
        self.storage.put_node_record(&stored_user_to_node(user)?)?;
        Ok(())
    }

    fn role_resolver(&self) -> RoleResolver<'_> {
        RoleResolver {
            entitlements: &self.entitlements,
            allowlist: &self.allowlist,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrivilegeDatabaseRef {
    pub name: String,
    pub owning_database_name: String,
}

pub trait DatabaseAccessMode: Send + Sync {
    fn can_see_database(&self, db_name: &str) -> bool;
    fn can_access_database(&self, db_name: &str) -> bool;
}

#[derive(Debug, Clone, Copy)]
pub struct FullDatabaseAccessMode;

impl DatabaseAccessMode for FullDatabaseAccessMode {
    fn can_see_database(&self, _db_name: &str) -> bool {
        true
    }
    fn can_access_database(&self, _db_name: &str) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DenyAllDatabaseAccessMode;

impl DatabaseAccessMode for DenyAllDatabaseAccessMode {
    fn can_see_database(&self, _db_name: &str) -> bool {
        false
    }
    fn can_access_database(&self, _db_name: &str) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolvedAccess {
    pub read: bool,
    pub write: bool,
}

#[derive(Debug, Clone)]
pub struct AllowlistDatabaseAccessMode {
    allowlist: HashMap<String, Vec<String>>,
    roles: Vec<String>,
}

impl AllowlistDatabaseAccessMode {
    pub fn new(allowlist: HashMap<String, Vec<String>>, principal_roles: Vec<String>) -> Self {
        Self {
            allowlist,
            roles: principal_roles,
        }
    }

    fn can_access(&self, db_name: &str) -> bool {
        if self.roles.is_empty() || self.allowlist.is_empty() {
            return false;
        }
        self.roles.iter().any(|role| {
            let role = normalize_name(role);
            match self.allowlist.get(&role) {
                None => true,
                Some(databases) if databases.is_empty() => true,
                Some(databases) => databases.iter().any(|db| db == db_name),
            }
        })
    }
}

impl DatabaseAccessMode for AllowlistDatabaseAccessMode {
    fn can_see_database(&self, db_name: &str) -> bool {
        self.can_access(db_name)
    }
    fn can_access_database(&self, db_name: &str) -> bool {
        self.can_access(db_name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DbPrivilege {
    pub read: bool,
    pub write: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrivilegeEntry {
    pub role: String,
    pub database: String,
    pub read: bool,
    pub write: bool,
}

pub struct RoleStore {
    storage: Arc<StorageEngine>,
    roles: RwLock<BTreeSet<String>>,
}

impl RoleStore {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self {
            storage,
            roles: RwLock::new(BTreeSet::new()),
        }
    }

    pub fn load(&self) -> Result<(), AuthError> {
        let mut roles = BTreeSet::new();
        for node in self.storage.get_nodes_by_label(ROLE_LABEL)? {
            if let Some(name) = string_property(&node, "name") {
                roles.insert(normalize_name(&name));
            }
        }
        *self.roles.write() = roles;
        Ok(())
    }

    pub fn all_roles(&self) -> Vec<String> {
        let mut roles = builtin_role_names()
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        roles.extend(self.roles.read().iter().cloned());
        roles.sort();
        roles.dedup();
        roles
    }

    pub fn exists(&self, name: &str) -> bool {
        let name = normalize_name(name);
        is_builtin_role(&name) || self.roles.read().contains(&name)
    }

    pub fn create_role(&self, name: &str) -> Result<(), AuthError> {
        let name = normalize_role_name(name)?;
        if is_builtin_role(&name) || self.roles.read().contains(&name) {
            return Err(AuthError::RoleExists(name));
        }
        self.storage.put_node_record(&simple_system_node(
            &format!("{ROLE_PREFIX}{name}"),
            ROLE_LABEL,
            BTreeMap::from([("name".into(), json!(name.clone()))]),
        ))?;
        self.roles.write().insert(name);
        Ok(())
    }

    pub fn delete_role(&self, name: &str) -> Result<(), AuthError> {
        let name = normalize_role_name(name)?;
        if is_builtin_role(&name) {
            return Err(AuthError::BuiltinRole(name));
        }
        if !self.roles.write().remove(&name) {
            return Err(AuthError::RoleNotFound(name));
        }
        self.storage
            .delete_node_record(&format!("{ROLE_PREFIX}{name}"))?;
        Ok(())
    }
}

pub struct AllowlistStore {
    storage: Arc<StorageEngine>,
    allowlist: RwLock<HashMap<String, Vec<String>>>,
}

impl AllowlistStore {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self {
            storage,
            allowlist: RwLock::new(HashMap::new()),
        }
    }

    pub fn load(&self) -> Result<(), AuthError> {
        let mut allowlist = HashMap::new();
        for node in self.storage.get_nodes_by_label(ROLE_DB_ACCESS_LABEL)? {
            if let Some(role) = string_property(&node, "role") {
                allowlist.insert(
                    normalize_name(&role),
                    string_vec_property(&node, "databases"),
                );
            }
        }
        *self.allowlist.write() = allowlist;
        Ok(())
    }

    pub fn allowlist(&self) -> HashMap<String, Vec<String>> {
        self.allowlist.read().clone()
    }

    pub fn databases_for_role(&self, role: &str) -> Option<Vec<String>> {
        self.allowlist.read().get(&normalize_name(role)).cloned()
    }

    pub fn save_role_databases(&self, role: &str, databases: Vec<String>) -> Result<(), AuthError> {
        let role = normalize_role_name(role)?;
        let normalized_databases = databases
            .into_iter()
            .map(|db| db.trim().to_string())
            .filter(|db| !db.is_empty())
            .collect::<Vec<_>>();
        self.storage.put_node_record(&simple_system_node(
            &format!("{ROLE_DB_ACCESS_PREFIX}{role}"),
            ROLE_DB_ACCESS_LABEL,
            BTreeMap::from([
                ("role".into(), json!(role.clone())),
                ("databases".into(), json!(normalized_databases.clone())),
            ]),
        ))?;
        self.allowlist.write().insert(role, normalized_databases);
        Ok(())
    }

    pub fn delete_role_databases(&self, role: &str) -> Result<(), AuthError> {
        let role = normalize_role_name(role)?;
        self.storage
            .delete_node_record(&format!("{ROLE_DB_ACCESS_PREFIX}{role}"))?;
        self.allowlist.write().remove(&role);
        Ok(())
    }

    pub fn seed_builtin_if_empty(&self) -> Result<(), AuthError> {
        if !self.allowlist.read().is_empty()
            || !self
                .storage
                .get_nodes_by_label(ROLE_DB_ACCESS_LABEL)?
                .is_empty()
        {
            return Ok(());
        }
        for role in builtin_role_names() {
            self.save_role_databases(role, Vec::new())?;
        }
        Ok(())
    }

    pub fn access_mode_for_roles(&self, roles: Vec<String>) -> AllowlistDatabaseAccessMode {
        AllowlistDatabaseAccessMode::new(self.allowlist(), roles)
    }
}

pub struct PrivilegesStore {
    storage: Arc<StorageEngine>,
    matrix: RwLock<HashMap<String, HashMap<String, DbPrivilege>>>,
}

impl PrivilegesStore {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self {
            storage,
            matrix: RwLock::new(HashMap::new()),
        }
    }

    pub fn load(&self) -> Result<(), AuthError> {
        let mut matrix: HashMap<String, HashMap<String, DbPrivilege>> = HashMap::new();
        for node in self.storage.get_nodes_by_label(DB_PRIVILEGE_LABEL)? {
            let Some(role) = string_property(&node, "role") else {
                continue;
            };
            let Some(database) = string_property(&node, "database") else {
                continue;
            };
            matrix.entry(normalize_name(&role)).or_default().insert(
                database,
                DbPrivilege {
                    read: bool_property(&node, "read"),
                    write: bool_property(&node, "write"),
                },
            );
        }
        *self.matrix.write() = matrix;
        Ok(())
    }

    pub fn save_privilege(
        &self,
        role: &str,
        database: &str,
        read: bool,
        write: bool,
    ) -> Result<(), AuthError> {
        let role = normalize_role_name(role)?;
        let database = database.trim().to_string();
        self.storage.put_node_record(&simple_system_node(
            &format!("{DB_PRIVILEGE_PREFIX}{role}:{database}"),
            DB_PRIVILEGE_LABEL,
            BTreeMap::from([
                ("role".into(), json!(role.clone())),
                ("database".into(), json!(database.clone())),
                ("read".into(), json!(read)),
                ("write".into(), json!(write)),
            ]),
        ))?;
        self.matrix
            .write()
            .entry(role)
            .or_default()
            .insert(database, DbPrivilege { read, write });
        Ok(())
    }

    pub fn matrix(&self) -> Vec<PrivilegeEntry> {
        let mut out = Vec::new();
        for (role, per_db) in self.matrix.read().iter() {
            for (database, privilege) in per_db {
                out.push(PrivilegeEntry {
                    role: role.clone(),
                    database: database.clone(),
                    read: privilege.read,
                    write: privilege.write,
                });
            }
        }
        out.sort_by(|a, b| {
            a.role
                .cmp(&b.role)
                .then_with(|| a.database.cmp(&b.database))
        });
        out
    }

    pub fn resolve(&self, roles: &[String], database: &str) -> ResolvedAccess {
        let matrix = self.matrix.read();
        let mut matched = false;
        let mut read = false;
        let mut write = false;
        for role in roles {
            if let Some(per_db) = matrix.get(&normalize_name(role))
                && let Some(privilege) = per_db.get(database)
            {
                matched = true;
                read |= privilege.read;
                write |= privilege.write;
            }
        }
        if matched {
            return ResolvedAccess { read, write };
        }
        let permissions = PermissionsForRoles::from_role_names(roles, None);
        ResolvedAccess {
            read: permissions.contains(&Permission::Read)
                || permissions.contains(&Permission::Admin),
            write: permissions.contains(&Permission::Write)
                || permissions.contains(&Permission::Admin),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum EntitlementCategory {
    Global,
    PerDatabase,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Entitlement {
    pub id: String,
    pub name: String,
    pub description: String,
    pub category: EntitlementCategory,
}

pub fn all_entitlements() -> Vec<Entitlement> {
    vec![
        entitlement(
            "read",
            "Read",
            "Read data and metadata",
            EntitlementCategory::Global,
        ),
        entitlement(
            "write",
            "Write",
            "Write graph data",
            EntitlementCategory::Global,
        ),
        entitlement(
            "create",
            "Create",
            "Create resources",
            EntitlementCategory::Global,
        ),
        entitlement(
            "delete",
            "Delete",
            "Delete resources",
            EntitlementCategory::Global,
        ),
        entitlement(
            "admin",
            "Admin",
            "Administrative control",
            EntitlementCategory::Global,
        ),
        entitlement(
            "schema",
            "Schema",
            "Schema operations",
            EntitlementCategory::Global,
        ),
        entitlement(
            "user_manage",
            "User management",
            "Manage users and roles",
            EntitlementCategory::Global,
        ),
        entitlement(
            "database_see",
            "Database: see",
            "See a database",
            EntitlementCategory::PerDatabase,
        ),
        entitlement(
            "database_access",
            "Database: access",
            "Access a database",
            EntitlementCategory::PerDatabase,
        ),
        entitlement(
            "database_read",
            "Database: read",
            "Read a database",
            EntitlementCategory::PerDatabase,
        ),
        entitlement(
            "database_write",
            "Database: write",
            "Write a database",
            EntitlementCategory::PerDatabase,
        ),
    ]
}

fn entitlement(
    id: &str,
    name: &str,
    description: &str,
    category: EntitlementCategory,
) -> Entitlement {
    Entitlement {
        id: id.into(),
        name: name.into(),
        description: description.into(),
        category,
    }
}

pub struct RoleEntitlementsStore {
    storage: Arc<StorageEngine>,
    entitlements: RwLock<HashMap<String, Vec<Permission>>>,
}

impl RoleEntitlementsStore {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self {
            storage,
            entitlements: RwLock::new(HashMap::new()),
        }
    }

    pub fn load(&self) -> Result<(), AuthError> {
        let mut entitlements = HashMap::new();
        for node in self.storage.get_nodes_by_label(ROLE_ENTITLEMENT_LABEL)? {
            if let Some(role) = string_property(&node, "role") {
                let permissions = string_vec_property(&node, "entitlements")
                    .into_iter()
                    .filter_map(|id| Permission::from_id(&id))
                    .collect::<Vec<_>>();
                if !permissions.is_empty() {
                    entitlements.insert(normalize_name(&role), permissions);
                }
            }
        }
        *self.entitlements.write() = entitlements;
        Ok(())
    }

    pub fn set(&self, role: &str, permissions: Vec<Permission>) -> Result<(), AuthError> {
        let role = normalize_role_name(role)?;
        if permissions.is_empty() {
            self.storage
                .delete_node_record(&format!("{ROLE_ENTITLEMENT_PREFIX}{role}"))?;
            self.entitlements.write().remove(&role);
            return Ok(());
        }
        let ids = permissions
            .iter()
            .map(|permission| permission.id())
            .collect::<Vec<_>>();
        self.storage.put_node_record(&simple_system_node(
            &format!("{ROLE_ENTITLEMENT_PREFIX}{role}"),
            ROLE_ENTITLEMENT_LABEL,
            BTreeMap::from([
                ("role".into(), json!(role.clone())),
                ("entitlements".into(), json!(ids)),
            ]),
        ))?;
        self.entitlements.write().insert(role, permissions);
        Ok(())
    }

    pub fn permissions_for_role(&self, role: &str) -> Vec<Permission> {
        let role = normalize_name(role);
        if let Some(permissions) = self.entitlements.read().get(&role) {
            return permissions.clone();
        }
        builtin_permissions(&role)
    }

    pub fn all(&self) -> HashMap<String, Vec<Permission>> {
        self.entitlements.read().clone()
    }
}

pub struct PermissionsForRoles;

impl PermissionsForRoles {
    pub fn from_role_names(
        roles: &[String],
        store: Option<&RoleEntitlementsStore>,
    ) -> BTreeSet<Permission> {
        let mut out = BTreeSet::new();
        for role in roles {
            let permissions = store
                .map(|store| store.permissions_for_role(role))
                .unwrap_or_else(|| builtin_permissions(&normalize_name(role)));
            out.extend(permissions);
        }
        out
    }
}

struct TokenCache {
    entries: HashMap<[u8; 32], TokenCacheEntry>,
    max_entries: usize,
    ttl: Duration,
}

#[derive(Clone)]
struct TokenCacheEntry {
    claims: Claims,
    expires_at_unix_ms: i64,
}

impl TokenCache {
    fn new(max_entries: usize, ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            max_entries,
            ttl,
        }
    }

    fn get(&mut self, token: &str) -> Option<Claims> {
        let now = now_unix_ms();
        let key = cache_key(token);
        let entry = self.entries.get(&key)?;
        if now >= entry.expires_at_unix_ms {
            self.entries.remove(&key);
            return None;
        }
        Some(entry.claims.clone())
    }

    fn set(&mut self, token: &str, claims: &Claims) {
        let now = now_unix_ms();
        self.entries
            .retain(|_, entry| now < entry.expires_at_unix_ms);
        while self.entries.len() >= self.max_entries {
            if let Some(key) = self.entries.keys().next().copied() {
                self.entries.remove(&key);
            } else {
                break;
            }
        }
        let mut expires_at = now + self.ttl.as_millis() as i64;
        if claims.exp > 0 {
            expires_at = expires_at.min((claims.exp as i64) * 1000);
        }
        self.entries.insert(
            cache_key(token),
            TokenCacheEntry {
                claims: claims.clone(),
                expires_at_unix_ms: expires_at,
            },
        );
    }
}

fn stored_user_to_node(user: &StoredUser) -> Result<NodeRecord, AuthError> {
    let value = serde_json::to_value(user)?;
    let mut properties = match value {
        Value::Object(map) => map.into_iter().collect::<BTreeMap<_, _>>(),
        _ => BTreeMap::new(),
    };
    properties.insert("username".into(), json!(user.username.clone()));
    Ok(NodeRecord {
        id: format!("{USER_PREFIX}{}", user.username),
        labels: vec![USER_LABEL.into(), SYSTEM_LABEL.into()],
        properties,
        named_embeddings: BTreeMap::new(),
        chunk_embeddings: Vec::new(),
        embed_meta: Default::default(),
        created_at_unix_ms: user.created_at_unix_ms,
        updated_at_unix_ms: user.updated_at_unix_ms,
    })
}

fn stored_user_from_node(node: &NodeRecord) -> Result<Option<StoredUser>, AuthError> {
    if !node.labels.iter().any(|label| label == USER_LABEL) {
        return Ok(None);
    }
    let value = Value::Object(node.properties.clone().into_iter().collect());
    Ok(Some(serde_json::from_value(value)?))
}

fn simple_system_node(id: &str, label: &str, properties: BTreeMap<String, Value>) -> NodeRecord {
    let now = now_unix_ms();
    NodeRecord {
        id: id.into(),
        labels: vec![label.into(), SYSTEM_LABEL.into()],
        properties,
        named_embeddings: BTreeMap::new(),
        chunk_embeddings: Vec::new(),
        embed_meta: Default::default(),
        created_at_unix_ms: now,
        updated_at_unix_ms: now,
    }
}

fn string_property(node: &NodeRecord, key: &str) -> Option<String> {
    node.properties
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn string_vec_property(node: &NodeRecord, key: &str) -> Vec<String> {
    node.properties
        .get(key)
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn bool_property(node: &NodeRecord, key: &str) -> bool {
    node.properties
        .get(key)
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn privilege_for_permissions(permissions: &[Permission]) -> Privilege {
    permissions.iter().fold(Privilege(0), |acc, permission| {
        acc.union(permission.privilege())
    })
}

pub const ROLE_ADMIN: &str = "admin";
pub const ROLE_EDITOR: &str = "editor";
pub const ROLE_VIEWER: &str = "viewer";
pub const ROLE_NONE: &str = "none";

pub fn builtin_role_names() -> Vec<&'static str> {
    vec![ROLE_ADMIN, ROLE_EDITOR, ROLE_VIEWER, ROLE_NONE]
}

pub fn is_builtin_role(name: &str) -> bool {
    let name = normalize_name(name);
    builtin_role_names().into_iter().any(|role| role == name)
}

pub fn builtin_permissions(role: &str) -> Vec<Permission> {
    match normalize_name(role).as_str() {
        ROLE_ADMIN => vec![
            Permission::Read,
            Permission::Write,
            Permission::Create,
            Permission::Delete,
            Permission::Admin,
            Permission::Schema,
            Permission::UserManage,
        ],
        ROLE_EDITOR => vec![
            Permission::Read,
            Permission::Write,
            Permission::Create,
            Permission::Delete,
        ],
        ROLE_VIEWER => vec![Permission::Read],
        _ => Vec::new(),
    }
}

fn normalize_username(username: &str) -> Result<String, AuthError> {
    let normalized = username.trim().to_string();
    if normalized.is_empty() {
        Err(AuthError::InvalidCredentials)
    } else {
        Ok(normalized)
    }
}

fn normalize_role_name(role: &str) -> Result<String, AuthError> {
    let role = normalize_name(role);
    if role.is_empty() || role.contains(':') || role.contains('/') {
        Err(AuthError::InvalidRoleName(role))
    } else {
        Ok(role)
    }
}

fn normalize_name(name: &str) -> String {
    name.trim()
        .to_ascii_lowercase()
        .trim_start_matches("role_")
        .to_string()
}

fn hash_password(password: &str) -> Result<String, AuthError> {
    let mut salt = [0u8; 16];
    fill_random(&mut salt).map_err(|err| AuthError::TokenInvalid(err.to_string()))?;
    let hash = password_digest(password.as_bytes(), &salt, PASSWORD_ITERATIONS);
    Ok(format!(
        "{PASSWORD_HASH_VERSION}${PASSWORD_ITERATIONS}${}${}",
        hex::encode(salt),
        hex::encode(hash)
    ))
}

fn verify_password(password: &str, hash: &str) -> bool {
    let parts = hash.split('$').collect::<Vec<_>>();
    if parts.len() != 4 || parts[0] != PASSWORD_HASH_VERSION {
        return false;
    }
    let Ok(iterations) = parts[1].parse::<u32>() else {
        return false;
    };
    let Ok(salt) = hex::decode(parts[2]) else {
        return false;
    };
    let Ok(expected) = hex::decode(parts[3]) else {
        return false;
    };
    let actual = password_digest(password.as_bytes(), &salt, iterations);
    constant_time_eq(&actual, &expected)
}

fn password_digest(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(salt).expect("HMAC accepts arbitrary key length");
    mac.update(password);
    let mut digest = mac.finalize().into_bytes().to_vec();
    for _ in 1..iterations {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&digest).expect("HMAC accepts arbitrary key length");
        mac.update(password);
        mac.update(salt);
        digest = mac.finalize().into_bytes().to_vec();
    }
    digest
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

fn cache_key(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(clippy::arc_with_non_send_sync)]
    fn test_auth() -> Authenticator {
        let storage = Arc::new(StorageEngine::open_temporary().unwrap());
        let auth = Authenticator::new(
            AuthConfig {
                jwt_secret: b"test-secret-at-least-32-bytes!!".to_vec(),
                token_expiry: Some(Duration::from_secs(3600)),
                max_failed_logins: 3,
                lockout_duration: Duration::from_secs(60),
                ..Default::default()
            },
            storage,
        )
        .unwrap();
        auth.seed_builtin_access_if_empty().unwrap();
        auth
    }

    #[test]
    fn token_manager_issue_and_verify() {
        let mgr = TokenManager::new("test-secret");
        let token = mgr.issue("alice", vec!["viewer".into()], 3600).unwrap();
        let claims = mgr.verify(&token).unwrap();
        assert_eq!(claims.sub, "alice");
        assert_eq!(claims.roles, vec!["viewer"]);
    }

    #[test]
    #[allow(clippy::arc_with_non_send_sync)]
    fn persistent_user_survives_reload() {
        let storage = Arc::new(StorageEngine::open_temporary().unwrap());
        let auth = Authenticator::new(
            AuthConfig {
                jwt_secret: b"test-secret-at-least-32-bytes!!".to_vec(),
                ..Default::default()
            },
            Arc::clone(&storage),
        )
        .unwrap();
        auth.seed_builtin_access_if_empty().unwrap();
        auth.create_user("alice", "password123", vec![ROLE_EDITOR.into()])
            .unwrap();

        let reloaded = Authenticator::new(
            AuthConfig {
                jwt_secret: b"test-secret-at-least-32-bytes!!".to_vec(),
                ..Default::default()
            },
            storage,
        )
        .unwrap();
        let user = reloaded.get_user("alice").unwrap();
        assert!(user.has_role(ROLE_EDITOR));
        assert!(user.has_permission(Permission::Write));
    }

    #[test]
    fn authenticate_issues_oauth_style_token_response_and_locks_account() {
        let auth = test_auth();
        auth.create_user("bob", "password123", vec![ROLE_ADMIN.into()])
            .unwrap();
        let (token, user) = auth.authenticate("bob", "password123").unwrap();
        assert_eq!(token.token_type, "Bearer");
        assert_eq!(token.expires_in, Some(3600));
        assert!(user.has_permission(Permission::UserManage));

        for _ in 0..3 {
            assert!(matches!(
                auth.authenticate("bob", "wrong"),
                Err(AuthError::InvalidCredentials)
            ));
        }
        assert!(matches!(
            auth.authenticate("bob", "password123"),
            Err(AuthError::AccountLocked)
        ));
        auth.unlock_user("bob").unwrap();
        assert!(auth.authenticate("bob", "password123").is_ok());
    }

    #[test]
    fn allowlist_is_persistent_and_resolves_access() {
        let auth = test_auth();
        auth.roles.create_role("analyst").unwrap();
        auth.allowlist
            .save_role_databases("analyst", vec!["copper".into()])
            .unwrap();
        let mode = auth.allowlist.access_mode_for_roles(vec!["analyst".into()]);
        assert!(mode.can_access_database("copper"));
        assert!(!mode.can_access_database("system"));
    }

    #[test]
    fn privilege_matrix_overrides_global_fallback() {
        let auth = test_auth();
        let viewer = vec![ROLE_VIEWER.to_string()];
        assert_eq!(
            auth.privileges.resolve(&viewer, "copper"),
            ResolvedAccess {
                read: true,
                write: false
            }
        );
        auth.privileges
            .save_privilege(ROLE_VIEWER, "copper", false, true)
            .unwrap();
        assert_eq!(
            auth.privileges.resolve(&viewer, "copper"),
            ResolvedAccess {
                read: false,
                write: true
            }
        );
    }

    #[test]
    #[allow(clippy::arc_with_non_send_sync)]
    fn role_entitlements_override_builtin_defaults_and_persist() {
        let storage = Arc::new(StorageEngine::open_temporary().unwrap());
        let auth = Authenticator::new(
            AuthConfig {
                jwt_secret: b"test-secret-at-least-32-bytes!!".to_vec(),
                ..Default::default()
            },
            Arc::clone(&storage),
        )
        .unwrap();
        auth.entitlements
            .set(ROLE_VIEWER, vec![Permission::Read, Permission::Schema])
            .unwrap();
        assert!(
            auth.entitlements
                .permissions_for_role(ROLE_VIEWER)
                .contains(&Permission::Schema)
        );

        let reloaded = Authenticator::new(
            AuthConfig {
                jwt_secret: b"test-secret-at-least-32-bytes!!".to_vec(),
                ..Default::default()
            },
            storage,
        )
        .unwrap();
        assert!(
            reloaded
                .entitlements
                .permissions_for_role(ROLE_VIEWER)
                .contains(&Permission::Schema)
        );
    }

    #[test]
    fn database_access_modes_match_secure_defaults() {
        assert!(FullDatabaseAccessMode.can_access_database("any"));
        assert!(!DenyAllDatabaseAccessMode.can_access_database("any"));
        let mode = AllowlistDatabaseAccessMode::new(HashMap::new(), vec![ROLE_ADMIN.into()]);
        assert!(!mode.can_access_database("copper"));
    }
}
