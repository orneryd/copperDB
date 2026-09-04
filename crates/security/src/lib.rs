//! Security validation for copperDB ingress and outbound URL handling.
//!
//! This crate owns protocol-neutral validation contracts. HTTP/Bolt/GraphQL/MCP
//! adapters should call these validators instead of embedding security rules in
//! protocol code.

use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use thiserror::Error;
use url::{Host, Url};

pub const MAX_TOKEN_LENGTH: usize = 8192;
pub const MAX_URL_LENGTH: usize = 2048;
pub const MAX_HEADER_LENGTH: usize = 4096;

const URL_PARAMETER_NAMES: &[&str] = &["callback", "redirect", "redirect_uri", "url", "webhook"];
const TOKEN_VALID_CHARS: &str =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~+/=";
const INJECTION_CHARS: &[char] = &['`', '"', '\'', ';', '\n', '\r', '\0', '\\'];

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SecurityError {
    #[error("token contains invalid characters")]
    TokenInvalidChars,
    #[error("token exceeds maximum length of {0} characters")]
    TokenTooLong(usize),
    #[error("token must be a non-empty string")]
    TokenEmpty,
    #[error("only HTTP/HTTPS protocols are allowed")]
    UrlInvalidProtocol,
    #[error("private IP addresses are not allowed")]
    UrlPrivateIp,
    #[error("localhost is not allowed in production")]
    UrlLocalhost,
    #[error("only HTTPS URLs are allowed in production")]
    UrlHttpNotAllowed,
    #[error("URL exceeds maximum length of {0} characters")]
    UrlTooLong(usize),
    #[error("invalid URL format")]
    UrlInvalid,
    #[error("header value exceeds maximum length of {0} characters")]
    HeaderTooLong(usize),
    #[error("header value contains invalid control characters")]
    HeaderInvalidChars,
    #[error("invalid HTTP origin")]
    OriginInvalid,
    #[error("HTTP origin does not match request host")]
    OriginMismatch,
    #[error("invalid identifier: {0}")]
    InvalidIdentifier(String),
    #[error("invalid label: {0}")]
    InvalidLabel(String),
    #[error("invalid property key: {0}")]
    InvalidPropertyKey(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SecurityConfig {
    pub environment: String,
    pub allow_http: bool,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            environment: "development".into(),
            allow_http: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityMiddleware {
    is_development: bool,
    allow_http: bool,
}

impl SecurityMiddleware {
    pub fn new() -> Self {
        Self::with_config(SecurityConfig::default())
    }

    pub fn with_config(config: SecurityConfig) -> Self {
        let environment = config.environment.trim().to_ascii_lowercase();
        Self {
            is_development: environment.is_empty()
                || environment == "development"
                || environment == "dev",
            allow_http: config.allow_http,
        }
    }

    pub fn is_development(&self) -> bool {
        self.is_development
    }

    pub fn allow_http(&self) -> bool {
        self.allow_http
    }

    pub fn validate_request(&self, request: &SecurityRequest) -> Result<(), RequestViolation> {
        for (name, value) in &request.headers {
            validate_header_value(value).map_err(|source| RequestViolation {
                target: RequestTarget::Header(name.clone()),
                source,
            })?;
        }

        if let Some(value) = request.header("authorization")
            && let Some(token) = bearer_or_basic_token(value)
        {
            validate_token(token).map_err(|source| RequestViolation {
                target: RequestTarget::Authorization,
                source,
            })?;
        }

        if let Some(token) = request.query_param("token") {
            validate_token(token).map_err(|source| RequestViolation {
                target: RequestTarget::QueryParam("token".into()),
                source,
            })?;
        }

        for name in URL_PARAMETER_NAMES {
            if let Some(value) = request.query_param(name) {
                validate_url(value, self.is_development, self.allow_http).map_err(|source| {
                    RequestViolation {
                        target: RequestTarget::QueryParam((*name).into()),
                        source,
                    }
                })?;
            }
        }

        Ok(())
    }
}

impl Default for SecurityMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SecurityRequest {
    pub headers: Vec<(String, String)>,
    pub query_params: Vec<(String, String)>,
}

impl SecurityRequest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    pub fn with_query_param(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.query_params.push((name.into(), value.into()));
        self
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    pub fn query_param(&self, name: &str) -> Option<&str> {
        self.query_params
            .iter()
            .find(|(candidate, _)| candidate == name)
            .map(|(_, value)| value.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestViolation {
    pub target: RequestTarget,
    pub source: SecurityError,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestTarget {
    Header(String),
    Authorization,
    QueryParam(String),
}

pub fn validate_token(token: &str) -> Result<(), SecurityError> {
    if token.trim().is_empty() {
        return Err(SecurityError::TokenEmpty);
    }
    if token.len() > MAX_TOKEN_LENGTH {
        return Err(SecurityError::TokenTooLong(MAX_TOKEN_LENGTH));
    }
    let lowercase = token.to_ascii_lowercase();
    if lowercase.contains("javascript:")
        || lowercase.contains("data:")
        || lowercase.contains("file:")
        || lowercase.contains("vbscript:")
    {
        return Err(SecurityError::TokenInvalidChars);
    }
    if token.chars().any(|ch| {
        matches!(
            ch,
            '\r' | '\n'
                | '<'
                | '>'
                | '\''
                | '"'
                | '&'
                | ';'
                | '('
                | ')'
                | '{'
                | '}'
                | '['
                | ']'
                | '\\'
                | '\0'
        )
    }) {
        return Err(SecurityError::TokenInvalidChars);
    }
    if !token.chars().all(|ch| TOKEN_VALID_CHARS.contains(ch)) {
        return Err(SecurityError::TokenInvalidChars);
    }
    Ok(())
}

pub fn validate_url(
    raw_url: &str,
    is_development: bool,
    allow_http: bool,
) -> Result<(), SecurityError> {
    if raw_url.len() > MAX_URL_LENGTH {
        return Err(SecurityError::UrlTooLong(MAX_URL_LENGTH));
    }
    if raw_url.trim().is_empty() {
        return Err(SecurityError::UrlInvalid);
    }

    let parsed = Url::parse(raw_url).map_err(|_| SecurityError::UrlInvalid)?;
    match parsed.scheme().to_ascii_lowercase().as_str() {
        "http" => {
            if !is_development && !allow_http {
                return Err(SecurityError::UrlHttpNotAllowed);
            }
        }
        "https" => {}
        _ => return Err(SecurityError::UrlInvalidProtocol),
    }

    let host = parsed.host().ok_or(SecurityError::UrlInvalid)?;
    match host {
        Host::Domain(domain) => {
            let lowercase = domain.to_ascii_lowercase();
            if !is_development && (lowercase == "localhost" || lowercase == "host.docker.internal")
            {
                return Err(SecurityError::UrlLocalhost);
            }
        }
        Host::Ipv4(ip) => validate_ip(IpAddr::V4(ip), is_development)?,
        Host::Ipv6(ip) => validate_ip(IpAddr::V6(ip), is_development)?,
    }

    Ok(())
}

pub fn validate_header_value(value: &str) -> Result<(), SecurityError> {
    if value.len() > MAX_HEADER_LENGTH {
        return Err(SecurityError::HeaderTooLong(MAX_HEADER_LENGTH));
    }
    if value.chars().any(|ch| matches!(ch, '\r' | '\n' | '\0')) {
        return Err(SecurityError::HeaderInvalidChars);
    }
    Ok(())
}

pub fn validate_http_origin(origin: &str, request_host: &str) -> Result<(), SecurityError> {
    if origin.len() > MAX_URL_LENGTH || request_host.len() > MAX_HEADER_LENGTH {
        return Err(SecurityError::OriginInvalid);
    }
    let parsed_origin = Url::parse(origin).map_err(|_| SecurityError::OriginInvalid)?;
    if !matches!(parsed_origin.scheme(), "http" | "https")
        || !parsed_origin.username().is_empty()
        || parsed_origin.password().is_some()
        || parsed_origin.path() != "/"
        || parsed_origin.query().is_some()
        || parsed_origin.fragment().is_some()
    {
        return Err(SecurityError::OriginInvalid);
    }
    let origin_host = parsed_origin
        .host_str()
        .ok_or(SecurityError::OriginInvalid)?;
    let request_port = explicit_authority_port(request_host)?;
    let parsed_request_host =
        Url::parse(&format!("http://{request_host}")).map_err(|_| SecurityError::OriginInvalid)?;
    if !parsed_request_host.username().is_empty()
        || parsed_request_host.password().is_some()
        || parsed_request_host.path() != "/"
        || parsed_request_host.query().is_some()
        || parsed_request_host.fragment().is_some()
    {
        return Err(SecurityError::OriginInvalid);
    }
    let request_hostname = parsed_request_host
        .host_str()
        .ok_or(SecurityError::OriginInvalid)?;
    if !origin_host.eq_ignore_ascii_case(request_hostname) {
        return Err(SecurityError::OriginMismatch);
    }
    match request_port {
        Some(port) if parsed_origin.port_or_known_default() != Some(port) => {
            Err(SecurityError::OriginMismatch)
        }
        None if parsed_origin.port().is_some() => Err(SecurityError::OriginMismatch),
        _ => Ok(()),
    }
}

fn explicit_authority_port(authority: &str) -> Result<Option<u16>, SecurityError> {
    if authority.is_empty() || authority.trim() != authority {
        return Err(SecurityError::OriginInvalid);
    }
    let port = if let Some(rest) = authority.strip_prefix('[') {
        let closing = rest.find(']').ok_or(SecurityError::OriginInvalid)?;
        match &rest[closing + 1..] {
            "" => None,
            suffix => Some(
                suffix
                    .strip_prefix(':')
                    .ok_or(SecurityError::OriginInvalid)?,
            ),
        }
    } else if let Some((host, port)) = authority.rsplit_once(':') {
        if host.is_empty() || host.contains(':') {
            return Err(SecurityError::OriginInvalid);
        }
        Some(port)
    } else {
        None
    };
    port.map(|port| {
        port.parse::<u16>()
            .map_err(|_| SecurityError::OriginInvalid)
    })
    .transpose()
}

pub fn sanitize_string(input: &str) -> String {
    input
        .chars()
        .filter(|ch| *ch != '\0' && (*ch >= ' ' || *ch == '\t' || *ch == '\n'))
        .collect::<String>()
        .trim()
        .to_string()
}

pub fn sanitize_identifier(input: &str) -> Result<String, SecurityError> {
    if input.is_empty() {
        return Err(SecurityError::InvalidIdentifier(
            "identifier must not be empty".into(),
        ));
    }
    for ch in INJECTION_CHARS {
        if input.contains(*ch) {
            return Err(SecurityError::InvalidIdentifier(format!(
                "identifier contains forbidden character: {:?}",
                ch
            )));
        }
    }
    if input.chars().any(|ch| ch.is_control()) {
        return Err(SecurityError::InvalidIdentifier(
            "identifier contains control characters".into(),
        ));
    }
    Ok(input.to_string())
}

pub fn sanitize_string_value(input: &str) -> String {
    input.replace('\\', "\\\\").replace('\'', "\\'")
}

pub fn validate_label(label: &str) -> Result<(), SecurityError> {
    validate_graph_name(label, "label", SecurityError::InvalidLabel)
}

pub fn validate_property_key(key: &str) -> Result<(), SecurityError> {
    validate_graph_name(key, "property key", SecurityError::InvalidPropertyKey)
}

pub fn generate_token() -> String {
    use getrandom::fill as fill_random;
    let mut bytes = [0u8; 32];
    fill_random(&mut bytes).expect("os rng should be available");
    hex::encode(bytes)
}

fn validate_graph_name(
    value: &str,
    kind: &str,
    error: fn(String) -> SecurityError,
) -> Result<(), SecurityError> {
    if value.is_empty() {
        return Err(error(format!("{kind} must not be empty")));
    }
    if !value.chars().all(|ch| ch.is_alphanumeric() || ch == '_') {
        return Err(error(format!(
            "{kind} '{value}' contains invalid characters"
        )));
    }
    Ok(())
}

fn bearer_or_basic_token(value: &str) -> Option<&str> {
    let (scheme, token) = value.split_once(' ')?;
    if scheme.eq_ignore_ascii_case("bearer") || scheme.eq_ignore_ascii_case("basic") {
        Some(token.trim())
    } else {
        None
    }
}

fn validate_ip(ip: IpAddr, is_development: bool) -> Result<(), SecurityError> {
    if is_development && ip.is_loopback() {
        return Ok(());
    }
    if is_private_ip(ip) {
        return Err(SecurityError::UrlPrivateIp);
    }
    Ok(())
}

fn is_private_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_private_ipv4(ip),
        IpAddr::V6(ip) => is_private_ipv6(ip),
    }
}

fn is_private_ipv4(ip: Ipv4Addr) -> bool {
    ip.is_loopback() || ip.is_private() || ip.is_link_local()
}

fn is_private_ipv6(ip: Ipv6Addr) -> bool {
    ip.is_loopback() || ip.is_unique_local() || is_ipv6_link_local(ip)
}

fn is_ipv6_link_local(ip: Ipv6Addr) -> bool {
    (ip.segments()[0] & 0xffc0) == 0xfe80
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_token_accepts_oauth_and_jwt_shapes() {
        validate_token(
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.SflKxwRJSM",
        )
        .unwrap();
        validate_token("ya29.a0AfH6SMBx").unwrap();
        validate_token("abc123-_~+/=").unwrap();
        validate_token(&"a".repeat(MAX_TOKEN_LENGTH)).unwrap();
    }

    #[test]
    fn validate_token_blocks_injection_patterns() {
        for token in [
            "token\r\nX-Evil: header",
            "<script>alert('xss')</script>",
            "javascript:alert('xss')",
            "data:text/html,<script>alert('xss')</script>",
            "file:///etc/passwd",
            "token\0evil",
            "token;rm -rf /",
        ] {
            assert_eq!(validate_token(token), Err(SecurityError::TokenInvalidChars));
        }
        assert_eq!(validate_token(""), Err(SecurityError::TokenEmpty));
        assert_eq!(
            validate_token(&"a".repeat(MAX_TOKEN_LENGTH + 1)),
            Err(SecurityError::TokenTooLong(MAX_TOKEN_LENGTH))
        );
    }

    #[test]
    fn validate_url_enforces_protocol_and_https_policy() {
        validate_url("https://oauth.example.com/userinfo", false, false).unwrap();
        validate_url("https://oauth.example.com:8443/api", false, false).unwrap();
        validate_url("http://localhost:8888/api", true, true).unwrap();
        validate_url("https://8.8.8.8/api", false, false).unwrap();
        assert_eq!(
            validate_url("ftp://example.com/data", false, false),
            Err(SecurityError::UrlInvalidProtocol)
        );
        assert_eq!(
            validate_url("http://example.com/api", false, false),
            Err(SecurityError::UrlHttpNotAllowed)
        );
        validate_url("http://example.com/api", false, true).unwrap();
        validate_url("http://example.com/api", true, false).unwrap();
    }

    #[test]
    fn validate_url_blocks_private_and_metadata_addresses() {
        for raw_url in [
            "https://10.0.0.1/api",
            "https://172.16.0.1/api",
            "https://172.31.255.255/api",
            "https://192.168.1.1/api",
            "https://169.254.169.254/latest/meta-data/",
            "https://127.0.0.1/api",
            "https://[fc00::1]/api",
            "https://[fe80::1]/api",
        ] {
            assert_eq!(
                validate_url(raw_url, false, false),
                Err(SecurityError::UrlPrivateIp)
            );
        }
    }

    #[test]
    fn validate_url_allows_loopback_only_in_development() {
        validate_url("http://127.0.0.1:9200/index", true, false).unwrap();
        validate_url("http://[::1]:8080/api", true, false).unwrap();
        validate_url("https://localhost:8080/api", true, false).unwrap();
        assert_eq!(
            validate_url("https://localhost:8080/api", false, false),
            Err(SecurityError::UrlLocalhost)
        );
    }

    #[test]
    fn validate_url_rejects_empty_invalid_and_too_long_urls() {
        assert_eq!(
            validate_url("", false, false),
            Err(SecurityError::UrlInvalid)
        );
        assert_eq!(
            validate_url("   ", false, false),
            Err(SecurityError::UrlInvalid)
        );
        assert_eq!(
            validate_url(
                &format!("https://example.com/{}", "a".repeat(MAX_URL_LENGTH)),
                false,
                false,
            ),
            Err(SecurityError::UrlTooLong(MAX_URL_LENGTH))
        );
    }

    #[test]
    fn header_and_string_validation_match_security_contract() {
        validate_header_value("Mozilla/5.0").unwrap();
        validate_header_value("application/json; charset=utf-8").unwrap();
        assert_eq!(
            validate_header_value("value\r\nX-Injected: evil"),
            Err(SecurityError::HeaderInvalidChars)
        );
        assert_eq!(
            validate_header_value("value\0injected"),
            Err(SecurityError::HeaderInvalidChars)
        );
        assert_eq!(
            validate_header_value(&"a".repeat(MAX_HEADER_LENGTH + 1)),
            Err(SecurityError::HeaderTooLong(MAX_HEADER_LENGTH))
        );
        assert_eq!(sanitize_string("hello\0\x01\x02world"), "helloworld");
        assert_eq!(sanitize_string("  hello world  "), "hello world");
    }

    #[test]
    fn http_origin_validation_matches_hosts_and_effective_ports() {
        for (origin, host) in [
            ("http://localhost", "localhost"),
            ("https://EXAMPLE.com", "example.com"),
            ("https://example.com", "example.com:443"),
            ("http://127.0.0.1:7474", "127.0.0.1:7474"),
            ("http://[::1]:7474", "[::1]:7474"),
        ] {
            validate_http_origin(origin, host).unwrap();
        }

        for (origin, host, expected) in [
            (
                "https://attacker.example",
                "localhost:7474",
                SecurityError::OriginMismatch,
            ),
            (
                "http://example.com:8080",
                "example.com",
                SecurityError::OriginMismatch,
            ),
            (
                "http://example.com",
                "example.com:443",
                SecurityError::OriginMismatch,
            ),
            ("null", "localhost:7474", SecurityError::OriginInvalid),
            (
                "https://user@example.com",
                "example.com",
                SecurityError::OriginInvalid,
            ),
            (
                "https://example.com/path",
                "example.com",
                SecurityError::OriginInvalid,
            ),
            (
                "https://example.com?query=1",
                "example.com",
                SecurityError::OriginInvalid,
            ),
            (
                "https://example.com",
                "example.com:not-a-port",
                SecurityError::OriginInvalid,
            ),
            (
                "https://example.com",
                "user@example.com",
                SecurityError::OriginInvalid,
            ),
            (
                "https://example.com",
                "example.com/path",
                SecurityError::OriginInvalid,
            ),
            (
                "https://example.com",
                "example.com?query=1",
                SecurityError::OriginInvalid,
            ),
        ] {
            assert_eq!(validate_http_origin(origin, host), Err(expected));
        }
    }

    #[test]
    fn middleware_config_and_request_validation_work() {
        let dev = SecurityMiddleware::with_config(SecurityConfig {
            environment: "dev".into(),
            allow_http: true,
        });
        assert!(dev.is_development());
        assert!(dev.allow_http());

        let prod = SecurityMiddleware::with_config(SecurityConfig {
            environment: "production".into(),
            allow_http: false,
        });
        assert!(!prod.is_development());
        assert!(!prod.allow_http());

        let request = SecurityRequest::new()
            .with_header("User-Agent", "Mozilla/5.0")
            .with_header("Authorization", "Bearer abc123-_~+/=")
            .with_query_param("callback", "https://example.com/callback");
        prod.validate_request(&request).unwrap();

        let bad_header = SecurityRequest::new().with_header("X-Custom", "value\0injection");
        let violation = prod.validate_request(&bad_header).unwrap_err();
        assert_eq!(violation.target, RequestTarget::Header("X-Custom".into()));
        assert_eq!(violation.source, SecurityError::HeaderInvalidChars);

        let bad_token = SecurityRequest::new().with_header("Authorization", "Bearer <script>");
        let violation = prod.validate_request(&bad_token).unwrap_err();
        assert_eq!(violation.target, RequestTarget::Authorization);
        assert_eq!(violation.source, SecurityError::TokenInvalidChars);

        let ssrf = SecurityRequest::new().with_query_param("webhook", "https://192.168.1.1/hook");
        let violation = prod.validate_request(&ssrf).unwrap_err();
        assert_eq!(
            violation.target,
            RequestTarget::QueryParam("webhook".into())
        );
        assert_eq!(violation.source, SecurityError::UrlPrivateIp);
    }

    #[test]
    fn graph_identifier_helpers_remain_strict() {
        assert!(sanitize_identifier("Person").is_ok());
        assert!(sanitize_identifier("my_label_123").is_ok());
        assert!(sanitize_identifier("Person`").is_err());
        assert!(sanitize_identifier("label; DROP").is_err());
        assert_eq!(sanitize_string_value("it's a test"), "it\\'s a test");
        validate_label("Movie_Title").unwrap();
        validate_property_key("created_at").unwrap();
        assert!(validate_label("Person-Node").is_err());
        assert!(validate_property_key("my-key").is_err());
    }

    #[test]
    fn generated_tokens_are_random_hex() {
        let first = generate_token();
        let second = generate_token();
        assert_eq!(first.len(), 64);
        assert_ne!(first, second);
        validate_token(&first).unwrap();
    }
}
