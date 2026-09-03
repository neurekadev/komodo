//! # Input Validation Module
//!
//! This module provides validation functions for user inputs to prevent
//! invalid data from entering the system and improve security.

use anyhow::Context;
use mogh_validations::{StringValidator, StringValidatorMatches};

use crate::config::core_config;

/// Minimum length for usernames
pub const MIN_USERNAME_LENGTH: usize = 1;
/// Maximum length for usernames
pub const MAX_USERNAME_LENGTH: usize = 100;

/// Validate usernames
///
/// - Between [MIN_USERNAME_LENGTH] and [MAX_USERNAME_LENGTH] characters
/// - Matches `^[a-zA-Z0-9._@-]+$`
pub fn validate_username(username: &str) -> anyhow::Result<()> {
  StringValidator::default()
    .min_length(MIN_USERNAME_LENGTH)
    .max_length(MAX_USERNAME_LENGTH)
    .matches(StringValidatorMatches::Username)
    .validate(username)
    .context("Failed to validate username")
}

/// Maximum length for passwords
pub const MAX_PASSWORD_LENGTH: usize = 1000;

/// Validate passwords
///
/// - Between [CoreConfig::min_password_length][komodo_client::entities::config::core::CoreConfig::min_password_length] and [MAX_PASSWORD_LENGTH] characters
pub fn validate_password(password: &str) -> anyhow::Result<()> {
  validate_password_length(
    password,
    core_config().min_password_length as usize,
  )
  .context("Failed to validate password")
}

pub(crate) fn validate_password_length(
  password: &str,
  min_length: usize,
) -> anyhow::Result<()> {
  let length = password.chars().count();
  anyhow::ensure!(
    length >= min_length,
    "Password must contain at least {min_length} characters"
  );
  anyhow::ensure!(
    length <= MAX_PASSWORD_LENGTH,
    "Password must contain at most {MAX_PASSWORD_LENGTH} characters"
  );
  Ok(())
}

/// Return the resource-specific webhook secret when set, otherwise the
/// global secret. Empty values and placeholders shipped in examples are
/// never valid authentication keys.
pub(crate) fn effective_webhook_secret<'a>(
  custom_secret: &'a str,
  global_secret: &'a str,
) -> anyhow::Result<&'a str> {
  let secret = if custom_secret.trim().is_empty() {
    global_secret
  } else {
    custom_secret
  };
  validate_webhook_secret(secret)?;
  Ok(secret)
}

pub(crate) fn validate_webhook_secret(
  secret: &str,
) -> anyhow::Result<()> {
  let secret = secret.trim();
  anyhow::ensure!(
    !secret.is_empty(),
    "No webhook secret is configured"
  );
  anyhow::ensure!(
    !["a_random_webhook_secret", "REPLACE_WITH_SECRET"]
      .iter()
      .any(|placeholder| secret.eq_ignore_ascii_case(placeholder)),
    "The configured webhook secret is an example placeholder"
  );
  Ok(())
}

/// Maximum length for API key names
pub const MAX_API_KEY_NAME_LENGTH: usize = 200;

/// Validate api key names
///
/// - Greater than [MAX_API_KEY_NAME_LENGTH] characters
pub fn validate_api_key_name(name: &str) -> anyhow::Result<()> {
  StringValidator::default()
    .max_length(MAX_API_KEY_NAME_LENGTH)
    .validate(name)
    .context("Failed to validate api key name")
}

/// Minimum length for variable names
pub const MIN_VARIABLE_NAME_LENGTH: usize = 1;
/// Maximum length for variable names
pub const MAX_VARIABLE_NAME_LENGTH: usize = 500;

/// Validate variable names
///
/// - Between [MIN_VARIABLE_NAME_LENGTH] and [MAX_VARIABLE_NAME_LENGTH] characters
/// - Matches `^[a-zA-Z_][a-zA-Z0-9_]*$`
pub fn validate_variable_name(name: &str) -> anyhow::Result<()> {
  StringValidator::default()
    .min_length(MIN_VARIABLE_NAME_LENGTH)
    .max_length(MAX_VARIABLE_NAME_LENGTH)
    .matches(StringValidatorMatches::VariableName)
    .validate(name)
    .context("Failed to validate variable name")
}

/// Maximum length for variable values
pub const MAX_VARIABLE_VALUE_LENGTH: usize = 10000;

/// Validate variable values
///
/// - Less than [MAX_VARIABLE_VALUE_LENGTH] characters
pub fn validate_variable_value(value: &str) -> anyhow::Result<()> {
  StringValidator::default()
    .max_length(MAX_VARIABLE_VALUE_LENGTH)
    .validate(value)
    .context("Failed to validate variable value")
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn password_length_counts_unicode_code_points() {
    assert!(validate_password_length("short", 15).is_err());
    assert!(validate_password_length("123456789012345", 15).is_ok());
    assert!(
      validate_password_length("🔐🔐🔐🔐🔐🔐🔐🔐🔐🔐🔐🔐🔐🔐🔐", 15)
        .is_ok()
    );
    assert!(
      validate_password_length(
        &"a".repeat(MAX_PASSWORD_LENGTH + 1),
        15
      )
      .is_err()
    );
  }

  #[test]
  fn webhook_secrets_require_a_real_configured_value() {
    assert!(effective_webhook_secret("", "").is_err());
    assert!(
      effective_webhook_secret("", "a_random_webhook_secret")
        .is_err()
    );
    assert!(
      effective_webhook_secret("REPLACE_WITH_SECRET", "valid-global")
        .is_err()
    );
    assert_eq!(
      effective_webhook_secret("", "valid-global").unwrap(),
      "valid-global"
    );
    assert_eq!(
      effective_webhook_secret("valid-resource", "valid-global")
        .unwrap(),
      "valid-resource"
    );
  }
}
