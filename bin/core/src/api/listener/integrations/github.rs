use anyhow::{Context, anyhow};
use axum::http::HeaderMap;
use hex::ToHex;
use hmac::{Hmac, KeyInit as _, Mac};
use serde::Deserialize;
use sha2::Sha256;

use crate::{
  config::core_config, helpers::validations::effective_webhook_secret,
};

use super::{ExtractBranch, VerifySecret};

type HmacSha256 = Hmac<Sha256>;

/// Listener implementation for Github type API, including Gitea
pub struct Github;

impl VerifySecret for Github {
  #[instrument("VerifyGithubSecret", skip_all)]
  fn verify_secret(
    headers: &HeaderMap,
    body: &str,
    custom_secret: &str,
  ) -> anyhow::Result<()> {
    let signature = headers
      .get("x-hub-signature-256")
      .context("No github signature in headers")?;
    let signature = signature
      .to_str()
      .context("Failed to get signature as string")?;
    let signature =
      signature.strip_prefix("sha256=").unwrap_or(signature);
    let secret = effective_webhook_secret(
      custom_secret,
      &core_config().webhook_secret,
    )?;
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
      .context("Failed to create hmac sha256 from secret")?;
    mac.update(body.as_bytes());
    let expected = mac.finalize().into_bytes().encode_hex::<String>();
    if signature == expected {
      Ok(())
    } else {
      Err(anyhow!("Signature does not equal expected"))
    }
  }
}

#[derive(Deserialize)]
struct GithubWebhookBody {
  #[serde(rename = "ref")]
  branch: String,
}

impl ExtractBranch for Github {
  fn extract_branch(body: &str) -> anyhow::Result<String> {
    let branch = serde_json::from_str::<GithubWebhookBody>(body)
      .context("Failed to parse github request body")?
      .branch
      .replace("refs/heads/", "");
    Ok(branch)
  }
}
