#![allow(async_fn_in_trait)]

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

use super::types::{DogmaAttributeId, TypeId};
use crate::RatelimitedClient;

#[derive(Error, Debug)]
pub enum HoboleaksError {
    #[error("HTTP error: {0}")]
    RequestError(#[from] reqwest::Error),

    #[error("API error: {status} - {message}")]
    ApiError { status: u16, message: String },

    #[error("Auth error: {0}")]
    AuthError(String),

    #[error("Parsing error: {0}")]
    ParseError(String),

    #[error("EVE server error: {0}")]
    ServerError(String),
}

impl HoboleaksError {
    pub async fn from_response(response: reqwest::Response) -> Result<reqwest::Response, Self> {
        let status = response.status();

        if status.is_success() {
            return Ok(response);
        }

        match status.as_u16() {
            401 | 403 => {
                let error_text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Authentication failed".to_string());
                Err(Self::AuthError(error_text))
            }
            500..=599 => {
                let error_text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Server error".to_string());
                Err(Self::ServerError(error_text))
            }
            _ => {
                let status_code = status.as_u16();
                let error_text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unknown error".to_string());

                Err(Self::ApiError {
                    status: status_code,
                    message: error_text,
                })
            }
        }
    }

    pub fn is_temporary(&self) -> bool {
        match self {
            HoboleaksError::RequestError(e) => {
                // Network errors are usually temporary
                e.is_timeout() || e.is_connect()
            }
            HoboleaksError::ServerError(_) => true, // 5xx errors are temporary
            HoboleaksError::ApiError { status, .. } => {
                // Rate limiting or temporary service issues
                *status == 429 || *status == 503 || *status == 502
            }
            HoboleaksError::AuthError(_) => false, // Auth errors are not temporary
            HoboleaksError::ParseError(_) => false, // Parse errors are not temporary
        }
    }
}

pub trait ResponseExt {
    async fn parse_esi_json<T: serde::de::DeserializeOwned>(self) -> Result<T, HoboleaksError>;
}

impl ResponseExt for reqwest::Response {
    async fn parse_esi_json<T: serde::de::DeserializeOwned>(self) -> Result<T, HoboleaksError> {
        self.json::<T>().await.map_err(|e| {
            if e.is_decode() {
                HoboleaksError::ParseError(format!("failed to parse JSON: {}", e))
            } else {
                HoboleaksError::RequestError(e)
            }
        })
    }
}

pub type MutaplasmidData = HashMap<TypeId, MutaplasmidsEffects>;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MutaplasmidsEffects {
    pub input_output_mapping: Vec<InputOutputMapping>,
    pub attribute_i_ds: HashMap<DogmaAttributeId, AttributeRange>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct InputOutputMapping {
    pub resulting_type: TypeId,
    pub applicable_types: Vec<TypeId>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AttributeRange {
    pub max: f64,
    pub min: f64,
}

pub async fn get_mutaplasmids(
    http_client: &RatelimitedClient,
) -> Result<MutaplasmidData, HoboleaksError> {
    println!("============5");

    let url = format!("https://sde.hoboleaks.space/tq/dynamicitemattributes.json");
    println!("calling url {url}");

    let response = http_client.get(&url).send().await?;

    println!(
        "response: {:?}, response code: {:?}",
        response.status(),
        response.headers()
    );

    response.parse_esi_json().await
}

// Enhanced get_mutaplasmids with retry logic
pub async fn get_mutaplasmids_with_retry(
    http_client: &RatelimitedClient,
    max_retries: u32,
) -> Result<MutaplasmidData, HoboleaksError> {
    let mut last_error = None;

    for attempt in 0..=max_retries {
        match get_mutaplasmids(http_client).await {
            Ok(data) => return Ok(data),
            Err(e) => {
                println!("Hoboleaks attempt {} failed: {}", attempt + 1, e);

                if !e.is_temporary() || attempt == max_retries {
                    return Err(e);
                }

                // Exponential backoff for temporary errors
                let delay = std::time::Duration::from_millis(1000 * (2_u64.pow(attempt)));
                println!("Retrying in {:?}...", delay);
                tokio::time::sleep(delay).await;

                last_error = Some(e);
            }
        }
    }

    Err(last_error.unwrap())
}
