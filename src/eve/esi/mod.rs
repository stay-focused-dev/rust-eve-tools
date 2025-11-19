#![allow(async_fn_in_trait)]

use oauth2::TokenResponse;
use oauth2::basic::BasicTokenResponse;
use thiserror::Error;

use super::types::{
    AssetItem, AssetName, CharacterResponse, DogmaAttribute, DogmaAttributeId, DynamicItem,
    ItemType, MarketGroup, MarketGroupId, MarketOrder, RegionId, Station, StationId, TypeId,
};
use crate::RatelimitedClient;

#[derive(Error, Debug)]
pub enum EsiError {
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

impl EsiError {
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
}

pub trait ResponseExt {
    async fn parse_esi_json<T: serde::de::DeserializeOwned>(self) -> Result<T, EsiError>;
}

impl ResponseExt for reqwest::Response {
    async fn parse_esi_json<T: serde::de::DeserializeOwned>(self) -> Result<T, EsiError> {
        self.json::<T>().await.map_err(|e| {
            if e.is_decode() {
                EsiError::ParseError(format!("failed to parse JSON: {}", e))
            } else {
                EsiError::RequestError(e)
            }
        })
    }
}

pub async fn get_character_info(
    http_client: &RatelimitedClient,
    token_response: &BasicTokenResponse,
) -> Result<CharacterResponse, EsiError> {
    println!("============1");

    let response = http_client
        .get("https://esi.evetech.net/verify/")
        .header(
            "Authorization",
            format!("Bearer {}", token_response.access_token().secret()),
        )
        .send()
        .await?;

    EsiError::from_response(response)
        .await?
        .json::<CharacterResponse>()
        .await
        .map_err(EsiError::from)
}

pub async fn get_assets_names(
    http_client: &RatelimitedClient,
    token_response: &BasicTokenResponse,
    character_id: u64,
    item_ids: &Vec<i64>,
) -> Result<Vec<AssetName>, EsiError> {
    println!("============2");
    let access_token = token_response.access_token().secret();

    let url = format!("https://esi.evetech.net/latest/characters/{character_id}/assets/names/");
    println!("get url: {url}, items count: {}", item_ids.len());

    let response = http_client
        .post(url)
        .header("Authorization", format!("Bearer {access_token}"))
        .json(&item_ids)
        .send()
        .await?;

    println!(
        "response: {:?}, response code: {:?}",
        response.status(),
        response.headers()
    );

    EsiError::from_response(response)
        .await?
        .parse_esi_json::<Vec<AssetName>>()
        .await
}

pub async fn get_assets_chunk(
    http_client: &RatelimitedClient,
    token_response: &BasicTokenResponse,
    character_id: u64,
    page: usize,
) -> Result<(Vec<AssetItem>, usize), EsiError> {
    println!("============3");
    let access_token = token_response.access_token().secret();

    let url =
        format!("https://esi.evetech.net/latest/characters/{character_id}/assets/?page={page}");
    println!("get url: {url}");

    let response = http_client
        .get(url)
        .header("Authorization", format!("Bearer {access_token}"))
        .send()
        .await?;

    println!(
        "response: {:?}, response code: {:?}",
        response.status(),
        response.headers()
    );

    let pages_str = response
        .headers()
        .get("x-pages")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("1");

    let total_pages = pages_str.parse::<usize>().unwrap_or(1);
    let assets = response.parse_esi_json::<Vec<AssetItem>>().await?;

    Ok((assets, total_pages))
}

pub async fn get_dynamic_item_attributes(
    http_client: &RatelimitedClient,
    item_id: i64,
    type_id: i32,
) -> Result<DynamicItem, EsiError> {
    println!("============4");

    let url = format!("https://esi.evetech.net/latest/dogma/dynamic/items/{type_id}/{item_id}/");
    println!("calling url {url}");

    let response = http_client
        .get(&url)
        //.header("Authorization", format!("Bearer {access_token}"))
        .send()
        .await?;

    println!(
        "response: {:?}, response code: {:?}",
        response.status(),
        response.headers()
    );

    response.parse_esi_json::<DynamicItem>().await
}

pub async fn get_station(
    http_client: &RatelimitedClient,
    station_id: StationId,
) -> Result<Station, EsiError> {
    println!("============5");

    let url = format!("https://esi.evetech.net/latest/universe/stations/{station_id}/");
    println!("calling url {url}");

    let response = http_client.get(&url).send().await?;

    println!(
        "response: {:?}, response code: {:?}",
        response.status(),
        response.headers()
    );

    response.parse_esi_json().await
}

pub async fn get_dogma_attribute(
    http_client: &RatelimitedClient,
    attribute_id: DogmaAttributeId,
) -> Result<DogmaAttribute, EsiError> {
    println!("============6");

    let url = format!("https://esi.evetech.net/latest/dogma/attributes/{attribute_id}/");
    println!("calling url {url}");

    let response = http_client.get(&url).send().await?;

    println!(
        "response: {:?}, response code: {:?}",
        response.status(),
        response.headers()
    );

    response.parse_esi_json::<DogmaAttribute>().await
}

pub async fn get_type(
    http_client: &RatelimitedClient,
    type_id: TypeId,
) -> Result<ItemType, EsiError> {
    println!("============7");

    let url = format!("https://esi.evetech.net/latest/universe/types/{type_id}/");
    println!("calling url {url}");

    let response = http_client.get(&url).send().await?;

    println!(
        "response: {:?}, response code: {:?}",
        response.status(),
        response.headers()
    );

    response.parse_esi_json().await
}

pub async fn get_market_group(
    http_client: &RatelimitedClient,
    market_group_id: MarketGroupId,
) -> Result<MarketGroup, EsiError> {
    println!("============8");

    let url = format!("https://esi.evetech.net/latest/markets/groups/{market_group_id}/");
    println!("calling url {url}");

    let response = http_client.get(&url).send().await?;

    println!(
        "response: {:?}, response code: {:?}",
        response.status(),
        response.headers()
    );

    response.parse_esi_json::<MarketGroup>().await
}

pub async fn get_sell_orders(
    http_client: &RatelimitedClient,
    region_id: RegionId,
    type_id: TypeId,
    page: usize,
) -> Result<(Vec<MarketOrder>, usize), EsiError> {
    get_orders(http_client, "sell", region_id, type_id, page).await
}

pub async fn get_buy_orders(
    http_client: &RatelimitedClient,
    region_id: RegionId,
    type_id: TypeId,
    page: usize,
) -> Result<(Vec<MarketOrder>, usize), EsiError> {
    get_orders(http_client, "buy", region_id, type_id, page).await
}

async fn get_orders(
    http_client: &RatelimitedClient,
    order_type: &str,
    region_id: RegionId,
    type_id: TypeId,
    page: usize,
) -> Result<(Vec<MarketOrder>, usize), EsiError> {
    let url = format!(
        "https://esi.evetech.net/latest/markets/{region_id}/orders?order_type={order_type}&type_id={type_id}&page={page}"
    );
    println!("calling url {url}");

    let response = http_client.get(&url).send().await?;

    println!(
        "response: {:?}, response_code: {:?}",
        response.status(),
        response.headers()
    );

    let pages_str = response
        .headers()
        .get("x-pages")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("1");

    let total_pages = pages_str.parse::<usize>().unwrap_or(1);
    let orders = response.parse_esi_json::<Vec<MarketOrder>>().await?;

    Ok((orders, total_pages))
}
