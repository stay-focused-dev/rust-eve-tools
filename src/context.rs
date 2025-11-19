use oauth2::basic::BasicTokenResponse;
use sqlx::sqlite::SqlitePool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

use crate::eve::hoboleaks::{self, MutaplasmidData};
use crate::{AllAssetsDb, CharacterAssetsDb, CharacterId, DynamicsDb, RatelimitedClient};

// OAuth2 client type - adjust based on your actual oauth2 setup
type ClientWithAuthAndTokenUrl = oauth2::basic::BasicClient<
    oauth2::EndpointSet,    // HasAuthUrl
    oauth2::EndpointNotSet, // HasDeviceAuthUrl
    oauth2::EndpointNotSet, // HasIntrospectionUrl
    oauth2::EndpointNotSet, // HasRevocationUrl
    oauth2::EndpointSet,    // HasTokenUrl
>;

pub struct AppContext {
    pub sde_pool: SqlitePool,
    pub http_client: Arc<RatelimitedClient>,
    pub oauth2_client: Arc<ClientWithAuthAndTokenUrl>,
    pub dynamics_db: RwLock<DynamicsDb>,
    pub assets_db: RwLock<AllAssetsDb>,
    pub character_assets_db: CharacterAssetsDb,
    pub data_dir: String,
    pub characters: Mutex<CharacterManager>,

    // Hoboleaks cache
    pub hoboleaks_data: Arc<tokio::sync::RwLock<Option<MutaplasmidData>>>,
    pub hoboleaks_last_fetch: Arc<tokio::sync::RwLock<Option<std::time::Instant>>>,
}

impl AppContext {
    pub async fn with_client(
        http_client: Arc<RatelimitedClient>,
        oauth2_client: Arc<ClientWithAuthAndTokenUrl>,
        sde_path: &str,
        data_dir: &str,
    ) -> anyhow::Result<Self> {
        let sde_pool = crate::eve::sde::create_conn_pool(sde_path).await?;
        let abyssal_items = crate::eve::sde::get_abyssal_modules(&sde_pool).await?;

        let dynamics_db = RwLock::new(DynamicsDb::from_dir(data_dir)?);
        let assets_db = RwLock::new(AllAssetsDb::from_dir(data_dir)?);
        let data_dir = data_dir.to_string();
        let characters = Mutex::new(CharacterManager::new());
        let character_assets_db = CharacterAssetsDb::from_dir(&data_dir.clone(), abyssal_items)?;

        Ok(Self {
            sde_pool,
            http_client,
            oauth2_client,
            dynamics_db,
            assets_db,
            data_dir,
            characters,
            character_assets_db,
            hoboleaks_data: Arc::new(RwLock::new(None)),
            hoboleaks_last_fetch: Arc::new(RwLock::new(None)),
        })
    }

    /// Get hoboleaks data with caching (cache for 1 hour)
    pub async fn get_hoboleaks_data(
        &self,
    ) -> Result<Option<MutaplasmidData>, hoboleaks::HoboleaksError> {
        const CACHE_DURATION: std::time::Duration = std::time::Duration::from_secs(3600); // 1 hour

        // Check if we have recent cached data
        {
            let last_fetch = self.hoboleaks_last_fetch.read().await;
            if let Some(last_time) = *last_fetch {
                if last_time.elapsed() < CACHE_DURATION {
                    let cached_data = self.hoboleaks_data.read().await;
                    if let Some(ref data) = *cached_data {
                        println!(
                            "✅ Using cached hoboleaks data (age: {:?})",
                            last_time.elapsed()
                        );
                        return Ok(Some(data.clone()));
                    }
                }
            }
        }

        // Fetch fresh data
        println!("🔄 Fetching fresh hoboleaks data...");
        match hoboleaks::get_mutaplasmids(&self.http_client).await {
            Ok(data) => {
                // Update cache
                {
                    let mut cached_data = self.hoboleaks_data.write().await;
                    *cached_data = Some(data.clone());
                }
                {
                    let mut last_fetch = self.hoboleaks_last_fetch.write().await;
                    *last_fetch = Some(std::time::Instant::now());
                }

                println!("✅ Successfully fetched and cached hoboleaks data");

                Ok(Some(data))
            }
            Err(e) => {
                println!("❌ Failed to fetch hoboleaks data: {}", e);

                // Try to return stale cached data if available
                let cached_data = self.hoboleaks_data.read().await;
                if let Some(ref data) = *cached_data {
                    println!("⚠️  Using stale cached hoboleaks data as fallback");
                    Ok(Some(data.clone()))
                } else {
                    Err(e)
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct CharacterClient {
    pub character_id: u64,
    pub character_name: String,
    pub oauth_token: BasicTokenResponse,
}

impl CharacterClient {
    pub fn new(character_id: u64, character_name: String, oauth_token: BasicTokenResponse) -> Self {
        Self {
            character_id,
            character_name,
            oauth_token,
        }
    }
}

pub struct CharacterManager {
    characters: HashMap<CharacterId, CharacterClient>,
}

impl CharacterManager {
    pub fn new() -> Self {
        Self {
            characters: HashMap::new(),
        }
    }

    pub fn add(&mut self, character: CharacterClient) {
        self.characters.insert(character.character_id, character);
    }

    pub fn get(&self, character_id: CharacterId) -> Option<&CharacterClient> {
        self.characters.get(&character_id)
    }

    pub fn list(&self) -> Vec<&CharacterClient> {
        self.characters.values().collect()
    }
}

#[derive(Clone)]
pub struct OauthConfig {
    pub client_id: oauth2::ClientId,
    pub auth_url: oauth2::AuthUrl,
    pub token_url: oauth2::TokenUrl,
    pub redirect_url: oauth2::RedirectUrl,
}
