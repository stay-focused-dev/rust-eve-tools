// saga/assets.rs - Assets saga implementation using the framework
use std::sync::Arc;
use thiserror::Error;

use crate::db::GetData;
use crate::eve::{esi, hoboleaks, sde};
use crate::saga::framework::{Saga, SagaError, SagaProcessor};
use crate::{
    AppContext, AssetItem, AssetName, CharacterId, DogmaAttribute, DogmaAttributeId, DynamicItem,
    ItemId, ItemType, MarketGroup, MarketGroupId, Station, StationId, TypeId,
};

/// Assets-specific work types
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum AssetsWorkType {
    GetHoboleaksMutators,
    GetAssetsPage {
        character_id: CharacterId,
        page: usize,
    },
    GetAssetsNames {
        item_ids: Vec<ItemId>,
        page: usize,
        character_id: CharacterId,
    },
    GetDynamic {
        type_id: TypeId,
        item_id: ItemId,
    },
    GetType {
        type_id: TypeId,
    },
    GetMarketGroup {
        market_group_id: MarketGroupId,
    },
    GetStation {
        station_id: StationId,
    },
    GetDogmaAttribute {
        dogma_attribute_id: DogmaAttributeId,
    },
}

/// Assets-specific resolution keys
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub enum AssetsWorkKey {
    HoboleaksMutators,
    AssetsPage {
        character_id: CharacterId,
        page: usize,
    },
    AssetsNames {
        character_id: CharacterId,
        page: usize,
    },
    Dynamic {
        item_id: ItemId,
    },
    Type {
        type_id: TypeId,
    },
    MarketGroup {
        market_group_id: MarketGroupId,
    },
    Station {
        station_id: StationId,
    },
    DogmaAttribute {
        dogma_attribute_id: DogmaAttributeId,
    },
}

/// Assets-specific work results
#[derive(Clone)]
pub enum AssetsWorkResult {
    HoboleaksMutators {
        data: hoboleaks::MutaplasmidData,
    },
    AssetsPage {
        character_id: CharacterId,
        page: usize,
        total_pages: usize,
        assets: Vec<AssetItem>,
    },
    AssetsNames {
        character_id: CharacterId,
        page: usize,
        assets_names: Vec<AssetName>,
    },
    Dynamic {
        type_id: TypeId,
        item_id: ItemId,
        dynamic: DynamicItem,
    },
    Type {
        type_id: TypeId,
        item_type: ItemType,
    },
    MarketGroup {
        market_group_id: MarketGroupId,
        market_group: MarketGroup,
    },
    Station {
        station_id: StationId,
        station: Station,
    },
    DogmaAttribute {
        dogma_attribute_id: DogmaAttributeId,
        dogma_attribute: DogmaAttribute,
    },
}

#[derive(Debug, Error)]
pub enum AssetsError {
    #[error("ESI client error: {0}")]
    EsiError(String),
    #[error("SDE client error: {0}")]
    SdeError(String),
    #[error("Database error: {0}")]
    DatabaseError(String),
    #[error("Consistency error: {0}")]
    ConsistencyError(String),
}

/// Initial event for assets saga
pub struct AssetsInitialEvent {
    pub character_id: CharacterId,
}

/// Assets saga processor implementation
pub struct AssetsSagaProcessor;

impl Clone for AssetsSagaProcessor {
    fn clone(&self) -> Self {
        AssetsSagaProcessor
    }
}

impl SagaProcessor for AssetsSagaProcessor {
    type WorkType = AssetsWorkType;
    type WorkKey = AssetsWorkKey;
    type WorkResult = AssetsWorkResult;
    type Error = AssetsError;
    type Context = AppContext;
    type InitialEvent = AssetsInitialEvent;

    fn to_resolution_key(work_type: &Self::WorkType) -> Self::WorkKey {
        match work_type {
            AssetsWorkType::GetHoboleaksMutators => AssetsWorkKey::HoboleaksMutators,
            AssetsWorkType::GetAssetsPage { character_id, page } => AssetsWorkKey::AssetsPage {
                character_id: *character_id,
                page: *page,
            },
            AssetsWorkType::GetAssetsNames {
                character_id, page, ..
            } => AssetsWorkKey::AssetsNames {
                character_id: *character_id,
                page: *page,
            },
            AssetsWorkType::GetDynamic { item_id, .. } => {
                AssetsWorkKey::Dynamic { item_id: *item_id }
            }
            AssetsWorkType::GetType { type_id } => AssetsWorkKey::Type { type_id: *type_id },
            AssetsWorkType::GetMarketGroup { market_group_id } => AssetsWorkKey::MarketGroup {
                market_group_id: *market_group_id,
            },
            AssetsWorkType::GetStation { station_id } => AssetsWorkKey::Station {
                station_id: *station_id,
            },
            AssetsWorkType::GetDogmaAttribute { dogma_attribute_id } => {
                AssetsWorkKey::DogmaAttribute {
                    dogma_attribute_id: *dogma_attribute_id,
                }
            }
        }
    }

    fn handle_initial_event(
        event: Self::InitialEvent,
    ) -> Result<Vec<Self::WorkType>, SagaError<Self::Error>> {
        Ok(vec![
            AssetsWorkType::GetHoboleaksMutators,
            AssetsWorkType::GetAssetsPage {
                character_id: event.character_id,
                page: 1,
            },
        ])
    }

    async fn process(
        context: &Arc<Self::Context>,
        work_type: &Self::WorkType,
    ) -> Result<Self::WorkResult, Self::Error> {
        match work_type {
            AssetsWorkType::GetHoboleaksMutators => {
                let data = hoboleaks::get_mutaplasmids(&context.http_client)
                    .await
                    .map_err(|e| AssetsError::EsiError(e.to_string()))?;

                Ok(AssetsWorkResult::HoboleaksMutators { data })
            }
            AssetsWorkType::GetAssetsPage { character_id, page } => {
                let characters_guard = context.characters.lock().await;
                let character_client =
                    characters_guard
                        .get(*character_id)
                        .ok_or(AssetsError::ConsistencyError(format!(
                            "unknown character with id: {character_id}"
                        )))?;

                let (assets, total_pages) = esi::get_assets_chunk(
                    &context.http_client,
                    &character_client.oauth_token,
                    *character_id,
                    *page,
                )
                .await
                .map_err(|e| AssetsError::EsiError(e.to_string()))?;

                Ok(AssetsWorkResult::AssetsPage {
                    character_id: *character_id,
                    page: *page,
                    assets,
                    total_pages,
                })
            }
            AssetsWorkType::GetAssetsNames {
                character_id,
                item_ids,
                page,
            } => {
                let characters_guard = context.characters.lock().await;
                let character_client =
                    characters_guard
                        .get(*character_id)
                        .ok_or(AssetsError::ConsistencyError(format!(
                            "unknown character with id: {character_id}"
                        )))?;

                let assets_names = esi::get_assets_names(
                    &context.http_client,
                    &character_client.oauth_token,
                    *character_id,
                    item_ids,
                )
                .await
                .map_err(|e| AssetsError::EsiError(e.to_string()))?;

                Ok(AssetsWorkResult::AssetsNames {
                    assets_names,
                    page: *page,
                    character_id: *character_id,
                })
            }
            AssetsWorkType::GetDynamic { type_id, item_id } => {
                let cached_dynamic = {
                    let dynamics_db = context.dynamics_db.read().await;
                    dynamics_db.get((*type_id, *item_id)).cloned()
                };

                let dynamic = match cached_dynamic {
                    Some(d) => d,
                    None => {
                        let dynamic = esi::get_dynamic_item_attributes(
                            &context.http_client,
                            *item_id,
                            *type_id,
                        )
                        .await
                        .map_err(|e| AssetsError::EsiError(e.to_string()))?;

                        {
                            let mut dynamics_db = context.dynamics_db.write().await;
                            dynamics_db.add((*type_id, *item_id), dynamic.clone());
                        }

                        dynamic
                    }
                };

                Ok(AssetsWorkResult::Dynamic {
                    type_id: *type_id,
                    item_id: *item_id,
                    dynamic,
                })
            }
            AssetsWorkType::GetType { type_id } => {
                let cached_item_type = {
                    let type_ids = vec![*type_id];
                    let mut res = sde::get_types_by_ids(&context.sde_pool, &type_ids)
                        .await
                        .map_err(|e| AssetsError::SdeError(e.to_string()))?;
                    res.pop()
                };

                let item_type = match cached_item_type {
                    Some(item_type) => {
                        println!("found type in sde: {}", type_id);
                        item_type
                    }
                    None => esi::get_type(&context.http_client, *type_id)
                        .await
                        .map_err(|e| AssetsError::EsiError(e.to_string()))?,
                };

                Ok(AssetsWorkResult::Type {
                    type_id: *type_id,
                    item_type,
                })
            }
            AssetsWorkType::GetMarketGroup { market_group_id } => {
                let cached_market_group = {
                    let market_group_ids = vec![*market_group_id];
                    let mut res =
                        sde::get_market_groups_by_ids(&context.sde_pool, &market_group_ids)
                            .await
                            .map_err(|e| AssetsError::SdeError(e.to_string()))?;
                    res.pop()
                };

                let market_group = match cached_market_group {
                    Some(market_group) => {
                        println!("found market group in sde: {}", market_group_id);
                        market_group
                    }
                    None => esi::get_market_group(&context.http_client, *market_group_id)
                        .await
                        .map_err(|e| AssetsError::EsiError(e.to_string()))?,
                };

                Ok(AssetsWorkResult::MarketGroup {
                    market_group_id: *market_group_id,
                    market_group,
                })
            }
            AssetsWorkType::GetStation { station_id } => {
                let station = esi::get_station(&context.http_client, *station_id)
                    .await
                    .map_err(|e| AssetsError::EsiError(e.to_string()))?;

                Ok(AssetsWorkResult::Station {
                    station_id: *station_id,
                    station,
                })
            }
            AssetsWorkType::GetDogmaAttribute { dogma_attribute_id } => {
                let cached_dogma_attribute = {
                    let dogma_attribute_ids = vec![*dogma_attribute_id];
                    let mut res =
                        sde::get_dogma_attributes_by_ids(&context.sde_pool, &dogma_attribute_ids)
                            .await
                            .map_err(|e| AssetsError::SdeError(e.to_string()))?;
                    res.pop()
                };

                let dogma_attribute = match cached_dogma_attribute {
                    Some(dogma_attribute) => {
                        println!("found dogma attribute in sde: {}", dogma_attribute_id);
                        dogma_attribute
                    }
                    None => esi::get_dogma_attribute(&context.http_client, *dogma_attribute_id)
                        .await
                        .map_err(|e| AssetsError::EsiError(e.to_string()))?,
                };

                Ok(AssetsWorkResult::DogmaAttribute {
                    dogma_attribute_id: *dogma_attribute_id,
                    dogma_attribute,
                })
            }
        }
    }

    async fn handle(
        context: &Arc<Self::Context>,
        work_result: Self::WorkResult,
    ) -> Result<Vec<Self::WorkType>, Self::Error> {
        let mut new_items = vec![];

        match work_result {
            AssetsWorkResult::HoboleaksMutators { data } => {
                for (mutator_type_id, mutator_data) in data {
                    let attributes = mutator_data
                        .attribute_i_ds
                        .iter()
                        .map(|(attribute_id, range)| (*attribute_id, range.min, range.max))
                        .collect();

                    let input_output = mutator_data
                        .input_output_mapping
                        .iter()
                        .map(|i| (i.resulting_type, i.applicable_types.clone()))
                        .collect();

                    let new_data = context
                        .character_assets_db
                        .add_mutaplasmid_effects(mutator_type_id, attributes, input_output)
                        .map_err(|e| {
                            AssetsError::DatabaseError(format!(
                                "Error adding mutaplasmid effects: {}",
                                e
                            ))
                        })?;

                    for item in new_data {
                        new_items.push(get_data_to_work_type(&item));
                    }
                }
            }
            AssetsWorkResult::AssetsPage {
                character_id,
                page,
                total_pages,
                assets,
            } => {
                for asset in &assets {
                    let new_data = context
                        .character_assets_db
                        .add_asset(asset.clone())
                        .map_err(|e| {
                            AssetsError::DatabaseError(format!("unable to store asset {e}"))
                        })?;

                    for item in new_data {
                        new_items.push(get_data_to_work_type(&item));
                    }
                }

                if page == 1 {
                    for page in 2..=total_pages {
                        new_items.push(AssetsWorkType::GetAssetsPage { character_id, page });
                    }
                }

                let item_ids = assets.iter().map(|asset| asset.item_id).collect();
                new_items.push(AssetsWorkType::GetAssetsNames {
                    character_id,
                    page,
                    item_ids,
                });
            }
            AssetsWorkResult::AssetsNames { assets_names, .. } => {
                for asset_name in assets_names {
                    context
                        .character_assets_db
                        .add_asset_name(asset_name.item_id, asset_name.name.clone())
                        .map_err(|e| {
                            AssetsError::DatabaseError(format!("unable to store asset name {e}"))
                        })?;
                }
            }
            AssetsWorkResult::Dynamic {
                type_id,
                item_id,
                dynamic,
            } => {
                let new_data = context
                    .character_assets_db
                    .add_dynamic(type_id, item_id, dynamic)
                    .map_err(|e| {
                        AssetsError::DatabaseError(format!("unable to store dynamic {e}"))
                    })?;

                for item in new_data {
                    new_items.push(get_data_to_work_type(&item));
                }
            }
            AssetsWorkResult::Type { item_type, .. } => {
                let new_data = context
                    .character_assets_db
                    .add_type(item_type)
                    .map_err(|e| AssetsError::DatabaseError(format!("unable to store type {e}")))?;

                for item in new_data {
                    new_items.push(get_data_to_work_type(&item));
                }
            }
            AssetsWorkResult::MarketGroup { market_group, .. } => {
                let new_data = context
                    .character_assets_db
                    .add_market_group(market_group)
                    .map_err(|e| {
                        AssetsError::DatabaseError(format!("unable to store market group {e}"))
                    })?;

                for item in new_data {
                    new_items.push(get_data_to_work_type(&item));
                }
            }
            AssetsWorkResult::Station {
                station_id,
                station,
            } => {
                let new_data = context
                    .character_assets_db
                    .add_station(station_id, station)
                    .map_err(|e| {
                        AssetsError::DatabaseError(format!("unable to store station {e}"))
                    })?;

                for item in new_data {
                    new_items.push(get_data_to_work_type(&item));
                }
            }
            AssetsWorkResult::DogmaAttribute {
                dogma_attribute, ..
            } => {
                let new_data = context
                    .character_assets_db
                    .add_dogma_attribute(dogma_attribute)
                    .map_err(|e| {
                        AssetsError::DatabaseError(format!("unable to store dogma attribute {e}"))
                    })?;

                for item in new_data {
                    new_items.push(get_data_to_work_type(&item));
                }
            }
        }

        Ok(new_items)
    }
}

// Helper function to convert GetData to WorkType
fn get_data_to_work_type(get_data: &GetData) -> AssetsWorkType {
    match get_data {
        GetData::Dynamic(type_id, item_id) => AssetsWorkType::GetDynamic {
            type_id: *type_id,
            item_id: *item_id,
        },
        GetData::MarketGroup(market_group_id) => AssetsWorkType::GetMarketGroup {
            market_group_id: *market_group_id,
        },
        GetData::Station(station_id) => AssetsWorkType::GetStation {
            station_id: *station_id,
        },
        GetData::Type(type_id) => AssetsWorkType::GetType { type_id: *type_id },
        GetData::DogmaAttribute(dogma_attribute_id) => AssetsWorkType::GetDogmaAttribute {
            dogma_attribute_id: *dogma_attribute_id,
        },
    }
}

// Convenience type alias
pub type AssetsSaga = Saga<AssetsSagaProcessor>;

// Usage example:
pub async fn run_assets_saga(
    context: Arc<AppContext>,
    character_id: CharacterId,
    workers_count: usize,
) -> Result<(), SagaError<AssetsError>> {
    let saga = AssetsSaga::new(context, workers_count);
    saga.start_with_event(AssetsInitialEvent { character_id })
        .await
}
