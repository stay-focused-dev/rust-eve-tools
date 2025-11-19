use serde::{Deserialize, Serialize};

pub type CharacterId = u64;
pub type ItemId = i64;
pub type TypeId = i32;
pub type RegionId = i64;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ItemType {
    pub capacity: Option<f64>,
    pub description: String,
    #[serde(default)]
    pub dogma_attributes: Vec<DogmaAttributeConcise>,
    #[serde(default)]
    pub dogma_effects: Vec<DogmaEffect>,
    pub graphic_id: Option<i32>,
    pub group_id: i32,
    pub icon_id: Option<i32>,
    pub market_group_id: Option<i32>,
    pub mass: Option<f64>,
    pub name: String,
    pub packaged_volume: Option<f64>,
    pub portion_size: Option<i32>,
    pub published: bool,
    pub radius: Option<f64>,
    pub type_id: TypeId,
    pub volume: Option<f64>,
}
pub type DynamicId = (TypeId, ItemId);

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DynamicItem {
    pub created_by: i64,
    pub dogma_attributes: Vec<DogmaAttributeConcise>,
    pub dogma_effects: Vec<DogmaEffect>,
    pub mutator_type_id: i32,
    pub source_type_id: i32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DogmaAttributeConcise {
    pub attribute_id: i32,
    pub value: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DogmaEffect {
    effect_id: i32,
    is_default: bool,
}

#[derive(Deserialize, Debug)]
pub struct CharacterResponse {
    #[serde(rename = "CharacterID")]
    pub character_id: u64,
    #[serde(rename = "CharacterName")]
    pub character_name: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct AssetItem {
    pub item_id: i64,
    pub type_id: i32,
    pub location_id: i64,
    pub location_type: String,
    pub quantity: i32,
    pub location_flag: String,
    pub is_singleton: bool,
    pub is_blueprint_copy: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AssetName {
    pub item_id: i64,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Position {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Station {
    pub max_dockable_ship_volume: f64,
    pub name: String,
    pub office_rental_cost: f64,
    pub owner: Option<i32>,
    pub position: Position,
    pub race_id: Option<i32>,
    pub reprocessing_efficiency: f64,
    pub reprocessing_stations_take: f64,
    pub services: Vec<String>,
    pub station_id: i32,
    pub system_id: i32,
    pub type_id: i32,
}

pub type StationId = i32;

pub type DogmaAttributeId = i32;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DogmaAttribute {
    pub attribute_id: DogmaAttributeId,
    pub default_value: Option<f64>,
    pub description: Option<String>,
    pub display_name: Option<String>,
    pub high_is_good: Option<bool>,
    pub icon_id: Option<i32>,
    pub name: Option<String>,
    pub published: Option<bool>,
    pub stackable: Option<bool>,
    pub unit_id: Option<i32>,
}

pub type MarketGroupId = i32;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MarketGroup {
    pub description: String,
    pub market_group_id: MarketGroupId,
    pub name: String,
    pub parent_group_id: Option<MarketGroupId>,
    pub types: Vec<TypeId>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MarketOrder {
    duration: i64,
    is_buy_order: bool,
    issued: String,
    location_id: i64,
    min_volume: i64,
    order_id: i64,
    price: f64,
    range: String,
    system_id: i64,
    type_id: i64,
    volume_remain: i64,
    volume_total: i64,
}
