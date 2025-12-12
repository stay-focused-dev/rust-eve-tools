#![allow(dead_code)]
use crate::{
    AssetItem, DogmaAttribute, DogmaAttributeId, DynamicItem, ItemId, ItemType, MarketGroup,
    MarketGroupId, Station, StationId, TypeId,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_cbor;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::RwLock;
use std::time::{Instant, Duration};
use std::sync::Arc;


#[derive(Default)]
pub struct ChainStats {
    pub direct_station: usize,
    pub lookups: usize,
    pub max_depth: u32,
    pub total_depth: u32,
    pub total_calls: u32,
}

impl ChainStats {
    pub fn avg_depth(&self) -> f64 {
        if self.total_calls == 0 {
            0.0
        } else {
            self.total_depth as f64 / self.total_calls as f64
        }
    }

    pub fn print_summary(&self) {
        println!("=== Chain Stats Summary ===");
        println!("Total calls: {}", self.total_calls);
        println!("Direct stations: {}", self.direct_station);
        println!("Total lookups: {}", self.lookups);
        println!("Max depth: {}", self.max_depth);
        println!("Average depth: {:.2}", self.avg_depth());
    }
}

#[derive(Default)]
pub struct ChainTimings {
    pub cache_hit: Duration,
    pub cache_lookup: Duration,
    pub asset_lookup: Duration,
    pub name_lookup: Duration,
    pub station_lookup: Duration,
    pub string_ops: Duration,
    pub arc_creation: Duration,
    pub total: Duration,
}

impl ChainTimings {
    pub fn print_breakdown(&self) {
        println!("=== Chain Timings Breakdown ===");
        let total_us = self.total.as_micros() as f64;
        println!("Total:          {:?} (100.0%)", self.total);
        println!("  Cache hits:   {:?} ({:.1}%)", self.cache_hit, self.cache_hit.as_micros() as f64 / total_us * 100.0);
        println!("  Cache lookup: {:?} ({:.1}%)", self.cache_lookup, self.cache_lookup.as_micros() as f64 / total_us * 100.0);
        println!("  Asset lookup: {:?} ({:.1}%)", self.asset_lookup, self.asset_lookup.as_micros() as f64 / total_us * 100.0);
        println!("  Name lookup:  {:?} ({:.1}%)", self.name_lookup, self.name_lookup.as_micros() as f64 / total_us * 100.0);
        println!("  Station lookup:{:?} ({:.1}%)", self.station_lookup, self.station_lookup.as_micros() as f64 / total_us * 100.0);
        println!("  String ops:   {:?} ({:.1}%)", self.string_ops, self.string_ops.as_micros() as f64 / total_us * 100.0);
        println!("  Arc creation: {:?} ({:.1}%)", self.arc_creation, self.arc_creation.as_micros() as f64 / total_us * 100.0);
    }    
}

pub struct CharacterAssets {
    pub assets: RwLock<BTreeMap<ItemId, AssetItem>>,
    pub assets_names: RwLock<BTreeMap<ItemId, String>>,
    pub stations: RwLock<BTreeMap<StationId, Station>>,
    pub dynamics: RwLock<BTreeMap<ItemId, DynamicItem>>,
    pub dogma_attributes: RwLock<BTreeMap<DogmaAttributeId, DogmaAttribute>>,
    pub dogma_attributes_name_to_id: RwLock<BTreeMap<String, DogmaAttributeId>>,
    pub types: RwLock<BTreeMap<TypeId, ItemType>>,
    pub market_groups: RwLock<BTreeMap<MarketGroupId, MarketGroup>>,
    pub abyssal_items: RwLock<BTreeSet<TypeId>>,
    pub mutaplasmid_effects: RwLock<MutaplasmidEffects>,
}

#[derive(PartialEq, Hash, Eq, Clone, Ord, PartialOrd, Debug)]
pub enum GetData {
    Dynamic(TypeId, ItemId),
    MarketGroup(MarketGroupId),
    Station(StationId),
    Type(TypeId),
    DogmaAttribute(DogmaAttributeId),
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct MutaplasmidEffects {
    // source_type_id => mutator_type_id => resulting_type_id
    source_to_mutator_to_resulting: BTreeMap<TypeId, BTreeMap<TypeId, TypeId>>,
    // resulting_type_id => [applicable_type_id, ...]
    resulting_to_applicable: BTreeMap<TypeId, BTreeSet<TypeId>>,
    //
    resulting_to_mutator_to_source: BTreeMap<TypeId, BTreeMap<TypeId, BTreeSet<TypeId>>>,
    // mutator_id => attributes
    attributes: BTreeMap<TypeId, BTreeMap<DogmaAttributeId, AttributeRange>>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct AttributeRange {
    pub max: f64,
    pub min: f64,
}

impl Clone for CharacterAssets {
    fn clone(&self) -> Self {
        CharacterAssets {
            assets: RwLock::new(self.assets.read().unwrap().clone()),
            assets_names: RwLock::new(self.assets_names.read().unwrap().clone()),
            stations: RwLock::new(self.stations.read().unwrap().clone()),
            dynamics: RwLock::new(self.dynamics.read().unwrap().clone()),
            types: RwLock::new(self.types.read().unwrap().clone()),
            dogma_attributes: RwLock::new(self.dogma_attributes.read().unwrap().clone()),
            dogma_attributes_name_to_id: RwLock::new(
                self.dogma_attributes_name_to_id.read().unwrap().clone(),
            ),
            market_groups: RwLock::new(self.market_groups.read().unwrap().clone()),
            abyssal_items: RwLock::new(self.abyssal_items.read().unwrap().clone()),
            mutaplasmid_effects: RwLock::new(self.mutaplasmid_effects.read().unwrap().clone()),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct SerializableCharacterAssets {
    assets: BTreeMap<ItemId, AssetItem>,
    assets_names: BTreeMap<ItemId, String>,
    stations: BTreeMap<StationId, Station>,
    dynamics: BTreeMap<ItemId, DynamicItem>,
    dogma_attributes: BTreeMap<DogmaAttributeId, DogmaAttribute>,
    dogma_attributes_name_to_id: BTreeMap<String, DogmaAttributeId>,
    types: BTreeMap<TypeId, ItemType>,
    market_groups: BTreeMap<MarketGroupId, MarketGroup>,
    abyssal_items: BTreeSet<TypeId>,
    mutaplasmid_effects: MutaplasmidEffects,
}

impl Serialize for CharacterAssets {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let assets = self.assets.read().map_err(serde::ser::Error::custom)?;
        let assets_names = self
            .assets_names
            .read()
            .map_err(serde::ser::Error::custom)?;
        let stations = self.stations.read().map_err(serde::ser::Error::custom)?;
        let dynamics = self.dynamics.read().map_err(serde::ser::Error::custom)?;
        let dogma_attributes = self
            .dogma_attributes
            .read()
            .map_err(serde::ser::Error::custom)?;
        let dogma_attributes_name_to_id = self
            .dogma_attributes_name_to_id
            .read()
            .map_err(serde::ser::Error::custom)?;
        let types = self.types.read().map_err(serde::ser::Error::custom)?;
        let market_groups = self
            .market_groups
            .read()
            .map_err(serde::ser::Error::custom)?;
        let abyssal_items = self
            .abyssal_items
            .read()
            .map_err(serde::ser::Error::custom)?;
        let mutaplasmid_effects = self
            .mutaplasmid_effects
            .read()
            .map_err(serde::ser::Error::custom)?;

        let serializable = SerializableCharacterAssets {
            assets: assets.clone(),
            assets_names: assets_names.clone(),
            stations: stations.clone(),
            dynamics: dynamics.clone(),
            dogma_attributes: dogma_attributes.clone(),
            dogma_attributes_name_to_id: dogma_attributes_name_to_id.clone(),
            types: types.clone(),
            market_groups: market_groups.clone(),
            abyssal_items: abyssal_items.clone(),
            mutaplasmid_effects: mutaplasmid_effects.clone(),
        };

        serializable.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CharacterAssets {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let serializable = SerializableCharacterAssets::deserialize(deserializer)?;
        Ok(CharacterAssets {
            assets: RwLock::new(serializable.assets),
            assets_names: RwLock::new(serializable.assets_names),
            stations: RwLock::new(serializable.stations),
            dynamics: RwLock::new(serializable.dynamics),
            dogma_attributes: RwLock::new(serializable.dogma_attributes),
            dogma_attributes_name_to_id: RwLock::new(serializable.dogma_attributes_name_to_id),
            types: RwLock::new(serializable.types),
            market_groups: RwLock::new(serializable.market_groups),
            abyssal_items: RwLock::new(serializable.abyssal_items),
            mutaplasmid_effects: RwLock::new(serializable.mutaplasmid_effects),
        })
    }
}

impl CharacterAssets {
    pub fn new(abyssal_items: Vec<TypeId>) -> Self {
        CharacterAssets {
            assets: RwLock::new(BTreeMap::new()),
            assets_names: RwLock::new(BTreeMap::new()),
            stations: RwLock::new(BTreeMap::new()),
            dynamics: RwLock::new(BTreeMap::new()),
            dogma_attributes: RwLock::new(BTreeMap::new()),
            dogma_attributes_name_to_id: RwLock::new(BTreeMap::new()),
            types: RwLock::new(BTreeMap::new()),
            market_groups: RwLock::new(BTreeMap::new()),
            abyssal_items: RwLock::new(BTreeSet::from_iter(abyssal_items)),
            mutaplasmid_effects: RwLock::new(MutaplasmidEffects::default()),
        }
    }

    pub fn add_mutaplasmid_effects(
        &self,
        mutator_type_id: TypeId,
        attributes: Vec<(DogmaAttributeId, f64, f64)>,
        input_output: Vec<(TypeId, Vec<TypeId>)>, // [(resulting_type, [applicable_types]), ...]
    ) -> Result<Vec<GetData>, String> {
        let mut mutaplasmid_effects = self
            .mutaplasmid_effects
            .write()
            .map_err(|e| format!("Failed to acquire write lock: {}", e))?;

        // println!("DEBUG: mutator_type_id {}", mutator_type_id);
        let mut new_items = BTreeSet::new();
        for (resulting_type_id, source_type_ids) in input_output {
            for source_type_id in source_type_ids {
                mutaplasmid_effects
                    .source_to_mutator_to_resulting
                    .entry(source_type_id)
                    .or_default()
                    .entry(mutator_type_id)
                    .or_insert(resulting_type_id);

                mutaplasmid_effects
                    .resulting_to_applicable
                    .entry(resulting_type_id)
                    .or_default()
                    .insert(source_type_id);

                mutaplasmid_effects
                    .resulting_to_mutator_to_source
                    .entry(resulting_type_id)
                    .or_default()
                    .entry(mutator_type_id)
                    .or_default()
                    .insert(source_type_id);

                // println!("    DEBUG: type {}", source_type_id);
                new_items.insert(GetData::Type(source_type_id));
            }
        }

        for (attribute_id, min, max) in attributes {
            mutaplasmid_effects
                .attributes
                .entry(mutator_type_id)
                .or_default()
                .entry(attribute_id)
                .or_insert(AttributeRange { min, max });
        }

        Ok(new_items.into_iter().collect())
    }

    pub fn get_mutators_by_resulting_type_id(
        &self,
        resulting_type_id: &TypeId,
    ) -> Result<BTreeMap<(TypeId, String), BTreeMap<DogmaAttributeId, AttributeRange>>, String>
    {
        let mutaplasmid_effects = self
            .mutaplasmid_effects
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;

        let types = self
            .types
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;

        let mut res = BTreeMap::new();

        if let Some(mutator_to_source) = mutaplasmid_effects
            .resulting_to_mutator_to_source
            .get(resulting_type_id)
        {
            for (mutator_type_id, _) in mutator_to_source {
                let mutator_type = types.get(mutator_type_id).unwrap();

                let attributes = mutaplasmid_effects.attributes.get(mutator_type_id).unwrap();
                res.entry((*mutator_type_id, mutator_type.name.clone()))
                    .or_insert_with(|| attributes.clone());
            }
        }

        Ok(res)
    }

    pub fn get_min_max_attributes_by_resulting_type_id(
        &self,
        resulting_type_id: &TypeId,
    ) -> Result<BTreeMap<DogmaAttributeId, AttributeRange>, String> {
        let mutaplasmid_effects = self
            .mutaplasmid_effects
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;

        let types = self
            .types
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;

        let mut min_max_attributes: BTreeMap<DogmaAttributeId, AttributeRange> = BTreeMap::new();

        for (mutator_type_id, source_type_ids) in mutaplasmid_effects
            .resulting_to_mutator_to_source
            .get(resulting_type_id)
            .unwrap_or(&BTreeMap::new())
        {
            let mutator_attributes = mutaplasmid_effects.attributes.get(mutator_type_id).unwrap();

            for source_type_id in source_type_ids {
                let source_type = types.get(source_type_id).unwrap();

                for attribute in &source_type.dogma_attributes {
                    if let Some(attr_range) = mutator_attributes.get(&attribute.attribute_id) {
                        let v1 = attr_range.min * attribute.value;
                        let v2 = attr_range.max * attribute.value;

                        let new_min = v1.min(v2);
                        let new_max = v1.max(v2);

                        match min_max_attributes.get(&attribute.attribute_id) {
                            Some(attr_range) => {
                                let min = new_min.min(attr_range.min);
                                let max = new_max.max(attr_range.max);

                                min_max_attributes.insert(
                                    attribute.attribute_id,
                                    AttributeRange { min: min, max: max },
                                );
                            }
                            None => {
                                min_max_attributes.insert(
                                    attribute.attribute_id,
                                    AttributeRange {
                                        min: new_min,
                                        max: new_max,
                                    },
                                );
                            }
                        }
                    }
                }
            }
        }
        Ok(min_max_attributes)
    }

    pub fn get_attributes_by_mutator_type_id(
        &self,
        mutator_type_id: &TypeId,
    ) -> Result<BTreeMap<DogmaAttributeId, AttributeRange>, String> {
        let mutaplasmid_effects = self
            .mutaplasmid_effects
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;

        let r = mutaplasmid_effects
            .attributes
            .get(mutator_type_id)
            .ok_or_else(|| format!("Mutator type ID {} not found", mutator_type_id))?;

        Ok(r.clone())
    }

    pub fn get_resulting_type_by_source_mutator(
        &self,
        source_type_id: TypeId,
        mutator_type_id: TypeId,
    ) -> Result<TypeId, String> {
        let mutaplasmid_effects = self
            .mutaplasmid_effects
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;

        let resulting_type = mutaplasmid_effects
            .source_to_mutator_to_resulting
            .get(&source_type_id)
            .ok_or_else(|| format!("source type id {} not found", source_type_id))?
            .get(&mutator_type_id)
            .ok_or_else(|| format!("mutator type id {} not found", mutator_type_id))?;

        Ok(resulting_type.clone())
    }

    pub fn get_attribute_ids_by_mutator(
        &self,
        mutator_type_id: &TypeId,
    ) -> Result<BTreeSet<DogmaAttributeId>, String> {
        let mutaplasmid_effects = self
            .mutaplasmid_effects
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;

        let attributes = mutaplasmid_effects
            .attributes
            .get(mutator_type_id)
            .ok_or_else(|| format!("mutator type id {} not found", mutator_type_id))?;

        Ok(attributes.keys().cloned().collect())
    }

    pub fn get_applicable_types_by_resulting_type(
        &self,
        resulting_type_id: &TypeId,
    ) -> Result<BTreeSet<TypeId>, String> {
        let mutaplasmid_effects = self
            .mutaplasmid_effects
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;

        let applicable_types = mutaplasmid_effects
            .resulting_to_applicable
            .get(resulting_type_id)
            .ok_or_else(|| format!("resulting type id {} not found", resulting_type_id))?;

        Ok(applicable_types.clone())
    }

    pub fn add_asset(&self, asset: AssetItem) -> Result<Vec<GetData>, String> {
        {
            let mut assets = self
                .assets
                .write()
                .map_err(|e| format!("Failed to acquire write lock: {}", e))?;
            assets.insert(asset.item_id, asset.clone());
        }

        let mut new_items = vec![];

        if self.is_on_station(&asset) {
            let station_id = asset.location_id as StationId;
            let stations = self
                .stations
                .read()
                .map_err(|e| format!("Failed to acquire read lock: {}", e))?;
            if !stations.contains_key(&station_id) {
                new_items.push(GetData::Station(station_id));
            }
        }

        if self.is_abyssal(&asset)? {
            let dynamics = self
                .dynamics
                .read()
                .map_err(|e| format!("Failed to acquire read lock: {}", e))?;
            if !dynamics.contains_key(&asset.item_id) {
                new_items.push(GetData::Dynamic(asset.type_id, asset.item_id));
            }
        }

        {
            let types = self
                .types
                .read()
                .map_err(|e| format!("Failed to acquire read lock: {}", e))?;
            if !types.contains_key(&asset.type_id) {
                new_items.push(GetData::Type(asset.type_id));
            }
        }

        Ok(new_items)
    }

    pub fn add_asset_name(&self, asset_id: ItemId, name: String) -> Result<Vec<GetData>, String> {
        let mut assets_names = self
            .assets_names
            .write()
            .map_err(|e| format!("Failed to acquire write lock: {}", e))?;
        assets_names.insert(asset_id, name);
        Ok(vec![])
    }

    pub fn add_station(
        &self,
        station_id: StationId,
        station: Station,
    ) -> Result<Vec<GetData>, String> {
        {
            let mut stations = self
                .stations
                .write()
                .map_err(|e| format!("Failed to acquire write lock: {}", e))?;
            stations.insert(station_id, station);
        }

        Ok(vec![])
    }

    pub fn add_dogma_attribute(
        &self,
        dogma_attribute: DogmaAttribute,
    ) -> Result<Vec<GetData>, String> {
        let attribute_id = dogma_attribute.attribute_id;
        let attribute_name = dogma_attribute.name.clone();

        {
            let mut dogma_attributes = self
                .dogma_attributes
                .write()
                .map_err(|e| format!("Failed to acquire write lock: {}", e))?;
            dogma_attributes.insert(attribute_id, dogma_attribute);

            let mut dogma_attributes_name_to_id = self
                .dogma_attributes_name_to_id
                .write()
                .map_err(|e| format!("Failed to acquire write lock: {}", e))?;

            let name = attribute_name.unwrap_or_else(|| format!("attribute_{}", attribute_id));
            dogma_attributes_name_to_id.insert(name, attribute_id);
        }

        Ok(vec![])
    }

    pub fn get_attribute_id_by_name(&self, name: String) -> Result<DogmaAttributeId, String> {
        let dogma_attributes_name_to_id = self
            .dogma_attributes_name_to_id
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;

        dogma_attributes_name_to_id
            .get(&name)
            .cloned()
            .ok_or_else(|| format!("Attribute '{}' not found", name))
    }

    pub fn add_type(&self, item_type: ItemType) -> Result<Vec<GetData>, String> {
        let type_id = item_type.type_id;
        let maybe_market_group_id = item_type.market_group_id;

        {
            let mut types = self
                .types
                .write()
                .map_err(|e| format!("Failed to acquire write lock: {}", e))?;
            types.insert(type_id, item_type);
        }

        let mut new_items = vec![];

        if let Some(market_group_id) = maybe_market_group_id {
            let market_groups = self
                .market_groups
                .read()
                .map_err(|e| format!("Failed to acquire read lock: {}", e))?;

            if !market_groups.contains_key(&market_group_id) {
                new_items.push(GetData::MarketGroup(market_group_id));
            }
        }

        Ok(new_items)
    }

    pub fn add_market_group(&self, market_group: MarketGroup) -> Result<Vec<GetData>, String> {
        let market_group_id = market_group.market_group_id;

        {
            let mut market_groups = self
                .market_groups
                .write()
                .map_err(|e| format!("Failed to acquire write lock: {}", e))?;
            market_groups.insert(market_group_id, market_group.clone());
        }

        let mut new_items = vec![];

        {
            let types = self
                .types
                .read()
                .map_err(|e| format!("Failed to acquire read lock: {}", e))?;

            for type_id in market_group.types {
                if !types.contains_key(&type_id) {
                    new_items.push(GetData::Type(type_id));
                }
            }
        }

        Ok(new_items)
    }

    pub fn add_dynamic(
        &self,
        type_id: TypeId,
        item_id: ItemId,
        dynamic: DynamicItem,
    ) -> Result<Vec<GetData>, String> {
        let new_items = self.add_dynamic_internal(type_id, item_id, dynamic.clone())?;
        Ok(new_items)
    }

    fn is_on_station(&self, asset: &AssetItem) -> bool {
        asset.location_type == "station"
    }

    pub fn is_abyssal(&self, asset: &AssetItem) -> Result<bool, String> {
        let abyssal_items = self
            .abyssal_items
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;
        Ok(abyssal_items.contains(&asset.type_id))
    }

    fn add_dynamic_internal(
        &self,
        _type_id: TypeId,
        item_id: ItemId,
        dynamic: DynamicItem,
    ) -> Result<Vec<GetData>, String> {
        let source_type_id = dynamic.source_type_id;

        let mut dynamics = self
            .dynamics
            .write()
            .map_err(|e| format!("Failed to acquire write lock: {}", e))?;
        dynamics.insert(item_id, dynamic.clone());

        let mut new_items = vec![];

        {
            let dogma_attributes = self
                .dogma_attributes
                .read()
                .map_err(|e| format!("Failed to acquire read lock: {}", e))?;

            for attr in &dynamic.dogma_attributes {
                if !dogma_attributes.contains_key(&attr.attribute_id) {
                    new_items.push(GetData::DogmaAttribute(attr.attribute_id));
                }
            }
        }

        // Add source type dependency
        {
            let types = self
                .types
                .read()
                .map_err(|e| format!("Failed to acquire read lock: {}", e))?;

            if !types.contains_key(&source_type_id) {
                new_items.push(GetData::Type(source_type_id));
            }

            if !types.contains_key(&dynamic.mutator_type_id) {
                new_items.push(GetData::Type(dynamic.mutator_type_id));
            }
        }

        Ok(new_items)
    }

    pub fn all_items_resolved(&self) -> Result<bool, String> {
        let assets = self
            .assets
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;

        let stations = self
            .stations
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;
        let dynamics = self
            .dynamics
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;
        let types = self
            .types
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;
        let market_groups = self
            .market_groups
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;

        for asset in assets.values() {
            if self.is_on_station(asset) {
                let station_id = asset.location_id as StationId;
                if !stations.contains_key(&station_id) {
                    println!("station not found for {asset:?}");
                    return Ok(false);
                }
            }

            let mut type_id = asset.type_id;

            let is_abyssal = self.is_abyssal(asset)?;
            if is_abyssal {
                let dynamic = dynamics.get(&asset.item_id);

                match dynamic {
                    Some(dynamic) => {
                        type_id = dynamic.source_type_id;
                    }
                    None => {
                        // println!("dynamic not found for {asset:?}");
                        return Ok(false);
                    }
                }
            }

            let item_type = types.get(&type_id);
            match item_type {
                Some(item_type) => {
                    if let Some(market_group_id) = item_type.market_group_id {
                        if !market_groups.contains_key(&market_group_id) {
                            // println!("market group not found for item type {item_type:?}");
                            return Ok(false);
                        }
                    }
                }
                None => {
                    // println!("type not found for {asset:?}");
                    return Ok(false);
                }
            }
        }

        let dogma_attributes = self
            .dogma_attributes
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;
        for dynamic in dynamics.values() {
            for attr in dynamic.dogma_attributes.iter() {
                if !dogma_attributes.contains_key(&attr.attribute_id) {
                    println!("dogma attribute not found for {attr:?}");
                    return Ok(false);
                }
            }
        }

        println!("all assets are valid");
        Ok(true)
    }
}

pub struct CharacterAssetsDb {
    pub db: CharacterAssets,
    dir: String,
    last_stored_at: RwLock<DateTime<Utc>>,
    last_updated_at: RwLock<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize)]
struct SerializableCharacterAssetsDb {
    db: CharacterAssets,
    dir: String,
    last_stored_at: DateTime<Utc>,
    last_updated_at: DateTime<Utc>,
}

impl Serialize for CharacterAssetsDb {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let last_stored_at = self
            .last_stored_at
            .read()
            .map_err(serde::ser::Error::custom)?;
        let last_updated_at = self
            .last_updated_at
            .read()
            .map_err(serde::ser::Error::custom)?;

        let serializable = SerializableCharacterAssetsDb {
            db: self.db.clone(),
            dir: self.dir.clone(),
            last_stored_at: *last_stored_at,
            last_updated_at: *last_updated_at,
        };
        serializable.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CharacterAssetsDb {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let serializable = SerializableCharacterAssetsDb::deserialize(deserializer)?;

        Ok(CharacterAssetsDb {
            db: serializable.db,
            dir: serializable.dir,
            last_stored_at: RwLock::new(serializable.last_stored_at),
            last_updated_at: RwLock::new(serializable.last_updated_at),
        })
    }
}

impl CharacterAssetsDb {
    pub fn from_dir(
        dir: &str,
        abyssal_items: Vec<TypeId>,
    ) -> Result<CharacterAssetsDb, std::io::Error> {
        let now = Utc::now();
        Ok(CharacterAssetsDb {
            db: CharacterAssets::new(abyssal_items),
            dir: dir.to_string(),
            last_stored_at: RwLock::new(now),
            last_updated_at: RwLock::new(now),
        })
    }

    pub fn with_all_data<R, F>(&self, f: F) -> Result<R, String>
    where
        F: FnOnce(
            &BTreeMap<ItemId, AssetItem>,
            &BTreeMap<ItemId, String>,
            &BTreeMap<StationId, Station>,
            &BTreeMap<ItemId, DynamicItem>,
            &BTreeMap<TypeId, ItemType>,
            &BTreeMap<DogmaAttributeId, DogmaAttribute>,
        ) -> R,
    {
        let assets = self
            .db
            .assets
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;
        let assets_names = self
            .db
            .assets_names
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;
        let stations = self
            .db
            .stations
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;
        let dynamics = self
            .db
            .dynamics
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;
        let types = self
            .db
            .types
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;
        let dogma_attributes = self
            .db
            .dogma_attributes
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;
        Ok(f(&*assets, &*assets_names, &*stations, &*dynamics, &*types, &*dogma_attributes))
    }




pub fn build_location_chain(
    &self,
    asset: &AssetItem,
    assets: &BTreeMap<ItemId, AssetItem>,
    assets_names: &BTreeMap<ItemId, String>,
    stations: &BTreeMap<StationId, Station>,
    stats: &mut ChainStats,
    cache: &mut HashMap<i64, (Arc<str>, Arc<str>, Arc<str>)>,
    timings: &mut ChainTimings,
) -> (Arc<str>, Arc<str>, Arc<str>) {
    let total_start = Instant::now();
    stats.total_calls += 1;

    let cache_start = Instant::now();
    if let Some(cached) = cache.get(&asset.location_id) {
        timings.cache_hit += cache_start.elapsed();
        timings.total += total_start.elapsed();
        return cached.clone();
    }
    timings.cache_lookup += cache_start.elapsed();

    let mut location_chain = vec![];
    let mut current_location_id = asset.location_id;
    let mut current_location_type = asset.location_type.clone();
    let mut station_name = "Unknown".to_string();

    if current_location_type == "station" {
        stats.direct_station += 1;
        let station_start = Instant::now();
        if let Some(station) = stations.get(&(current_location_id as StationId)) {
            station_name = station.name.clone();
        }
        timings.station_lookup += station_start.elapsed();

        let arc_start = Instant::now();
        let result = (
            Arc::from(station_name.as_str()), 
            Arc::from(current_location_type.as_str()), 
            Arc::from("Direct")
        );
        timings.arc_creation += arc_start.elapsed();

        cache.insert(asset.location_id, result.clone());
        timings.total += total_start.elapsed();
        return result;
    }

    let mut depth = 0;
    const MAX_DEPTH: u32 = 10;
    
    while depth < MAX_DEPTH {
        stats.lookups += 1;

        let asset_start = Instant::now();
        let parent_asset = assets.get(&(ItemId::from(current_location_id)));
        timings.asset_lookup += asset_start.elapsed();
        
        if let Some(parent_asset) = parent_asset {
            let name_start = Instant::now();
            let name = assets_names
                .get(&parent_asset.item_id)
                .cloned()
                .unwrap_or_else(|| format!("Container_{}", parent_asset.item_id));
            timings.name_lookup += name_start.elapsed();

            location_chain.push(name);
            current_location_id = parent_asset.location_id;
            current_location_type = parent_asset.location_type.clone();

            if current_location_type == "station" {
                let station_start = Instant::now();
                if let Some(station) = stations.get(&(current_location_id as StationId)) {
                    station_name = station.name.clone();
                }
                timings.station_lookup += station_start.elapsed();
                break;
            }
        } else {
            if current_location_type == "station" {
                let station_start = Instant::now();
                if let Some(station) = stations.get(&(current_location_id as StationId)) {
                    station_name = station.name.clone();
                }
                timings.station_lookup += station_start.elapsed();
            }
            break;
        }

        depth += 1;
    }

    stats.max_depth = stats.max_depth.max(depth);
    stats.total_depth += depth;
    
    let string_start = Instant::now();
    location_chain.reverse();
    let location_name = if location_chain.is_empty() {
        "Direct".to_string()
    } else {
        location_chain.join(" -> ")
    };
    timings.string_ops += string_start.elapsed();

    let arc_start = Instant::now();
    let result = (
        Arc::from(station_name.as_str()),
        Arc::from(current_location_type.as_str()),
        Arc::from(location_name.as_str())
    );
    timings.arc_creation += arc_start.elapsed();

    cache.insert(asset.location_id, result.clone());
    timings.total += total_start.elapsed();
    
    result
}


    // Getter methods for accessing inner data structures
    pub fn get_all_assets(&self) -> Result<BTreeMap<ItemId, AssetItem>, String> {
        let assets = self
            .db
            .assets
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;
        Ok(assets.clone())
    }

    pub fn with_assets<R, F>(&self, f: F) -> Result<R, String>
    where
        F: FnOnce(&BTreeMap<ItemId, AssetItem>) -> R
    {
        let assets = self
            .db
            .assets
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;
        Ok(f(&*assets))
    }

    pub fn get_all_types(&self) -> Result<BTreeMap<TypeId, ItemType>, String> {
        let types = self
            .db
            .types
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;
        Ok(types.clone())
    }

    pub fn with_types<R, F>(&self, f: F) -> Result<R, String>
    where
        F: FnOnce(&BTreeMap<TypeId, ItemType>) -> R,
    {
        let types = self
            .db
            .types
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;
        Ok(f(&*types))
    }

    pub fn get_all_market_groups(&self) -> Result<BTreeMap<MarketGroupId, MarketGroup>, String> {
        let market_groups = self
            .db
            .market_groups
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;
        Ok(market_groups.clone())
    }

    pub fn get_all_stations(&self) -> Result<BTreeMap<StationId, Station>, String> {
        let stations = self
            .db
            .stations
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;
        Ok(stations.clone())
    }

    pub fn get_all_dynamics(&self) -> Result<BTreeMap<ItemId, DynamicItem>, String> {
        let dynamics = self
            .db
            .dynamics
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;
        Ok(dynamics.clone())
    }

    pub fn with_dynamics<R, F>(&self, f: F) -> Result<R, String>
    where
        F: FnOnce(&BTreeMap<ItemId, DynamicItem>) -> R,
    {
        let dynamics = self
            .db
            .dynamics
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;
        Ok(f(&*dynamics))
    }

    pub fn get_all_dogma_attributes(
        &self,
    ) -> Result<BTreeMap<DogmaAttributeId, DogmaAttribute>, String> {
        let dogma_attributes = self
            .db
            .dogma_attributes
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;
        Ok(dogma_attributes.clone())
    }

    pub fn get_all_asset_names(&self) -> Result<BTreeMap<ItemId, String>, String> {
        let asset_names = self
            .db
            .assets_names
            .read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;
        Ok(asset_names.clone())
    }

    pub fn add_asset(&self, item: AssetItem) -> Result<Vec<GetData>, String> {
        let new_items = self.db.add_asset(item)?;
        let mut t = self
            .last_updated_at
            .write()
            .map_err(|_| "Failed to write last_updated_at")?;
        *t = Utc::now();
        Ok(new_items)
    }

    pub fn add_asset_name(&self, item_id: ItemId, name: String) -> Result<(), String> {
        self.db.add_asset_name(item_id, name)?;
        let mut t = self
            .last_updated_at
            .write()
            .map_err(|_| "Failed to write last_updated_at")?;
        *t = Utc::now();
        Ok(())
    }

    pub fn add_station(
        &self,
        station_id: StationId,
        station: Station,
    ) -> Result<Vec<GetData>, String> {
        let new_items = self.db.add_station(station_id, station)?;
        let mut t = self
            .last_updated_at
            .write()
            .map_err(|_| "Failed to write last_updated_at")?;
        *t = Utc::now();
        Ok(new_items)
    }

    pub fn add_dogma_attribute(
        &self,
        dogma_attribute: DogmaAttribute,
    ) -> Result<Vec<GetData>, String> {
        let new_items = self.db.add_dogma_attribute(dogma_attribute)?;
        let mut t = self
            .last_updated_at
            .write()
            .map_err(|_| "Failed to write last_updated_at")?;
        *t = Utc::now();
        Ok(new_items)
    }

    pub fn get_attribute_id_by_name(
        &self,
        attribute_name: String,
    ) -> Result<DogmaAttributeId, String> {
        self.db.get_attribute_id_by_name(attribute_name)
    }

    pub fn add_market_group(&self, market_group: MarketGroup) -> Result<Vec<GetData>, String> {
        let new_items = self.db.add_market_group(market_group)?;
        let mut t = self
            .last_updated_at
            .write()
            .map_err(|_| "Failed to write last_updated_at")?;
        *t = Utc::now();
        Ok(new_items)
    }

    pub fn add_dynamic(
        &self,
        type_id: TypeId,
        item_id: ItemId,
        dynamic: DynamicItem,
    ) -> Result<Vec<GetData>, String> {
        let new_items = self.db.add_dynamic(type_id, item_id, dynamic)?;
        let mut t = self
            .last_updated_at
            .write()
            .map_err(|_| "Failed to write last_updated_at")?;
        *t = Utc::now();
        Ok(new_items)
    }

    pub fn add_type(&self, item_type: ItemType) -> Result<Vec<GetData>, String> {
        let new_items = self.db.add_type(item_type)?;
        let mut t = self
            .last_updated_at
            .write()
            .map_err(|_| "Failed to write last_updated_at")?;
        *t = Utc::now();
        Ok(new_items)
    }

    pub fn add_mutaplasmid_effects(
        &self,
        mutator_type_id: TypeId,
        attributes: Vec<(DogmaAttributeId, f64, f64)>,
        input_output: Vec<(TypeId, Vec<TypeId>)>, // [(resulting_type, [applicable_types]), ...]
    ) -> Result<Vec<GetData>, String> {
        let new_items =
            self.db
                .add_mutaplasmid_effects(mutator_type_id, attributes, input_output)?;
        let mut t = self
            .last_updated_at
            .write()
            .map_err(|_| "Failed to write last_updated_at")?;
        *t = Utc::now();
        Ok(new_items)
    }

    pub fn get_mutator_ids_by_resulting_type_id(
        &self,
        resulting_type_id: &TypeId,
    ) -> Result<BTreeMap<(TypeId, String), BTreeMap<DogmaAttributeId, AttributeRange>>, String>
    {
        self.db.get_mutators_by_resulting_type_id(resulting_type_id)
    }

    pub fn get_min_max_attributes_by_resulting_type_id(
        &self,
        resulting_type_id: &TypeId,
    ) -> Result<BTreeMap<DogmaAttributeId, AttributeRange>, String> {
        self.db
            .get_min_max_attributes_by_resulting_type_id(resulting_type_id)
    }

    pub fn get_attributes_by_mutator_type_id(
        &self,
        mutator_type_id: &TypeId,
    ) -> Result<BTreeMap<DogmaAttributeId, AttributeRange>, String> {
        self.db.get_attributes_by_mutator_type_id(mutator_type_id)
    }

    pub fn get_attribute_ids_by_mutator(
        &self,
        mutator_type_id: &TypeId,
    ) -> Result<BTreeSet<DogmaAttributeId>, String> {
        self.db.get_attribute_ids_by_mutator(mutator_type_id)
    }

    pub fn get_resulting_type_by_source_mutator(
        &self,
        source_type_id: TypeId,
        mutator_type_id: TypeId,
    ) -> Result<TypeId, String> {
        self.db
            .get_resulting_type_by_source_mutator(source_type_id, mutator_type_id)
    }

    pub fn get_applicable_types_by_resulting_type(
        &self,
        resulting_type_id: &TypeId,
    ) -> Result<BTreeSet<TypeId>, String> {
        self.db
            .get_applicable_types_by_resulting_type(resulting_type_id)
    }

    pub fn is_abyssal(&self, asset: &AssetItem) -> Result<bool, String> {
        self.db.is_abyssal(asset)
    }

    pub fn all_items_resolved(&self) -> Result<bool, String> {
        self.db.all_items_resolved()
    }

    pub fn store(&self) -> Result<(), String> {
        let should_store = {
            let last_stored_at = self
                .last_stored_at
                .read()
                .map_err(|_| "Failed to read last_stored_at")?;
            let last_updated_at = self
                .last_updated_at
                .read()
                .map_err(|_| "Failed to read last_updated_at")?;
            println!("character_assets_db: {last_stored_at} / {last_updated_at}");

            *last_stored_at < *last_updated_at
        };

        if should_store {
            {
                let mut last_stored_at = self
                    .last_stored_at
                    .write()
                    .map_err(|_| "Failed to write last_stored_at")?;
                *last_stored_at = Utc::now();
            }

            let file_path = Self::last_file(&self.dir);
            println!("character_assets_db: file_path: {file_path}");
            let temp_path = format!("{file_path}.tmp");
            println!("character_assets_db: temp_path: {temp_path}");
            let encoded = serde_cbor::ser::to_vec(&self)
                .map_err(|e| format!("Failed to serialize data: {}", e));

            println!("character_assets_db: encoded");
            std::fs::write(&temp_path, encoded?)
                .map_err(|e| format!("Failed to write to file: {}", e))?;
            println!("character_assets_db: temp_path written");
            std::fs::rename(temp_path, file_path)
                .map_err(|e| format!("Failed to rename file: {}", e))?;
            println!("character_assets_db: file renamed");
        } else {
            println!("character_assets_db: Using old file")
        }
        println!("character_assets_db: Done");

        Ok(())
    }

    fn last_file(dir: &str) -> String {
        format!("{}/new_assets.cbor", dir)
    }
}
