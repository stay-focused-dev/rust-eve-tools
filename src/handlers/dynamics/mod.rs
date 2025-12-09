use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use crate::AppContext;
use crate::{DogmaAttributeId, ItemId, TypeId};

pub mod virtual_attributes;
use virtual_attributes::{
    append_attribute_values, append_min_max_attribute_values, append_varying_attributes,
    initialize_virtual_attributes,
};

#[derive(Serialize)]
pub struct DynamicsReport {
    data: BTreeMap<String, ResultingGroup>,
    generated_at: String,
}

#[derive(Serialize)]
pub struct ResultingGroup {
    pub source_mutator_groups: Vec<SourceMutatorGroup>,
    pub base_types: Vec<BaseItemType>,
    pub mutators: Vec<MutatorConcise>,
    pub varying_attributes: Vec<VaryingAttribute>,
    pub min_max_attributes: Vec<AttributeRange>,
}

#[derive(Serialize)]
pub struct SourceMutatorGroup {
    pub source_type_id: TypeId,
    pub mutator_type_id: TypeId,
    pub attributes: Vec<AttributeRange>,
    pub dynamics: Vec<DynamicItemData>,
}

#[derive(Serialize)]
pub struct MutatorConcise {
    pub id: TypeId,
    pub name: String,
    pub attributes: Vec<AttributeRange>,
}

#[derive(Serialize)]
pub struct BaseItemType {
    pub id: TypeId,
    pub name: String,
    pub attributes: Vec<AttributeValue>,
}

// // check base_types attributes

impl DynamicsReport {
    fn check_integrity(&self) -> Result<(), String> {
        for (item_group_name, item_group) in &self.data {
            let varying_attribute_ids: BTreeSet<DogmaAttributeId> =
                item_group.varying_attributes.iter().map(|a| a.id).collect();

            if varying_attribute_ids.len() != item_group.varying_attributes.len() {
                return Err(format!(
                    "Duplicate found for varying_attributes for item group {}",
                    item_group_name,
                ))
            }

            for source_mutator_group in &item_group.source_mutator_groups {
                if !item_group
                    .base_types
                    .iter()
                    .any(|t| t.id == source_mutator_group.source_type_id)
                {
                    return Err(format!(
                        "Source type {} not found in base types for item group {}",
                        source_mutator_group.source_type_id, item_group_name
                    ));
                }

                let base_type_ids: BTreeSet<TypeId> =
                    item_group.base_types.iter().map(|t| t.id).collect();
                if base_type_ids.len() != item_group.base_types.len() {
                    return Err(format!(
                        "Duplicate found for base_types for item group {}",
                        item_group_name,
                    ))
                }

                if !item_group
                    .mutators
                    .iter()
                    .any(|g| g.id == source_mutator_group.mutator_type_id)
                {
                    return Err(format!(
                        "Mutator type {} not found in mutators for item group {}",
                        source_mutator_group.mutator_type_id, item_group_name
                    ));
                }

                let mutator_ids: BTreeSet<TypeId> =
                    item_group.mutators.iter().map(|t| t.id).collect();
                if mutator_ids.len() != item_group.mutators.len() {
                    return Err(format!(
                        "Duplicate found for mutators for item group {}",
                        item_group_name,
                    ))
                }

                let attribute_ids: BTreeSet<DogmaAttributeId> =
                    source_mutator_group.attributes.iter().map(|a| a.id).collect();

                if attribute_ids != varying_attribute_ids {
                    return Err(format!(
                        "Attribute mismatch for source mutator group {item_group_name}/{}.{}",
                        source_mutator_group.source_type_id, source_mutator_group.mutator_type_id
                    ));
                }

                for dynamic in &source_mutator_group.dynamics {
                    let attribute_ids: BTreeSet<DogmaAttributeId> =
                        dynamic.attributes.iter().map(|a| a.id).collect();

                    if attribute_ids != varying_attribute_ids {
                        return Err(format!(
                            "Attribute mismatch for dynamic {item_group_name}/{}.{}/{}",
                            source_mutator_group.source_type_id,
                            source_mutator_group.mutator_type_id,
                            dynamic.item_id,
                        ));
                    }
                }
            }

            for base_type in &item_group.base_types {
                let attribute_ids: BTreeSet<DogmaAttributeId> =
                    base_type.attributes.iter().map(|a| a.id).collect();

                if attribute_ids != varying_attribute_ids {
                    return Err(format!(
                        "Attribute mismatch for type {item_group_name}/{}",
                        base_type.id
                    ));
                }
            }

            for mutator in &item_group.mutators {
                let attribute_ids: BTreeSet<DogmaAttributeId> =
                    mutator.attributes.iter().map(|a| a.id).collect();

                if attribute_ids != varying_attribute_ids {
                    return Err(format!(
                        "Attribute mismatch for mutator {item_group_name}/{}",
                        mutator.id
                    ));
                }
            }

            {
                let attribute_ids: BTreeSet<DogmaAttributeId> =
                    item_group.min_max_attributes.iter().map(|a| a.id).collect();

                if attribute_ids != varying_attribute_ids {
                    return Err(format!(
                        "Attribute mismatch for min_max attributes {item_group_name}"
                    ));
                }
            }
        }

        Ok(())
    }

    pub async fn new(context: &AppContext) -> Result<Self, String> {
        let start_time = Instant::now();

        let character_assets_db = &context.character_assets_db;

        character_assets_db.with_all_data(|_assets, dynamics, types, dogma_attributes| {
            println!(
                "get all from character_assets_db: {:?}",
                start_time.elapsed()
            );

            let location_start = Instant::now();
            let item_ids: Vec<ItemId> = dynamics.keys().cloned().collect();
            let location_cache = character_assets_db.build_location_chains_batch(&item_ids)?;
            println!(
                "pre-computed {} location chains: {:?}",
                item_ids.len(),
                location_start.elapsed()
            );

            let name_to_id_resolver = |attribute_name: &str| -> DogmaAttributeId {
                let res = character_assets_db.get_attribute_id_by_name(attribute_name.to_string());
                match res {
                    Ok(id) => id,
                    Err(err) => panic!("Failed to resolve attribute name: {}", err),
                }
            };
            initialize_virtual_attributes(&name_to_id_resolver);

            let mut dynamics_by_source_mutator: BTreeMap<(TypeId, TypeId), Vec<DynamicItemData>> =
                BTreeMap::new();

            // for (item_id, dynamic) in dynamics {
            //     let asset = assets.get(item_id).unwrap();

            //     let (station_name, location_type, location_name) =
            //         character_assets_db.build_location_chain(asset);

            //     // let (station_name, location_type, location_name) =
            //     //     ("".to_string(), "".to_string(), "".to_string());

            //     let attributes = dynamic
            //         .dogma_attributes
            //         .iter()
            //         .map(|attr| AttributeValue {
            //             id: attr.attribute_id,
            //             value: attr.value,
            //         })
            //         .collect();

            //     let item = DynamicItemData {
            //         item_id: *item_id,
            //         station_name,
            //         location_type,
            //         location_name,
            //         attributes,
            //     };

            //     dynamics_by_source_mutator
            //         .entry((dynamic.source_type_id, dynamic.mutator_type_id))
            //         .or_default()
            //         .push(item);
            // }
            let mut asset_lookup_time = std::time::Duration::new(0, 0);
            let mut location_chain_time = std::time::Duration::new(0, 0);
            let mut attributes_collect_time: std::time::Duration = std::time::Duration::new(0, 0);
            let mut struct_creation_time = std::time::Duration::new(0, 0);
            let mut btree_insert_time = std::time::Duration::new(0, 0);

            let total_items = dynamics.len();
            let mut processed_items = 0;

            for (item_id, dynamic) in dynamics {
                // 1. Asset lookup timing
                let start = Instant::now();
                // let asset = assets.get(item_id).unwrap();
                asset_lookup_time += start.elapsed();

                // 2. Location chain timing (likely the bottleneck)
                let start = Instant::now();
                // let (station_name, location_type, location_name) =
                //     character_assets_db.build_location_chain(asset);
                let (station_name, location_type, location_name) = location_cache
                    .get(item_id)
                    .map(|(s, t, l)| (s.clone(), t.clone(), l.clone()))
                    .unwrap_or_else(|| {
                        (
                            "Unknown".to_string(),
                            "Unknown".to_string(),
                            "Unknown".to_string(),
                        )
                    });
                location_chain_time += start.elapsed();

                // 3. Attributes mapping timing
                let start = Instant::now();
                let attributes = dynamic.dogma_attributes.iter().map(|attr| AttributeValue {
                    id: attr.attribute_id,
                    value: attr.value,
                }).collect();
                attributes_collect_time += start.elapsed();
                


                // 5. Struct creation timing
                let start = Instant::now();
                let item = DynamicItemData {
                    item_id: *item_id,
                    station_name,
                    location_type,
                    location_name,
                    attributes,
                };
                struct_creation_time += start.elapsed();

                // 6. BTreeMap insertion timing
                let start = Instant::now();
                dynamics_by_source_mutator
                    .entry((dynamic.source_type_id, dynamic.mutator_type_id))
                    .or_default()
                    .push(item);
                btree_insert_time += start.elapsed();

                processed_items += 1;

                // Print progress every 1000 items
                if processed_items % 5000 == 0 {
                    println!("Processed {}/{} items", processed_items, total_items);
                }
            }

            // Print the breakdown
            println!("=== LOOP TIMING BREAKDOWN ===");
            println!("Total items processed: {}", total_items);
            let total_time = (asset_lookup_time
                        + location_chain_time
                        + attributes_collect_time
                        + struct_creation_time
                        + btree_insert_time)
                        .as_secs_f64();

            println!(
                "Asset lookup:      {:?} ({:.1}%)",
                asset_lookup_time,
                asset_lookup_time.as_secs_f64() / total_time * 100.0
            );
            println!(
                "Location chain:    {:?} ({:.1}%)",
                location_chain_time,
                location_chain_time.as_secs_f64() / total_time * 100.0
            );
            println!(
                "Attributes collect:{:?} ({:.1}%)",
                attributes_collect_time,
                attributes_collect_time.as_secs_f64() / total_time * 100.0
            );
            println!(
                "Struct creation:   {:?} ({:.1}%)",
                struct_creation_time,
                struct_creation_time.as_secs_f64() / total_time * 100.0
            );
            println!(
                "BTree insert:      {:?} ({:.1}%)",
                btree_insert_time,
                btree_insert_time.as_secs_f64() / total_time * 100.0
            );
            println!("=============================");
            println!("analyzed all dynamics: {:?}", start_time.elapsed());

            let mut resulting_to_source_mutator: BTreeMap<TypeId, Vec<(TypeId, TypeId)>> =
                BTreeMap::new();
            for ((source_type_id, mutator_type_id), _) in &dynamics_by_source_mutator {
                let resulting_type_id = character_assets_db
                    .get_resulting_type_by_source_mutator(*source_type_id, *mutator_type_id)?;

                resulting_to_source_mutator
                    .entry(resulting_type_id)
                    .or_default()
                    .push((*source_type_id, *mutator_type_id));
            }
            println!("analyzed all resulting types: {:?}", start_time.elapsed());

            let mut report = BTreeMap::new();

            for (resulting_type_id, source_mutators) in &resulting_to_source_mutator {
                let resulting_type_name = types.get(resulting_type_id).unwrap().name.clone();

                let mut possible_attributes: Vec<BTreeSet<DogmaAttributeId>> = vec![];

                for (_source_type_id, mutator_type_id) in source_mutators {
                    let attribute_ids =
                        character_assets_db.get_attribute_ids_by_mutator(mutator_type_id)?;
                    possible_attributes.push(attribute_ids);
                }

                let (all_same, intersected_attributes) = {
                    let first = possible_attributes.first().unwrap();
                    let all_same = possible_attributes.iter().skip(1).all(|set| set == first);
                    let intersected_attributes =
                        possible_attributes
                            .iter()
                            .skip(1)
                            .fold(first.clone(), |mut acc, set| {
                                acc.retain(|x| set.contains(x));
                                acc
                            });
                    (all_same, intersected_attributes)
                };

                if !all_same {
                    println!(
                        "attributes not all same for resulting type {}",
                        resulting_type_name
                    );
                }

                let mut varying_attributes = vec![];
                let mut varying_attribute_ids = BTreeSet::new();
                for attr_id in intersected_attributes {
                    let attribute = dogma_attributes.get(&attr_id).unwrap();
                    varying_attributes.push(VaryingAttribute {
                        id: attribute.attribute_id,
                        name: attribute
                            .name
                            .clone()
                            .unwrap_or_else(|| format!("attribute_{}", attribute.attribute_id)),
                        high_is_good: attribute.high_is_good,
                    });
                    varying_attribute_ids.insert(attribute.attribute_id);
                }
                append_varying_attributes(&mut varying_attributes);
                // add possible virtual attributes ids
                varying_attribute_ids = varying_attributes.iter().map(|a| a.id).collect();

                println!(
                    "{}: analyzed all varying attributes: {:?}",
                    resulting_type_name,
                    start_time.elapsed()
                );

                let base_types: Vec<BaseItemType> = character_assets_db
                    .get_applicable_types_by_resulting_type(resulting_type_id)?
                    .iter()
                    .filter_map(|type_id| match types.get(type_id) {
                        Some(item_type) => {
                            let mut attributes: Vec<_> = item_type
                                .dogma_attributes
                                .iter()
                                .filter(|a| varying_attribute_ids.contains(&a.attribute_id))
                                .map(|a| AttributeValue {
                                    id: a.attribute_id,
                                    value: a.value,
                                })
                                .collect();

                            append_attribute_values(&mut attributes);

                            Some(
                                BaseItemType {
                                    id: *type_id,
                                    name: item_type.name.clone(),
                                    attributes,
                                },
                            )
                        }
                        None => {
                            eprintln!("Type not found: {}", type_id);
                            None
                        }
                    })
                    .collect();

                let raw_mutators =
                    character_assets_db.get_mutator_ids_by_resulting_type_id(resulting_type_id)?;

                let mut mutators = vec![];
                for ((mutator_type_id, mutator_name), attributes_map) in raw_mutators {
                    let mut attributes = attributes_map
                        .into_iter()
                        .map(|(id, range)| AttributeRange {
                            id,
                            min: range.min,
                            max: range.max,
                        })
                        .collect();
                    append_min_max_attribute_values(&mut attributes);

                    let mutator = MutatorConcise {
                        id: mutator_type_id,
                        name: mutator_name,
                        attributes,
                    };
                    mutators.push(mutator);
                }

                let raw_min_max_attributes = character_assets_db
                    .get_min_max_attributes_by_resulting_type_id(resulting_type_id)?;

                let mut min_max_attributes: Vec<AttributeRange> = raw_min_max_attributes
                    .iter()
                    .map(|(attr_id, attr_range)| AttributeRange {
                        id: *attr_id,
                        min: attr_range.min,
                        max: attr_range.max,
                    })
                    .collect();

                append_min_max_attribute_values(&mut min_max_attributes);

                let mut resulting_group = ResultingGroup {
                    source_mutator_groups: vec![],
                    base_types,
                    mutators,
                    varying_attributes,
                    min_max_attributes,
                };

                for (source_type_id, mutator_type_id) in source_mutators {
                    let mut dynamics = dynamics_by_source_mutator
                        .get(&(*source_type_id, *mutator_type_id))
                        .unwrap()
                        .to_vec();

                    for dynamic in &mut dynamics {
                        dynamic.attributes.retain(|attr| varying_attribute_ids.contains(&attr.id));
                        append_attribute_values(&mut dynamic.attributes);
                    }

                    let source_type = types.get(source_type_id).unwrap();

                    let attributes =
                        character_assets_db.get_attributes_by_mutator_type_id(mutator_type_id)?;

                    let mut attributes = source_type
                        .dogma_attributes
                        .clone()
                        .into_iter()
                        .filter_map(|attr| {
                            attributes.get(&attr.attribute_id).map(|attr_range| {
                                let v1 = attr.value * attr_range.min;
                                let v2 = attr.value * attr_range.max;

                                let (min, max) = if v1 < v2 { (v1, v2) } else { (v2, v1) };

                                AttributeRange {
                                    id: attr.attribute_id,
                                    min,
                                    max,
                                }
                            })
                        })
                        .collect();

                    append_min_max_attribute_values(&mut attributes);

                    let source_mutator_group = SourceMutatorGroup {
                        source_type_id: *source_type_id,
                        mutator_type_id: *mutator_type_id,
                        dynamics,
                        attributes,
                    };
                    resulting_group
                        .source_mutator_groups
                        .push(source_mutator_group);
                }

                report.insert(resulting_type_name, resulting_group);
            }

            let ret = DynamicsReport {
                data: report,
                generated_at: chrono::Utc::now().to_rfc3339(),
            };
            if let Err(err) = Self::check_integrity(&ret) {
                eprintln!("check_integrity failed: {}", err);
            } else {
                println!("check_integrity passed");
            }
            println!("created report: {:?}", start_time.elapsed());

            Ok(ret)
        })?
    }
}

#[derive(Serialize, Clone)]
pub struct DynamicItemData {
    item_id: ItemId,
    station_name: String,
    location_type: String,
    location_name: String,
    attributes: Vec<AttributeValue>,
}

#[derive(Serialize, Clone)]
pub struct VaryingAttribute {
    id: DogmaAttributeId,
    name: String,
    high_is_good: Option<bool>,
}

#[derive(Serialize, Clone, Debug)]
pub struct AttributeValue {
    id: DogmaAttributeId,
    value: f64,
}

#[derive(Serialize, Clone, Debug)]
pub struct AttributeRange {
    id: DogmaAttributeId,
    min: f64,
    max: f64,
}
