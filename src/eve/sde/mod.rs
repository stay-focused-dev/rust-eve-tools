use super::types::{
    DogmaAttribute, DogmaAttributeConcise, ItemType, MarketGroup, MarketGroupId, TypeId,
};
use sqlx::{Result, Row, sqlite::SqlitePool, sqlite::SqlitePoolOptions};
use std::collections::HashMap;

pub async fn create_conn_pool(fp: &str) -> Result<SqlitePool> {
    let pool = SqlitePoolOptions::new()
        .max_connections(10)
        .connect(fp)
        .await?;
    Ok(pool)
}

pub async fn get_abyssal_modules(pool: &SqlitePool) -> Result<Vec<i32>> {
    let mut modules = vec![];
    let query = "
        SELECT
            typeID
        FROM
            invTypes
        WHERE
               typeName LIKE '%Abyssal%'
            OR typeName LIKE '%Mutated%'";
    let rows = sqlx::query(query).fetch_all(pool).await?;

    for row in rows {
        modules.push(row.get(0));
    }
    Ok(modules)
}

pub async fn get_types_by_ids(
    pool: &SqlitePool,
    type_ids: &[TypeId],
) -> Result<Vec<ItemType>, sqlx::Error> {
    if type_ids.is_empty() {
        return Ok(vec![]);
    }

    let placeholders = type_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");

    // Single query to get types with all their dogma attributes
    let query = format!(
        "SELECT
            -- Type fields
            t.typeID,
            t.typeName,
            t.description as typeDescription,
            t.groupID,
            t.marketGroupID,
            t.capacity,
            t.mass,
            t.volume,
            t.portionSize,
            t.published,
            t.graphicID,
            t.iconID,
            -- Dogma attribute fields (NULL if no attributes)
            dta.attributeID,
            COALESCE(dta.valueFloat, CAST(dta.valueInt AS REAL)) as attributeValue
        FROM invTypes t
        LEFT JOIN dgmTypeAttributes dta ON t.typeID = dta.typeID
        WHERE t.typeID IN ({})
        ORDER BY t.typeID, dta.attributeID",
        placeholders
    );

    let mut query_builder = sqlx::query(&query);
    for type_id in type_ids {
        query_builder = query_builder.bind(type_id);
    }

    let rows = query_builder.fetch_all(pool).await?;

    // Group rows by typeID since we'll get multiple rows per type
    let mut types_map: HashMap<TypeId, ItemType> = HashMap::new();

    for row in rows {
        let type_id: TypeId = row.get("typeID");

        // Get or create the ItemType
        let item_type = types_map.entry(type_id).or_insert_with(|| ItemType {
            type_id,
            name: row.get("typeName"),
            description: row
                .get::<Option<String>, _>("typeDescription")
                .unwrap_or_default(),
            group_id: row.get("groupID"),
            market_group_id: row.get("marketGroupID"),
            capacity: row.get("capacity"),
            mass: row.get("mass"),
            volume: row.get("volume"),
            portion_size: row.get("portionSize"),
            published: row.get::<Option<bool>, _>("published").unwrap_or(false),
            graphic_id: row.get("graphicID"),
            icon_id: row.get("iconID"),
            // These fields don't exist in SDE, only in ESI
            packaged_volume: None,
            radius: None,
            // Initialize empty vectors
            dogma_attributes: Vec::new(),
            dogma_effects: Vec::new(),
        });

        // Add dogma attribute if it exists (attributeID will be NULL if no attributes)
        if let Some(attribute_id) = row.get::<Option<i32>, _>("attributeID") {
            let attribute = DogmaAttributeConcise {
                attribute_id,
                value: row.get("attributeValue"),
            };
            item_type.dogma_attributes.push(attribute);
        }
    }

    // Convert HashMap to Vec, maintaining the original order of type_ids
    let mut result = Vec::new();
    for &type_id in type_ids {
        if let Some(item_type) = types_map.remove(&type_id) {
            result.push(item_type);
        }
    }

    Ok(result)
}

pub async fn get_dogma_attributes_by_ids(
    pool: &SqlitePool,
    attribute_ids: &[i32],
) -> Result<Vec<DogmaAttribute>> {
    if attribute_ids.is_empty() {
        return Ok(vec![]);
    }

    let placeholders = attribute_ids
        .iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(",");
    let query = format!(
        "SELECT
            attributeID,
            attributeName,
            description,
            iconID,
            defaultValue,
            published,
            displayName,
            unitID,
            stackable,
            highIsGood
        FROM dgmAttributeTypes
        WHERE attributeID IN ({})",
        placeholders
    );

    let mut query_builder = sqlx::query(&query);
    for attribute_id in attribute_ids {
        query_builder = query_builder.bind(attribute_id);
    }

    let rows = query_builder.fetch_all(pool).await?;
    let mut dogma_attributes = Vec::new();

    for row in rows {
        let dogma_attribute = DogmaAttribute {
            attribute_id: row.get("attributeID"),
            name: row.get("attributeName"),
            description: row.get("description"),
            icon_id: row.get("iconID"),
            default_value: row.get("defaultValue"),
            published: row.get("published"),
            display_name: row.get("displayName"),
            unit_id: row.get("unitID"),
            stackable: row.get("stackable"),
            high_is_good: row.get("highIsGood"),
        };
        dogma_attributes.push(dogma_attribute);
    }

    Ok(dogma_attributes)
}

pub async fn get_market_groups_by_ids(
    pool: &SqlitePool,
    market_group_ids: &[MarketGroupId],
) -> Result<Vec<MarketGroup>> {
    if market_group_ids.is_empty() {
        return Ok(vec![]);
    }

    let placeholders = market_group_ids
        .iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(",");

    let query = format!(
        "SELECT
            mg.marketGroupID,
            mg.parentGroupID,
            mg.marketGroupName,
            mg.description,
            GROUP_CONCAT(t.typeID) as type_ids
        FROM invMarketGroups mg
        LEFT JOIN invTypes t ON mg.marketGroupID = t.marketGroupID
        WHERE mg.marketGroupID IN ({})
        GROUP BY mg.marketGroupID, mg.parentGroupID, mg.marketGroupName, mg.description",
        placeholders
    );

    let mut query_builder = sqlx::query(&query);
    for market_group_id in market_group_ids {
        query_builder = query_builder.bind(market_group_id);
    }

    let rows = query_builder.fetch_all(pool).await?;
    let mut market_groups = Vec::new();

    for row in rows {
        let market_group_id: MarketGroupId = row.get("marketGroupID");

        // Parse the comma-separated type IDs
        let types: Vec<TypeId> =
            if let Some(type_ids_str) = row.get::<Option<String>, _>("type_ids") {
                type_ids_str
                    .split(',')
                    .filter_map(|s| s.trim().parse().ok())
                    .collect()
            } else {
                vec![]
            };

        let market_group = MarketGroup {
            description: row
                .get::<Option<String>, _>("description")
                .unwrap_or_default(),
            market_group_id,
            name: row.get("marketGroupName"),
            parent_group_id: row.get("parentGroupID"),
            types,
        };
        market_groups.push(market_group);
    }

    Ok(market_groups)
}

/// Resolve market group hierarchy to build full names like "Small Energy Nosferatu"
pub async fn resolve_market_group_hierarchy(
    pool: &SqlitePool,
    market_group_ids: &[MarketGroupId],
) -> Result<HashMap<MarketGroupId, String>> {
    if market_group_ids.is_empty() {
        return Ok(HashMap::new());
    }

    // First, get all market groups we might need (including parents)
    let mut all_needed_ids = std::collections::HashSet::new();
    let mut to_process: Vec<MarketGroupId> = market_group_ids.to_vec();

    // Build a map of all market groups and their parents
    let mut market_group_data: HashMap<MarketGroupId, (String, Option<MarketGroupId>)> =
        HashMap::new();

    while !to_process.is_empty() {
        let current_batch: Vec<MarketGroupId> = to_process.drain(..).collect();
        let mut new_parents = Vec::new();

        // Get data for current batch
        let placeholders = current_batch
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(",");
        let query = format!(
            "SELECT marketGroupID, parentGroupID, marketGroupName
             FROM invMarketGroups
             WHERE marketGroupID IN ({})",
            placeholders
        );

        let mut query_builder = sqlx::query(&query);
        for id in &current_batch {
            query_builder = query_builder.bind(id);
        }

        let rows = query_builder.fetch_all(pool).await?;

        for row in rows {
            let market_group_id: MarketGroupId = row.get("marketGroupID");
            let parent_group_id: Option<MarketGroupId> = row.get("parentGroupID");
            let name: String = row.get("marketGroupName");

            market_group_data.insert(market_group_id, (name, parent_group_id));
            all_needed_ids.insert(market_group_id);

            // If this group has a parent we haven't seen, add it to process
            if let Some(parent_id) = parent_group_id {
                if !all_needed_ids.contains(&parent_id)
                    && !market_group_data.contains_key(&parent_id)
                {
                    new_parents.push(parent_id);
                }
            }
        }

        to_process = new_parents;
    }

    // Now build the hierarchy names
    let mut result = HashMap::new();

    for &market_group_id in market_group_ids {
        let mut name_parts = Vec::new();
        let mut current_id = market_group_id;
        let mut visited = std::collections::HashSet::new();

        // Walk up the hierarchy
        loop {
            // Prevent infinite loops
            if visited.contains(&current_id) {
                println!(
                    "Warning: Circular reference detected in market group hierarchy for ID {}",
                    current_id
                );
                break;
            }
            visited.insert(current_id);

            if let Some((name, parent_id)) = market_group_data.get(&current_id) {
                name_parts.push(name.clone());

                if let Some(parent) = parent_id {
                    current_id = *parent;
                } else {
                    break;
                }
            } else {
                break;
            }

            // Safety limit
            if name_parts.len() > 10 {
                println!(
                    "Warning: Market group hierarchy too deep for ID {}, truncating",
                    market_group_id
                );
                break;
            }
        }

        // Reverse to get "parent > child" order, then join with slashes
        name_parts.reverse();
        let full_name = name_parts.join(" / ");
        result.insert(market_group_id, full_name);
    }

    Ok(result)
}
