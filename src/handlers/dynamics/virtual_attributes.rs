use std::sync::OnceLock;

use super::{AttributeRange, AttributeValue, VaryingAttribute};
use crate::DogmaAttributeId;

pub const VIRTUAL_ARMOR_REPAIR_EFFICIENCY_ID: DogmaAttributeId = -1;
pub const VIRTUAL_ARMOR_REPAIR_SPEED_ID: DogmaAttributeId = -2;
pub const VIRTUAL_SHIELD_REPAIR_EFFICIENCY_ID: DogmaAttributeId = -3;
pub const VIRTUAL_SHIELD_REPAIR_SPEED_ID: DogmaAttributeId = -4;
pub const VIRTUAL_DPS_MODIFIER_ID: DogmaAttributeId = -5;
pub const VIRTUAL_MISSILE_DPS_MODIFIER_ID: DogmaAttributeId = -6;
pub const VIRTUAL_NEUTRALIZATION_EFFICIENCY_ID: DogmaAttributeId = -7;

struct VirtualAttributeFormula {
    virtual_id: DogmaAttributeId,
    name: &'static str,
    high_is_good: Option<bool>,
    numerator_attr_names: &'static [&'static str],
    denominator_attr_names: &'static [&'static str],
}

#[derive(Debug)]
struct ResolvedVirtualAttributeFormula {
    virtual_id: DogmaAttributeId,
    name: &'static str,
    high_is_good: Option<bool>,
    numerator_attr_ids: Vec<DogmaAttributeId>,
    denominator_attr_ids: Vec<DogmaAttributeId>,
}

const VIRTUAL_FORMULAS: &[VirtualAttributeFormula] = &[
    VirtualAttributeFormula {
        virtual_id: VIRTUAL_ARMOR_REPAIR_EFFICIENCY_ID,
        name: "Armor Repair Efficiency",
        high_is_good: Some(true),
        numerator_attr_names: &["Armor Hitpoints Repaired"],
        denominator_attr_names: &["Activation Cost"],
    },
    VirtualAttributeFormula {
        virtual_id: VIRTUAL_ARMOR_REPAIR_SPEED_ID,
        name: "Armor Repair Speed",
        high_is_good: Some(true),
        numerator_attr_names: &["Armor Hitpoints Repaired"],
        denominator_attr_names: &["Activation time / duration"],
    },
    VirtualAttributeFormula {
        virtual_id: VIRTUAL_SHIELD_REPAIR_EFFICIENCY_ID,
        name: "Shield Repair Efficiency",
        high_is_good: Some(true),
        numerator_attr_names: &["Shield Bonus"],
        denominator_attr_names: &["Activation Cost"],
    },
    VirtualAttributeFormula {
        virtual_id: VIRTUAL_SHIELD_REPAIR_SPEED_ID,
        name: "Shield Repair Speed",
        high_is_good: Some(true),
        numerator_attr_names: &["Shield Bonus"],
        denominator_attr_names: &["Activation time / duration"],
    },
    VirtualAttributeFormula {
        virtual_id: VIRTUAL_DPS_MODIFIER_ID,
        name: "DPS Modifier",
        high_is_good: Some(true),
        numerator_attr_names: &["Damage Modifier"],
        denominator_attr_names: &["rate of fire bonus"],
    },
    VirtualAttributeFormula {
        virtual_id: VIRTUAL_MISSILE_DPS_MODIFIER_ID,
        name: "Missile DPS Modifier",
        high_is_good: Some(true),
        numerator_attr_names: &["Missile Damage Bonus"],
        denominator_attr_names: &["rate of fire bonus"],
    },
    VirtualAttributeFormula {
        virtual_id: VIRTUAL_NEUTRALIZATION_EFFICIENCY_ID,
        name: "Neutralization Efficiency",
        high_is_good: Some(true),
        numerator_attr_names: &["Neutralization Amount"],
        denominator_attr_names: &["Activation Cost"],
    },
];

static RESOLVED_FORMULAS: OnceLock<Vec<ResolvedVirtualAttributeFormula>> = OnceLock::new();

pub fn initialize_virtual_attributes(name_to_id_resolver: &dyn Fn(&str) -> DogmaAttributeId) {
    let resolved_formulas: Vec<ResolvedVirtualAttributeFormula> = VIRTUAL_FORMULAS
        .iter()
        .map(|formula| {
            let numerator_attr_ids: Vec<DogmaAttributeId> = formula
                .numerator_attr_names
                .iter()
                .map(|name| name_to_id_resolver(name))
                .collect();

            let denominator_attr_ids: Vec<DogmaAttributeId> = formula
                .denominator_attr_names
                .iter()
                .map(|name| name_to_id_resolver(name))
                .collect();

            ResolvedVirtualAttributeFormula {
                virtual_id: formula.virtual_id,
                name: formula.name,
                high_is_good: formula.high_is_good,
                numerator_attr_ids,
                denominator_attr_ids,
            }
        })
        .collect();

    let _ = RESOLVED_FORMULAS.set(resolved_formulas);
}

fn get_resolved_formulas() -> &'static Vec<ResolvedVirtualAttributeFormula> {
    RESOLVED_FORMULAS
        .get()
        .expect("virtual attributes not initialized")
}

pub fn append_attribute_values(attributes: &mut Vec<AttributeValue>) {
    let resolved_formulas = get_resolved_formulas();

    for formula in resolved_formulas {
        let mut numerator_product = 1.0;
        let mut missing_numerators = 0;

        for numerator_id in &formula.numerator_attr_ids {
            let mut found = false;
            for attr in attributes.iter() {
                if attr.id == *numerator_id {
                    numerator_product *= attr.value;
                    found = true;
                    break;
                }
            }
            if !found {
                missing_numerators += 1;
            }
        }

        let mut denominator_product = 1.0;
        let mut missing_denominators = 0;

        for denominator_id in &formula.denominator_attr_ids {
            let mut found = false;
            for attr in attributes.iter() {
                if attr.id == *denominator_id {
                    denominator_product *= attr.value;
                    found = true;
                    break;
                }
            }
            if !found {
                missing_denominators += 1;
            }
        }

        let can_calculate =
            missing_numerators == 0 && missing_denominators == 0 && denominator_product != 0.0;

        if can_calculate {
            attributes.push(AttributeValue {
                id: formula.virtual_id,
                value: numerator_product / denominator_product,
            });
        }
    }
}

pub fn append_min_max_attribute_values(attributes: &mut Vec<AttributeRange>) {
    let resolved_formulas = get_resolved_formulas();

    for formula in resolved_formulas {
        let mut min_numerator_product = 1.0;
        let mut max_numerator_product = 1.0;
        let mut missing_numerators = 0;

        for numerator_id in &formula.numerator_attr_ids {
            let mut found_attribute = false;
            for attr in attributes.iter() {
                if attr.id == *numerator_id {
                    min_numerator_product *= attr.min;
                    max_numerator_product *= attr.max;
                    found_attribute = true;
                    break;
                }
            }

            if !found_attribute {
                missing_numerators += 1;
            }
        }

        let mut min_denominator_product = 1.0;
        let mut max_denominator_product = 1.0;
        let mut missing_denominators = 0;

        for denominator_id in &formula.denominator_attr_ids {
            let mut found_attribute = false;
            for attr in attributes.iter() {
                if attr.id == *denominator_id {
                    min_denominator_product *= attr.min;
                    max_denominator_product *= attr.max;
                    found_attribute = true;
                    break;
                }
            }
            if !found_attribute {
                missing_denominators += 1;
            }
        }

        let can_calculate = missing_numerators == 0 && missing_denominators == 0;

        if can_calculate {
            if min_denominator_product != 0.0 && max_denominator_product != 0.0 {
                let v1 = min_numerator_product / max_denominator_product;
                let v2 = max_numerator_product / min_denominator_product;

                let min = v1.min(v2);
                let max = v1.max(v2);

                attributes.push(AttributeRange {
                    id: formula.virtual_id,
                    min,
                    max,
                })
            }
        }
    }
}

pub fn append_varying_attributes(attributes: &mut Vec<VaryingAttribute>) {
    let resolved_formulas = get_resolved_formulas();

    for formula in resolved_formulas {
        let mut missing_numerators = 0;

        for numerator_id in &formula.numerator_attr_ids {
            let mut found = false;
            for attr in attributes.iter() {
                if attr.id == *numerator_id {
                    found = true;
                    break;
                }
            }
            if !found {
                missing_numerators += 1;
            }
        }

        let mut missing_denominators = 0;

        for denominator_id in &formula.denominator_attr_ids {
            let mut found = false;
            for attr in attributes.iter() {
                if attr.id == *denominator_id {
                    found = true;
                    break;
                }
            }
            if !found {
                missing_denominators += 1;
            }
        }

        let can_calculate = missing_numerators == 0 && missing_denominators == 0;

        if can_calculate {
            attributes.push(VaryingAttribute {
                id: formula.virtual_id,
                name: formula.name.to_string(),
                high_is_good: formula.high_is_good,
            });
        }
    }
}
