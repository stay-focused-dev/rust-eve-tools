use crate::{DynamicId, DynamicItem};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_cbor;
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Serialize, Deserialize)]
pub struct DynamicsDb {
    db: BTreeMap<DynamicId, DynamicItem>,
    dir: String,
    pub last_stored_at: DateTime<Utc>,
    pub last_updated_at: DateTime<Utc>,
}

impl DynamicsDb {
    pub fn from_dir(dir: &str) -> Result<DynamicsDb, std::io::Error> {
        let file_path = Self::last_file(dir);
        let path = Path::new(&file_path);
        if path.exists() {
            let cbor_data = std::fs::read(&path)?;

            match serde_cbor::from_slice::<DynamicsDb>(&cbor_data) {
                Ok(db) => {
                    println!("sucessfully deserialized DynamicItemDb");
                    return Ok(db);
                }
                Err(e) => {
                    eprintln!("Error deserializing DynamicItemDb: {}", e);

                    match serde_cbor::from_slice::<BTreeMap<DynamicId, DynamicItem>>(&cbor_data) {
                        Ok(db_map) => {
                            eprintln!("sucessfully deserialized just the BTreeMap portion");
                            return Ok(DynamicsDb {
                                db: db_map,
                                dir: dir.to_string(),
                                last_updated_at: Utc::now(),
                                last_stored_at: Utc::now(),
                            });
                        }
                        Err(e2) => {
                            eprintln!("error deserializing BTreeMap: {}", e2);
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("failed to deserialize the database file: {e}"),
                            ));
                        }
                    }
                }
            }
        }

        let now = Utc::now();
        Ok(DynamicsDb {
            db: BTreeMap::new(),
            dir: dir.to_string().clone(),
            last_stored_at: now,
            last_updated_at: now,
        })
    }

    pub fn add(&mut self, id: DynamicId, item: DynamicItem) {
        self.db.insert(id, item);
        let old_updated = self.last_updated_at;
        self.last_updated_at = Utc::now();
        println!(
            "➕ Added dynamic {:?}, updated timestamp from {} to {}",
            id, old_updated, self.last_updated_at
        );
    }

    pub fn store(&mut self) -> Result<(), std::io::Error> {
        println!(
            "🔍 Store called - last_stored: {}, last_updated: {}, need_store: {}",
            self.last_stored_at,
            self.last_updated_at,
            self.last_stored_at < self.last_updated_at
        );

        if self.last_stored_at < self.last_updated_at {
            self.last_stored_at = Utc::now();
            let file_path = Self::last_file(&self.dir);
            let temp_path = format!("{file_path}.tmp");
            let encoded = serde_cbor::ser::to_vec(&self)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            std::fs::write(&temp_path, encoded)?;
            std::fs::rename(temp_path, file_path)?;
            println!(
                "✅ Dynamics stored successfully with {} items",
                self.db.len()
            );
        } else {
            println!(
                "⏭️ Using old file - no changes to store (count: {})",
                self.db.len()
            );
        }
        Ok(())
    }

    pub fn contain(&self, id: DynamicId) -> bool {
        self.db.contains_key(&id)
    }

    pub fn get(&self, id: DynamicId) -> Option<&DynamicItem> {
        self.db.get(&id)
    }

    pub fn len(&self) -> usize {
        self.db.len()
    }

    fn last_file(dir: &str) -> String {
        format!("{}/dynamics/dynamics.cbor", dir)
    }
}
