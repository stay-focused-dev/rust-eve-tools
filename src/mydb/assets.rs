use crate::eve::{AssetItem, CharacterId, ItemId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_cbor;
use std::collections::BTreeMap;
use std::io;
use std::path::Path;

#[derive(Serialize, Deserialize)]
pub struct AllAssetsDb {
    dir: String,
    db: BTreeMap<CharacterId, AssetsDb>,
}

impl AllAssetsDb {
    pub fn from_dir(dir: &str) -> Result<AllAssetsDb, std::io::Error> {
        let path = Path::new(dir);
        if path.exists() {
            if !path.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("Path {} is not a directory", dir),
                ));
            }
        } else {
            std::fs::create_dir_all(dir)?;
        }

        Ok(AllAssetsDb {
            dir: dir.to_string(),
            db: BTreeMap::new(),
        })
    }

    fn add_character(&mut self, character_id: CharacterId) -> Result<(), std::io::Error> {
        let path = Path::new(&self.dir)
            .join("assets")
            .join(character_id.to_string());
        if !path.exists() {
            std::fs::create_dir_all(&path)?;
        }

        if self.db.contains_key(&character_id) {
            return Ok(());
        }

        let dir = path.to_str().unwrap();

        self.db.insert(character_id, AssetsDb::from_dir(dir)?);

        Ok(())
    }

    pub fn add(
        &mut self,
        character_id: CharacterId,
        item: AssetItem,
    ) -> Result<(), std::io::Error> {
        if !self.db.contains_key(&character_id) {
            self.add_character(character_id)?;
        }
        let db = self.db.get_mut(&character_id).unwrap();
        db.add(item);
        Ok(())
    }

    pub fn store(&mut self) -> Result<(), std::io::Error> {
        for (character_id, db) in self.db.iter_mut() {
            println!("Storing assets for character {}", character_id);
            db.store()?;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub struct AssetsDb {
    db: BTreeMap<ItemId, AssetItem>,
    dir: String,
    last_stored_at: DateTime<Utc>,
    last_updated_at: DateTime<Utc>,
}

impl AssetsDb {
    pub fn from_dir(dir: &str) -> Result<AssetsDb, std::io::Error> {
        let now = Utc::now();
        Ok(AssetsDb {
            db: BTreeMap::new(),
            dir: dir.to_string(),
            last_stored_at: now,
            last_updated_at: now,
        })
    }

    pub fn add(&mut self, item: AssetItem) {
        self.db.insert(item.item_id, item);
        self.last_updated_at = Utc::now();
    }

    pub fn store(&mut self) -> Result<(), std::io::Error> {
        if self.last_stored_at < self.last_updated_at {
            self.last_stored_at = Utc::now();
            let file_path = Self::last_file(&self.dir);
            let temp_path = format!("{file_path}.tmp");
            let encoded = serde_cbor::ser::to_vec(&self)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            std::fs::write(&temp_path, encoded)?;
            std::fs::rename(temp_path, file_path)?;
        } else {
            println!("Using old file")
        }

        Ok(())
    }

    fn last_file(dir: &str) -> String {
        format!("{}/assets.cbor", dir)
    }
}
