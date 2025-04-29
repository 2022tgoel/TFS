use anyhow::Result;
use dashmap::DashMap;
use dashmap::mapref::one::RefMut;
use std::cmp::max;

struct ChunkVersions {
    clean_version: Option<u64>,
    dirty_versions: Vec<u64>,
}

#[derive(PartialEq)]
pub enum State {
    Commited,
    Aborted,
}

pub struct ChunkVersionTable {
    inner: DashMap<(u64, u64), ChunkVersions>,
    name: String,
}

/// Invariants:
/// 1. The clean version is always increasing.
/// 2. The dirty versions are always increasing. (1 and 2 help guarantee this)

impl ChunkVersions {
    pub fn new_with_version(version: u64) -> Self {
        Self {
            clean_version: None,
            dirty_versions: Vec::from([version]),
        }
    }

    pub fn new() -> Self {
        Self {
            clean_version: None,
            dirty_versions: Vec::new(),
        }
    }
}

impl ChunkVersionTable {
    pub fn new(name: String) -> Self {
        Self {
            inner: DashMap::new(),
            name,
        }
    }

    // get a version that is higher than all the existing versions
    pub fn get_new_version(&self, file_id: u64, chunk_id: u64) -> u64 {
        let mut versions = self.inner.get_mut(&(file_id, chunk_id));
        let version = if let Some(ref mut versions) = versions {
            let mut version = if let Some(v) = versions.clean_version {
                v
            } else {
                0
            };
            // get the highest dirty version
            version = if let Some(v) = versions.dirty_versions.iter().max() {
                max(*v, version)
            } else {
                version
            } + 1;
            versions.dirty_versions.push(version);
            version
        } else {
            self.inner
                .insert((file_id, chunk_id), ChunkVersions::new_with_version(0));
            0
        };
        drop(versions);
        version
    }

    pub fn insert_version(&self, file_id: u64, chunk_id: u64, version: u64) -> Result<()> {
        let mut versions = self
            .inner
            .entry((file_id, chunk_id))
            .or_insert(ChunkVersions::new());
        if let Some(v) = versions.clean_version {
            if v > version {
                return Err(anyhow::anyhow!("Version is lower than the clean version"));
            }
        }
        versions.dirty_versions.push(version);
        Ok(())
    }

    pub fn get_version(&self, file_id: u64, chunk_id: u64) -> Option<u64> {
        if let Some(versions) = self.inner.get(&(file_id, chunk_id)) {
            if let Some(v) = versions.clean_version {
                if versions.dirty_versions.iter().any(|x| *x > v) {
                    return None;
                } else {
                    return Some(v);
                }
            }
        }
        None
    }

    pub fn remove_dirty_version(&self, file_id: u64, chunk_id: u64, version: u64) -> Result<()> {
        let mut versions = self.inner.get_mut(&(file_id, chunk_id));
        if let Some(mut versions) = versions {
            versions.dirty_versions.retain(|&x| x != version);
        }
        Ok(())
    }

    pub fn commit_version(&self, file_id: u64, chunk_id: u64, version: u64) -> Result<State> {
        let mut versions: RefMut<'_, (u64, u64), ChunkVersions> = self
            .inner
            .get_mut(&(file_id, chunk_id))
            .ok_or(anyhow::anyhow!("Chunk version table in an invalid state"))?;

        if let Some(v) = versions.clean_version {
            if v > version {
                return Ok(State::Aborted);
            } else {
                std::fs::remove_file(format!(
                    "/scratch/files/{}/{}_{}_{}.txt",
                    self.name, file_id, chunk_id, v
                ))?;
            }
        }

        versions.clean_version = Some(version);
        versions.dirty_versions.retain(|&x| x != version);
        Ok(State::Commited)
    }
}
