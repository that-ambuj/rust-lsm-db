/// MemTable holds a sorted list of the latest written records
///
/// Writes are dublicated to the WAL(Write Ahead Log) for the
/// recovery of the MemTable in case of a restart.
///
/// Memtables have a max capacity and when that is reached we
/// flush the MemTable to the disk as a Table(SSTable).
///
/// Entries are stored in a Vector instead of a HashMap to
/// support scans.
pub struct MemTable {
    entries: Vec<MemTableEntry>,
    size: usize,
}

/// A MemTable Entry
pub struct MemTableEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp_ms: u128,
    pub is_deleted: bool,
}

impl MemTable {
    /// Creates a new empty MemTable
    pub fn new() -> MemTable {
        MemTable {
            entries: Vec::new(),
            size: 0,
        }
    }

    /// Sets a Key-Value pair in the MemTable.
    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp_ms: u128) {
        let entry = MemTableEntry {
            key: key.to_owned(),
            value: Some(value.to_owned()),
            timestamp_ms,
            is_deleted: false,
        };

        match self.get_index(key) {
            Ok(idx) => {
                // If a value existed on the is_deleted record,
                // then add the difference of the new and old Value
                // to the MemTable's size.
                if let Some(v) = self.entries[idx].value.as_ref() {
                    if value.len() < v.len() {
                        self.size -= v.len() - value.len();
                    } else {
                        self.size += value.len() - v.len();
                    }
                }
                self.entries[idx] = entry;
            }
            Err(idx) => {
                // Increase the size of the MemTable by the size of the Key, Value, Timestamp(16
                // bytes) and Tombstone(1 byte).
                self.size += key.len() + value.len() + 16 + 1;
                self.entries.insert(idx, entry)
            }
        }
    }

    /// Deletes a Key-Value pair in the MemTable
    ///
    /// This is achieved using tombstones
    pub fn delete(&mut self, key: &[u8], timestamp_ms: u128) {
        let entry = MemTableEntry {
            key: key.to_owned(),
            value: None,
            timestamp_ms,
            is_deleted: true,
        };

        match self.get_index(key) {
            Ok(idx) => {
                if let Some(value) = self.entries[idx].value.as_ref() {
                    self.size -= value.len();
                }
                self.entries[idx] = entry;
            }
            Err(idx) => {
                // Increase the size of the MemTable by the size of the Key, Timestamp(16 bytes)
                // and Tombstone(1 byte).
                self.size += key.len() + 16 + 1;
                self.entries.insert(idx, entry)
            }
        }
    }

    /// Get a Key-Value pair from the MemTable
    ///
    /// If no record with the same key exists in the MemTable, return None
    pub fn get(&self, key: &[u8]) -> Option<&MemTableEntry> {
        if let Ok(idx) = self.get_index(key) {
            return Some(&self.entries[idx]);
        }
        None
    }

    /// Performs Binary Search to find a record in the MemTable
    ///
    /// If the record is found `[Result::Ok]` is returned, with
    /// the index of record. If the record is not found then
    /// `[Result::Err]` is returned, with the index to insert
    /// the record at
    fn get_index(&self, key: &[u8]) -> Result<usize, usize> {
        self.entries
            .binary_search_by_key(&key, |e| e.key.as_slice())
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn entries(&self) -> &[MemTableEntry] {
        &self.entries
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

#[cfg(test)]
mod tests {
    use crate::mem_table::MemTable;

    #[test]
    fn test_mem_table_put_start() {
        let mut table = MemTable::new();
        table.set(b"Lime", b"Lime Smoothie", 0); // 17 + 16 + 1
        table.set(b"Orange", b"Orange Smoothie", 10); // 21 + 16 + 1

        table.set(b"Apple", b"Apple Smoothie", 20); // 19 + 16 + 1

        assert_eq!(table.entries[0].key, b"Apple");
        assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Apple Smoothie");
        assert_eq!(table.entries[0].timestamp_ms, 20);
        assert_eq!(table.entries[0].is_deleted, false);
        assert_eq!(table.entries[1].key, b"Lime");
        assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Lime Smoothie");
        assert_eq!(table.entries[1].timestamp_ms, 0);
        assert_eq!(table.entries[1].is_deleted, false);
        assert_eq!(table.entries[2].key, b"Orange");
        assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Orange Smoothie");
        assert_eq!(table.entries[2].timestamp_ms, 10);
        assert_eq!(table.entries[2].is_deleted, false);

        assert_eq!(table.size, 108);
    }

    #[test]
    fn test_mem_table_put_middle() {
        let mut table = MemTable::new();
        table.set(b"Apple", b"Apple Smoothie", 0);
        table.set(b"Orange", b"Orange Smoothie", 10);

        table.set(b"Lime", b"Lime Smoothie", 20);

        assert_eq!(table.entries[0].key, b"Apple");
        assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Apple Smoothie");
        assert_eq!(table.entries[0].timestamp_ms, 0);
        assert_eq!(table.entries[0].is_deleted, false);
        assert_eq!(table.entries[1].key, b"Lime");
        assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Lime Smoothie");
        assert_eq!(table.entries[1].timestamp_ms, 20);
        assert_eq!(table.entries[1].is_deleted, false);
        assert_eq!(table.entries[2].key, b"Orange");
        assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Orange Smoothie");
        assert_eq!(table.entries[2].timestamp_ms, 10);
        assert_eq!(table.entries[2].is_deleted, false);

        assert_eq!(table.size, 108);
    }

    #[test]
    fn test_mem_table_put_end() {
        let mut table = MemTable::new();
        table.set(b"Apple", b"Apple Smoothie", 0);
        table.set(b"Lime", b"Lime Smoothie", 10);

        table.set(b"Orange", b"Orange Smoothie", 20);

        assert_eq!(table.entries[0].key, b"Apple");
        assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Apple Smoothie");
        assert_eq!(table.entries[0].timestamp_ms, 0);
        assert_eq!(table.entries[0].is_deleted, false);
        assert_eq!(table.entries[1].key, b"Lime");
        assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Lime Smoothie");
        assert_eq!(table.entries[1].timestamp_ms, 10);
        assert_eq!(table.entries[1].is_deleted, false);
        assert_eq!(table.entries[2].key, b"Orange");
        assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Orange Smoothie");
        assert_eq!(table.entries[2].timestamp_ms, 20);
        assert_eq!(table.entries[2].is_deleted, false);

        assert_eq!(table.size, 108);
    }

    #[test]
    fn test_mem_table_put_overwrite() {
        let mut table = MemTable::new();
        table.set(b"Apple", b"Apple Smoothie", 0);
        table.set(b"Lime", b"Lime Smoothie", 10);
        table.set(b"Orange", b"Orange Smoothie", 20);

        table.set(b"Lime", b"A sour fruit", 30);

        assert_eq!(table.entries[0].key, b"Apple");
        assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Apple Smoothie");
        assert_eq!(table.entries[0].timestamp_ms, 0);
        assert_eq!(table.entries[0].is_deleted, false);
        assert_eq!(table.entries[1].key, b"Lime");
        assert_eq!(table.entries[1].value.as_ref().unwrap(), b"A sour fruit");
        assert_eq!(table.entries[1].timestamp_ms, 30);
        assert_eq!(table.entries[1].is_deleted, false);
        assert_eq!(table.entries[2].key, b"Orange");
        assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Orange Smoothie");
        assert_eq!(table.entries[2].timestamp_ms, 20);
        assert_eq!(table.entries[2].is_deleted, false);

        assert_eq!(table.size, 107);
    }

    #[test]
    fn test_mem_table_get_exists() {
        let mut table = MemTable::new();
        table.set(b"Apple", b"Apple Smoothie", 0);
        table.set(b"Lime", b"Lime Smoothie", 10);
        table.set(b"Orange", b"Orange Smoothie", 20);

        let entry = table.get(b"Orange").unwrap();

        assert_eq!(entry.key, b"Orange");
        assert_eq!(entry.value.as_ref().unwrap(), b"Orange Smoothie");
        assert_eq!(entry.timestamp_ms, 20);
    }

    #[test]
    fn test_mem_table_get_not_exists() {
        let mut table = MemTable::new();
        table.set(b"Apple", b"Apple Smoothie", 0);
        table.set(b"Lime", b"Lime Smoothie", 0);
        table.set(b"Orange", b"Orange Smoothie", 0);

        let res = table.get(b"Potato");
        assert_eq!(res.is_some(), false);
    }

    #[test]
    fn test_mem_table_delete_exists() {
        let mut table = MemTable::new();
        table.set(b"Apple", b"Apple Smoothie", 0);

        table.delete(b"Apple", 10);

        let res = table.get(b"Apple").unwrap();
        assert_eq!(res.key, b"Apple");
        assert_eq!(res.value, None);
        assert_eq!(res.timestamp_ms, 10);
        assert_eq!(res.is_deleted, true);

        assert_eq!(table.entries[0].key, b"Apple");
        assert_eq!(table.entries[0].value, None);
        assert_eq!(table.entries[0].timestamp_ms, 10);
        assert_eq!(table.entries[0].is_deleted, true);

        assert_eq!(table.size, 22);
    }

    #[test]
    fn test_mem_table_delete_empty() {
        let mut table = MemTable::new();

        table.delete(b"Apple", 10);

        let res = table.get(b"Apple").unwrap();
        assert_eq!(res.key, b"Apple");
        assert_eq!(res.value, None);
        assert_eq!(res.timestamp_ms, 10);
        assert_eq!(res.is_deleted, true);

        assert_eq!(table.entries[0].key, b"Apple");
        assert_eq!(table.entries[0].value, None);
        assert_eq!(table.entries[0].timestamp_ms, 10);
        assert_eq!(table.entries[0].is_deleted, true);

        assert_eq!(table.size, 22);
    }
}
