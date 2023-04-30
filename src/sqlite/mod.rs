pub mod actor;
pub mod structure;
use serde::{Serialize, Deserialize};


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DBTxn {
    conditions: Vec<String>, // Must be of the form "EXISTS(SELECT ...)"
    deletes: Vec<String>, // Must be of the form "DELETE FROM ..."
    puts: Vec<String>, // Must be of the form "REPLACE INTO "
    query: Option<String>, // Must be of the form "SELECT ..."
    snapshot: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DBTxnResult {
    reads: Vec<String>,
    committed: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
enum LogEntry {
    Txn(DBTxn),
    Completion(usize),
}


impl DBTxn {
    /// Create new transaction.
    pub fn new(conditions: Vec<String>, deletes: Vec<String>, puts: Vec<String>, query: Option<String>) -> Self {
        DBTxn { conditions, deletes, puts, query, snapshot: false }
    }

    /// Make a new snapshot txn.
    pub fn new_snapshot(query: String) -> DBTxn {
        DBTxn {
            conditions: vec![],
            deletes: vec![],
            puts: vec![],
            query: Some(query),
            snapshot: true,
        }
    }

    /// Check if is snapshot.
    pub fn is_snapshot(&self) -> bool {
        self.snapshot
    }
}


