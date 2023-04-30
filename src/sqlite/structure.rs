use super::{DBTxn, DBTxnResult, LogEntry};
use std::{sync::Arc, collections::{HashMap, BTreeMap}};
use obelisk::PersistentLog;
use tokio::sync::{Mutex, RwLock, oneshot};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;



pub struct SqliteStructure {
    plog: Arc<PersistentLog>,
    inner: Arc<Mutex<SqliteStructureInner>>,
    execute_lock: Arc<Mutex<()>>,
    db: Arc<RwLock<SqliteDB>>,
}

struct SqliteStructureInner {
    txn_queue: Vec<(DBTxn, usize)>,
    last_executed_lsn: usize,
    txn_results: HashMap<usize, DBTxnResult>,
}

struct SqliteDB {
    pub pool: Pool<SqliteConnectionManager>,
}

impl SqliteDB {
    /// Get sqlite db.
    pub async fn new() -> Self {
        let shared_prefix = obelisk::common::shared_storage_prefix();
        let db_dir = format!("{shared_prefix}/essai_dir");
        let db_file = format!("{db_dir}/essai.db");
        let manager = r2d2_sqlite::SqliteConnectionManager::file(db_file.clone());
        tokio::task::block_in_place(move || {
            let pool = match r2d2::Pool::builder().max_size(10).build(manager) {
                Ok(pool) => pool,
                Err(x) => {
                    println!("SqliteDB Builder: {x:?}");
                    std::process::exit(1);
                }
            };
            
            let conn = pool.get().unwrap();
            match conn.pragma_update(None, "locking_mode", "exclusive") {
                Ok(_) => {}
                Err(x) => {
                    println!("Pragma 1: {x:?}");
                    std::process::exit(1);
                }
            }
            match conn.pragma_update(None, "journal_mode", "wal") {
                Ok(_) => {}
                Err(x) => {
                    println!("Pragma 2: {x:?}");
                    std::process::exit(1);
                }
            }
            match conn.execute("CREATE TABLE IF NOT EXISTS last_lsn (unique_row INT PRIMARY KEY, lsn BIGINT)", ()) {
                Ok(_) => {},
                Err(x) => {
                    println!("Create error: {x:?}");
                    std::process::exit(1);
                }
            }
            match conn.execute("CREATE TABLE IF NOT EXISTS keyvalues (key TEXT PRIMARY KEY, value TEXT NOT NULL)", ()) {
                Ok(_) => {},
                Err(x) => {
                    println!("Create error: {x:?}");
                    std::process::exit(1);
                }
            }
            SqliteDB { pool }  
        })
    }
}

impl SqliteStructure {
    /// Create sqlite structure.
    pub async fn new(_name: &str, plog: Arc<PersistentLog>) -> Self {
        let db = SqliteDB::new().await;
        let inner = SqliteStructureInner {
             last_executed_lsn: 0,
             txn_queue: vec![],
             txn_results: HashMap::new(),
        };
        let structure = SqliteStructure {
            plog,
            inner: Arc::new(Mutex::new(inner)),
            execute_lock: Arc::new(Mutex::new(())),
            db: Arc::new(RwLock::new(db)),
        };
        structure.recover().await;
        structure
    }


    /// Execute a transaction.
    pub async fn execute(&self, db_txn: DBTxn) {
        let entry = LogEntry::Txn(db_txn.clone());
        let entry = bincode::serialize(&entry).unwrap();
        let lsn = {
            let mut inner = self.inner.lock().await;    
            let lsn = self.plog.enqueue(entry).await;
            inner.txn_queue.push((db_txn, lsn));
            lsn
        };
        let _res = self.execute_queue(lsn).await;
    }

    /// Run a query.
    pub async fn query(&self, query: String) -> DBTxnResult {
        let db = self.db.read().await;
        let pool = db.pool.clone();
        let results = tokio::task::spawn_blocking(move || {
            let conn = pool.get().unwrap();
            let mut stmt = conn.prepare(&query).unwrap();
            let res_iter = stmt.query_map([], |row| {
                let res: String = row.get(0).unwrap();
                Ok(res)
            }).unwrap();
            let mut results = Vec::new();
            for res in res_iter {
                results.push(res.unwrap());
            }
            results
        }).await.unwrap();
        DBTxnResult {
            reads: results,
            committed: true,
        }
    }

    /// Execute the txns in the queue, potentially for recovery.
    pub async fn perform_execute_queue(&self, txn_queue: Vec<(DBTxn, usize)>, recovering: bool) {
        let last_lsn = txn_queue.last().unwrap().1;
        // Flush if not recovering.
        if !recovering {
            self.plog.flush_at(Some(last_lsn)).await;
        }
        // Now execute in order in a deterministic fashion.
        let db = self.db.read().await;
        let pool = db.pool.clone();
        let (send_ch, rcv_ch) = oneshot::channel();
        tokio::task::spawn_blocking(move || {
            let mut conn = pool.get().unwrap();
            let mut txn = conn.transaction().unwrap();
            let prev_executed_lsn = txn.query_row("SELECT lsn FROM last_lsn", [], |row| {
                row.get(0)
            });
            let prev_executed_lsn: usize = match prev_executed_lsn {
                Ok(prev_executed_lsn) => prev_executed_lsn,
                Err(rusqlite::Error::QueryReturnedNoRows) => 0,
                err => {
                    println!("Error: {err:?}");
                    std::process::exit(1);
                } 
            };
            let mut results = HashMap::new();
            for (db_txn, lsn) in txn_queue {
                if prev_executed_lsn > lsn {
                    continue;
                }
                // TODO: Instead of all the unwrap(), check failure and rollback savepoint.
                let svp = txn.savepoint().unwrap();
                // Check conditions.
                let mut satisfies_conditions = true;
                for condition in db_txn.conditions {
                    let res: usize = svp.query_row(&condition, [], |row| {
                        row.get(0)
                    }).unwrap();
                    if res > 0 {
                        satisfies_conditions = false;
                        break;
                    }
                }
                if !satisfies_conditions {
                    if !recovering {
                        let res = DBTxnResult {
                            committed: false,
                            reads: vec![],
                        };
                        results.insert(lsn, res);    
                    }
                    continue;
                }
                // Do the deletes.
                for delete in db_txn.deletes {
                    svp.execute(&delete, []).unwrap();
                }
                // Do the updates.
                for update in db_txn.puts {
                    svp.execute(&update, []).unwrap();
                }
                // Do the reads.
                if !recovering {
                    let reads = if let Some(query) = db_txn.query {
                        let mut stmt = svp.prepare(&query).unwrap();
                        let res_iter = stmt.query_map([], |row| {
                            let res: String = row.get(0).unwrap();
                            Ok(res)
                        }).unwrap();
                        let mut reads = Vec::new();
                        for res in res_iter {
                            reads.push(res.unwrap());
                        }
                        reads
                    } else {
                        vec![]
                    };
                    let res = DBTxnResult {
                        committed: true,
                        reads,
                    };
                    results.insert(lsn, res);    
                }
                svp.commit().unwrap();
            }
            if last_lsn > prev_executed_lsn {
                txn.execute("REPLACE INTO last_lsn VALUES (0, ?)", [last_lsn]).unwrap();
            }
            send_ch.send(results).unwrap();
            txn.commit().unwrap();
        });
        let results = rcv_ch.await.unwrap();
        let completion = LogEntry::Completion(last_lsn);
        let entry = bincode::serialize(&completion).unwrap();
        self.plog.enqueue(entry).await;
        // Mark results.
        {
            let mut inner = self.inner.lock().await;
            if last_lsn > inner.last_executed_lsn { // May not always be true during recovery.
                inner.last_executed_lsn = last_lsn;
            }
            if !recovering {
                for (lsn, db_txn_result) in results {
                    inner.txn_results.insert(lsn, db_txn_result);
                }
            }
        }
    }

    /// Perform the txns in the queue, if the given lsn hasn't been executed yet.
    pub async fn execute_queue(&self, at_lsn: usize) -> Option<DBTxnResult> {
        let _l = self.execute_lock.lock().await;
        // Drain queue.
        let txn_queue: Vec<(DBTxn, usize)> = {
            let mut inner = self.inner.lock().await;
            if inner.last_executed_lsn >= at_lsn {
                // Get already computed result.
                return inner.txn_results.remove(&at_lsn);
            }
            inner.txn_queue.drain(..).collect()
        };
        assert!(!txn_queue.is_empty());
        self.perform_execute_queue(txn_queue, false).await;
        // Get result.
        {
            let mut inner = self.inner.lock().await;
            return inner.txn_results.remove(&at_lsn);
        }
    }

    /// Truncate log file.
    pub async fn truncate_log(&self) {
        let mut curr_start_lsn = 0;
        let mut to_replay: BTreeMap<usize, DBTxn> = BTreeMap::new();
        println!("SqliteStructure::recover(). Starting recovery.");
        loop {
            let entries = self.plog.replay(curr_start_lsn).await;
            let entries = match entries {
                Ok(entries) => entries,
                Err(x) => {
                    println!("SqliteStructure::recover(): {x:?}");
                    // Must own db at this point.
                    std::process::exit(1);
                }
            };
            if entries.is_empty() {
                break;
            }
            for (lsn, entry) in entries {
                curr_start_lsn = lsn;
                let entry: LogEntry = bincode::deserialize(&entry).unwrap();
                println!("SqliteStructure::recover(): Found entry ({lsn}): {entry:?}.");
                match entry {
                    LogEntry::Completion(lsn) => {
                        let mut to_remove = Vec::new();
                        for key in to_replay.keys() {
                            if *key <= lsn {
                                to_remove.push(*key);
                            }
                        }
                        for key in to_remove {
                            to_replay.remove(&key);
                        }
                    }
                    LogEntry::Txn(db_txn) => {
                        to_replay.insert(lsn, db_txn);
                    }
                }
            }
        }
    }

    /// Perform recovery.
    pub async fn recover(&self) {
        let mut curr_start_lsn = 0;
        let mut to_replay: BTreeMap<usize, DBTxn> = BTreeMap::new();
        println!("SqliteStructure::recover(). Starting recovery.");
        loop {
            let entries = self.plog.replay(curr_start_lsn).await;
            let entries = match entries {
                Ok(entries) => entries,
                Err(x) => {
                    println!("SqliteStructure::recover(): {x:?}");
                    // Must own db at this point.
                    std::process::exit(1);
                }
            };
            if entries.is_empty() {
                break;
            }
            for (lsn, entry) in entries {
                curr_start_lsn = lsn;
                let entry: LogEntry = bincode::deserialize(&entry).unwrap();
                println!("SqliteStructure::recover(): Found entry ({lsn}): {entry:?}.");
                match entry {
                    LogEntry::Completion(lsn) => {
                        let mut to_remove = Vec::new();
                        for key in to_replay.keys() {
                            if *key <= lsn {
                                to_remove.push(*key);
                            }
                        }
                        for key in to_remove {
                            to_replay.remove(&key);
                        }
                        let mut inner = self.inner.lock().await;
                        inner.last_executed_lsn = lsn;
                    }
                    LogEntry::Txn(db_txn) => {
                        to_replay.insert(lsn, db_txn);
                    }
                }
            }
        }
        let txn_queue: Vec<(DBTxn, usize)> = to_replay.into_iter().map(|(k, v)| (v, k)).collect();
        if !txn_queue.is_empty() {
            self.perform_execute_queue(txn_queue, true).await;
            self.plog.flush().await;
        }
    }
}