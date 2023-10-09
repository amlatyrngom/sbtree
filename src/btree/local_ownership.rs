use base64::{engine::general_purpose, Engine as _};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

#[derive(Clone)]
pub struct LocalOwnership {
    incarnation_num: usize,
    pool: Pool<SqliteConnectionManager>,
    db_file: String,
}

impl LocalOwnership {
    /// Create database.
    pub async fn new(ownership_key: &str) -> Self {
        tokio::task::block_in_place(move || Self::new_sync(ownership_key))
    }

    /// Fetch a block.
    pub async fn fetch_raw_block(&self, block_id: &str) -> Option<Vec<u8>> {
        tokio::task::block_in_place(move || loop {
            let conn = match self.pool.get() {
                Ok(conn) => conn,
                Err(x) => {
                    println!("{x:?}");
                    continue;
                }
            };
            // let start_time = std::time::Instant::now();
            let resp = conn.query_row(
                "SELECT data FROM blocks WHERE block_id=?",
                [block_id],
                |row| row.get(0),
            );
            // let end_time = std::time::Instant::now();
            // let duration = end_time.duration_since(start_time);
            // println!("Fetch block duration: {duration:?}");
            match resp {
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    break None;
                }
                Err(x) => {
                    println!("{x:?}");
                    continue;
                }
                Ok(data) => {
                    break Some(data);
                }
            }
        })
    }

    /// Delete block.
    pub async fn delete_block(&self, block_id: &str) {
        tokio::task::block_in_place(move || loop {
            let mut conn = match self.pool.get() {
                Ok(conn) => conn,
                Err(x) => {
                    println!("DeleteBlock({block_id}, conn): {x:?}");
                    std::process::exit(1);
                }
            };
            let txn = match conn.transaction() {
                Ok(txn) => txn,
                Err(x) => {
                    println!("DeleteBlock({block_id}, txn): {x:?}");
                    std::process::exit(1);
                }
            };
            let incarn_num = txn.query_row("SELECT incarnation_num FROM incarnation", [], |row| {
                row.get(0)
            });
            match incarn_num {
                Err(x) => {
                    println!("DeleteBlock({block_id}, incarn): {x:?}");
                    std::process::exit(1);
                }
                Ok(incarn_num) => {
                    let incarn_num: usize = incarn_num;
                    if incarn_num != self.incarnation_num {
                        let _ = txn.commit();
                        println!("Lost database ownership");
                        std::process::exit(1);
                    }
                }
            }
            let resp = txn.execute("DELETE FROM blocks WHERE block_id = ?", [block_id]);
            match resp {
                Err(x) => {
                    println!("DeleteBlock({block_id}, exec): {x:?}");
                    std::process::exit(1);
                }
                Ok(_executed) => {}
            }
            match txn.commit() {
                Err(x) => {
                    println!("DeleteBlock({block_id}, commit): {x:?}");
                    std::process::exit(1);
                }
                Ok(_) => return,
            }
        })
    }

    /// Assumes in order write of versions.
    pub async fn write_block(&self, block_id: &str, data: Vec<u8>) {
        tokio::task::block_in_place(move || loop {
            let conn = match self.pool.get() {
                Ok(conn) => conn,
                Err(x) => {
                    println!("WriteBlock({block_id}, conn): {x:?}");
                    std::process::exit(1);
                }
            };
            let executed = conn.execute(
                "REPLACE INTO blocks(block_id, data) \
                    SELECT ?, ? FROM incarnation WHERE incarnation_num=?",
                rusqlite::params![block_id, data, self.incarnation_num],
            );
            match executed {
                Ok(executed) => {
                    if executed == 0 {
                        eprintln!("Incarnation change! Exiting...");
                        std::process::exit(1);
                    }
                    return;
                }
                Err(x) => {
                    println!("WriteBlock({block_id}): {x:?}");
                    std::process::exit(1);
                }
            }
        });
    }

    /// Move from source to destination.
    pub async fn move_blocks(&self, to: &Self, block_ids: Vec<String>) {
        // Split block_ids into chunks.
        let chunks: Vec<&[String]> = block_ids.chunks(64).collect();
        for chunk in chunks {
            // from: SELECT block_id, data FROM blocks WHERE block_id IN (chunk ids).
            // to: INSERT INTO blocks(block_id, data) VALUES (chunk ids, chunk data).
            // from: DELETE FROM blocks WHERE block_id IN (chunk ids).
            let in_params = rusqlite::params_from_iter(chunk.iter());
            let in_clause: Vec<String> = chunk.iter().map(|_| "?".to_string()).collect();
            let in_clause = in_clause.join(", ");
            let select_stmt =
                format!("SELECT block_id, data FROM blocks WHERE block_id IN ({in_clause})");
            let delete_stmt = format!("DELETE FROM blocks WHERE block_id IN ({in_clause})");
            let (insert_stmt, insert_params) = loop {
                let conn = match self.pool.get() {
                    Ok(conn) => conn,
                    Err(x) => {
                        println!("MoveBlocks: {x:?}");
                        continue;
                    }
                };
                let mut select_stmt = match conn.prepare(&select_stmt) {
                    Ok(select_stmt) => select_stmt,
                    Err(x) => {
                        println!("MoveBlocks: {x:?}");
                        continue;
                    }
                };
                let block_iter = select_stmt.query_map(in_params.clone(), |row| {
                    let block_id: String = row.get(0).unwrap();
                    let data: Vec<u8> = row.get(1).unwrap();
                    Ok((block_id, data))
                });
                let block_iter = match block_iter {
                    Ok(block_iter) => block_iter,
                    Err(x) => {
                        println!("MoveBlocks: {x:?}");
                        continue;
                    }
                };
                let mut insert_params = Vec::new();
                let mut values_clause = Vec::new();
                for row in block_iter {
                    let (block_id, data) = row.unwrap();
                    insert_params.push(rusqlite::types::Value::Text(block_id));
                    insert_params.push(rusqlite::types::Value::Blob(data));
                    values_clause.push("(?, ?)");
                }
                let values_clause = values_clause.join(", ");
                let insert_stmt =
                    format!("REPLACE INTO blocks(block_id, data) VALUES {values_clause}");
                break (insert_stmt, insert_params);
            };
            if insert_params.is_empty() {
                // Nothing to do.
                continue;
            }
            let insert_params = rusqlite::params_from_iter(insert_params.iter());
            // Write.
            loop {
                let mut conn = match to.pool.get() {
                    Ok(conn) => conn,
                    Err(x) => {
                        println!("MoveBlocks: {x:?}");
                        continue;
                    }
                };
                let txn = match conn.transaction() {
                    Ok(txn) => txn,
                    Err(x) => {
                        println!("MoveBlocks: {x:?}");
                        continue;
                    }
                };
                let incarn_num =
                    txn.query_row("SELECT incarnation_num FROM incarnation", [], |row| {
                        row.get(0)
                    });
                match incarn_num {
                    Err(x) => {
                        println!("MoveBlocks: {x:?}");
                        continue;
                    }
                    Ok(incarn_num) => {
                        let incarn_num: usize = incarn_num;
                        if incarn_num != to.incarnation_num {
                            let _ = txn.commit();
                            println!("Lost database ownership");
                            std::process::exit(1);
                        }
                    }
                }
                let resp = txn.execute(&insert_stmt, insert_params.clone());
                match resp {
                    Err(x) => {
                        println!("MoveBlocks: {x:?}");
                        continue;
                    }
                    Ok(_executed) => {}
                }
                match txn.commit() {
                    Err(x) => {
                        println!("MoveBlocks: {x:?}");
                        continue;
                    }
                    Ok(_) => break,
                }
            }
            // Delete.
            loop {
                let mut conn = match self.pool.get() {
                    Ok(conn) => conn,
                    Err(x) => {
                        println!("MoveBlocks: {x:?}");
                        continue;
                    }
                };
                let txn = match conn.transaction() {
                    Ok(txn) => txn,
                    Err(x) => {
                        println!("MoveBlocks: {x:?}");
                        continue;
                    }
                };
                let incarn_num =
                    txn.query_row("SELECT incarnation_num FROM incarnation", [], |row| {
                        row.get(0)
                    });
                match incarn_num {
                    Err(x) => {
                        println!("MoveBlocks: {x:?}");
                        continue;
                    }
                    Ok(incarn_num) => {
                        let incarn_num: usize = incarn_num;
                        if incarn_num != self.incarnation_num {
                            let _ = txn.commit();
                            println!("Lost database ownership");
                            std::process::exit(1);
                        }
                    }
                }
                let resp = txn.execute(&delete_stmt, in_params.clone());
                match resp {
                    Err(x) => {
                        println!("MoveBlocks: {x:?}");
                        continue;
                    }
                    Ok(_executed) => {}
                }
                match txn.commit() {
                    Err(x) => {
                        println!("MoveBlocks: {x:?}");
                        continue;
                    }
                    Ok(_) => break,
                }
            }
        }
        // Vacuum.
        self.vacuum().await;
    }

    pub async fn vacuum(&self) {
        let pool = self.pool.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let conn = loop {
                match pool.get() {
                    Ok(conn) => break conn,
                    Err(x) => {
                        println!("Vacuum: {x:?}");
                        continue;
                    }
                };
            };
            let _resp = conn.execute("VACUUM", []);
        })
        .await;
    }

    /// Release lock on file.
    pub async fn release(self) {
        tokio::task::spawn_blocking(move || {
            let conn = loop {
                match self.pool.get() {
                    Ok(conn) => break conn,
                    Err(x) => {
                        println!("MoveBlocks: {x:?}");
                        continue;
                    }
                };
            };
            match conn.pragma_update(None, "journal_mode", "delete") {
                Ok(_) => {}
                Err(x) => {
                    println!("Cannot release: {x:?}");
                    std::process::exit(1);
                }
            }
            match conn.pragma_update(None, "locking_mode", "normal") {
                Ok(_) => {}
                Err(x) => {
                    println!("Cannot release: {x:?}");
                    std::process::exit(1);
                }
            }
            // Actually release lock in sqlite.
            match conn.query_row("SELECT unique_row FROM incarnation", [], |r| r.get(0)) {
                Ok(res) => {
                    let _res: usize = res;
                }
                Err(x) => {
                    println!("Cannot release: {x:?}");
                    std::process::exit(1);
                }
            }
        });
    }

    /// Delete database.
    pub async fn delete(self) {
        let db_file = self.db_file.clone();
        println!("Deleting file: {db_file}");
        std::mem::drop(self);
        tokio::task::block_in_place(move || {
            let resp = std::fs::remove_file(db_file.clone());
            match resp {
                Ok(_) => return,
                Err(err) => match err.kind() {
                    std::io::ErrorKind::NotFound => return,
                    _ => {
                        println!("Error: {err:?}");
                        std::process::exit(1);
                    }
                },
            }
        })
    }

    /// Make a block from an ownership or a split key.
    /// TODO: I have yet to think about if these keys are truly unique.
    pub fn block_id_from_key(key: &str) -> String {
        general_purpose::URL_SAFE_NO_PAD.encode(key)
    }

    pub fn data_file_from_key(key: &str) -> String {
        let fs_prefix = obelisk::common::shared_storage_prefix();
        let fs_prefix = format!("{fs_prefix}/sless_btree/data");
        let _create_dir_resp = std::fs::create_dir_all(&fs_prefix);
        let ownership_key = Self::block_id_from_key(key);
        format!("{fs_prefix}/{ownership_key}.db")
    }

    /// Create database.
    pub fn new_sync(ownership_key: &str) -> Self {
        let db_file = Self::data_file_from_key(ownership_key);
        let mut first = true;
        loop {
            if !first {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            first = false;
            let manager = r2d2_sqlite::SqliteConnectionManager::file(&db_file);
            // Set to 1 to prevent concurrency issues.
            let pool = match r2d2::Pool::builder().max_size(1).build(manager) {
                Ok(pool) => pool,
                Err(x) => {
                    println!("NewLocalOwnership Pool {ownership_key:?}: {x:?}");
                    let file = std::fs::File::open(&db_file);
                    println!("File: {file:?}");
                    continue;
                }
            };
            let mut conn = match pool.get() {
                Ok(conn) => conn,
                Err(x) => {
                    println!("NewLocalOwnership Conn {ownership_key:?}: {x:?}");
                    let file = std::fs::File::open(&db_file);
                    println!("File: {file:?}");
                    continue;
                }
            };
            // match conn.pragma_update(None, "locking_mode", "exclusive") {
            //     Ok(_) => {}
            //     Err(x) => {
            //         println!("NewLocalOwnership Pragma1 {ownership_key:?}: {x:?}");
            //         let file = std::fs::File::open(&db_file);
            //         println!("File: {file:?}");
            //         continue;
            //     }
            // }
            // match conn.pragma_update(None, "journal_mode", "wal") {
            //     Ok(_) => {}
            //     Err(x) => {
            //         println!("NewLocalOwnership Pragma2 {ownership_key:?}: {x:?}");
            //         let file = std::fs::File::open(&db_file);
            //         println!("File: {file:?}");
            //         continue;
            //     }
            // }
            let txn = match conn.transaction() {
                Ok(txn) => txn,
                Err(x) => {
                    println!("NewLocalOwnership {ownership_key:?}: {x:?}");
                    continue;
                }
            };
            match txn.execute("CREATE TABLE IF NOT EXISTS incarnation (unique_row INTEGER PRIMARY KEY, incarnation_num INT)", ()) {
                Ok(_) => {},
                Err(x) => {
                    println!("NewLocalOwnership {ownership_key:?}: {x:?}");
                    continue;
                }
            }
            match txn.execute(
                "CREATE TABLE IF NOT EXISTS blocks (block_id TEXT PRIMARY KEY, data BLOB)",
                (),
            ) {
                Ok(_) => {}
                Err(x) => {
                    println!("NewLocalOwnership {ownership_key:?}: {x:?}");
                    continue;
                }
            }
            let incarnation_num =
                txn.query_row("SELECT incarnation_num FROM incarnation", [], |r| r.get(0));
            let incarnation_num = incarnation_num.unwrap_or(0_usize) + 1;
            match txn.execute(
                "REPLACE INTO incarnation (unique_row, incarnation_num) VALUES (0, ?)",
                [&incarnation_num],
            ) {
                Ok(_) => {}
                Err(x) => {
                    println!("NewLocalOwnership {ownership_key:?}: {x:?}");
                    continue;
                }
            };
            match txn.commit() {
                Ok(_) => {
                    println!("Successfully opened database {ownership_key} at {db_file}!");
                    return LocalOwnership {
                        pool,
                        incarnation_num,
                        db_file,
                    };
                }
                _ => {
                    continue;
                }
            }
        }
    }
}
