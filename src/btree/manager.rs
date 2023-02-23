use super::{BTreeReqMeta, RescalingOp};
use crate::global_ownership::GlobalOwnership;
// use obelisk::common::leasing::Leaser;
// use obelisk::common::scaling_table_name;
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedMutexGuard};

/// BTree Manager.
pub struct BTreeManager {
    global_ownership: Arc<GlobalOwnership>,
    // leaser: Leaser,
    managing_lock: Arc<Mutex<()>>,
}

impl BTreeManager {
    /// Create a new manager.
    pub async fn new(global_ownership: Arc<GlobalOwnership>) -> Self {
        // let leaser = Leaser::new(&scaling_table_name("messaging")).await;
        let managing_lock = Arc::new(Mutex::new(()));
        BTreeManager {
            global_ownership,
            // leaser,
            managing_lock,
        }
    }

    /// Perform rescaling.
    pub async fn manage_rescaling(&self, rescaling_op: RescalingOp, recovering: bool) {
        self.global_ownership
            .start_rescaling(rescaling_op.clone(), recovering)
            .await;
        let (from, to) = match &rescaling_op {
            RescalingOp::ScaleIn { from, to, uid: _ } => (from, to),
            RescalingOp::ScaleOut { from, to, uid: _ } => (from, to),
        };
        let from_mc = self.global_ownership.get_or_make_client(from).await;
        let to_mc = self.global_ownership.get_or_make_client(to).await;
        let req = BTreeReqMeta::Rescale {
            op: rescaling_op.clone(),
        };
        let req = serde_json::to_string(&req).unwrap();
        loop {
            println!("Sending rescale to: {from:?}");
            let resp = from_mc.send_message(&req, &[]).await;
            if resp.is_some() {
                break;
            }
        }
        loop {
            println!("Sending rescale to: {to:?}");
            let resp = to_mc.send_message(&req, &[]).await;
            if resp.is_some() {
                break;
            }
        }
        self.global_ownership.finish_rescaling().await;
    }

    /// Prevent concurrent rescaling.
    pub async fn lock_manager(&self) -> Option<OwnedMutexGuard<()>> {
        // // Renew lease.
        // println!("Trying to renew lease!");
        // let acquired = self.leaser.renew("sbtree_management_leasing", false).await;
        // if !acquired {
        //     println!("Could not acquire lease!");
        //     return None;
        // }
        // println!("Acquired lease!");
        // Only allow one management task at a time.
        let lock = self.managing_lock.clone().try_lock_owned().ok();
        if lock.is_none() {
            println!("Could not try_lock!");
            return None;
        }
        println!("Successful try_lock!");
        // // Keep lease fresh. This is not perfect to remove duplicate rescaling.
        // // But the only correctness issue, is avoiding overwriting rescaling, which we do.
        // let leaser = self.leaser.clone();
        // tokio::spawn(async move {
        //     for _ in 0..10 {
        //         tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        //         leaser.renew("sbtree_management_leasing", false).await;
        //     }
        // });
        lock
    }

    /// Perform management tasks.
    pub async fn manage(&self) {
        // Check external access and leasing.
        if !obelisk::common::has_external_access() {
            return;
        }
        if !self.global_ownership.can_do_regular_rescaling().await {
            return;
        }
        // Prevent concurrent ops.
        let locked = self.lock_manager().await;
        if locked.is_none() {
            return;
        }
        // If there is an ongoing rescaling, perform it.
        let rescaling_op = self.global_ownership.get_ongoing_rescaling();
        match rescaling_op {
            None => {}
            Some(rescaling_op) => {
                self.manage_rescaling(rescaling_op, true).await;
            }
        }
        // Read loads.
        let loads = self.global_ownership.read_loads().await;
        let (lo, next_lo, hi) = {
            let mut loads: Vec<(String, f64)> = loads
                .iter()
                .map(|(w_id, load)| (w_id.clone(), *load))
                .collect();
            loads.sort_by(|(_, load1), (_, load2)| load1.total_cmp(load2));
            let lo = loads.get(0);
            let next_lo = loads.get(1);
            let hi = loads.last();
            (lo.cloned(), next_lo.cloned(), hi.cloned())
        };

        // Attempt scale out.
        match hi {
            None => {}
            Some((hi_id, hi_load)) => {
                if hi_load >= 1.1 {
                    let free_worker = self.global_ownership.get_free_worker(true).await;
                    let uid = uuid::Uuid::new_v4().to_string();
                    let rescaling_op = RescalingOp::ScaleOut {
                        from: hi_id,
                        to: free_worker,
                        uid,
                    };
                    let valid = self
                        .global_ownership
                        .start_rescaling(rescaling_op.clone(), false)
                        .await;
                    if valid {
                        self.manage_rescaling(rescaling_op, false).await;
                    }
                }
            }
        }

        // Attempt scale in.
        match lo {
            None => {}
            Some((lo_id, lo_load)) => {
                match next_lo {
                    None => {}
                    Some((next_id, next_load)) => {
                        let (lo_id, next_id) = if lo_id == "manager" {
                            // Never scale in manager.
                            (next_id, lo_id)
                        } else {
                            (lo_id, next_id)
                        };
                        if lo_load + next_load <= 0.9 {
                            let uid = uuid::Uuid::new_v4().to_string();
                            let rescaling_op = RescalingOp::ScaleIn {
                                from: lo_id,
                                to: next_id,
                                uid,
                            };
                            let valid = self
                                .global_ownership
                                .start_rescaling(rescaling_op.clone(), false)
                                .await;
                            if valid {
                                self.manage_rescaling(rescaling_op, false).await;
                            }
                        }
                    }
                }
            }
        }
    }
}
