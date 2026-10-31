//! The output writer: a dedicated thread draining a channel into an
//! [`AlignedSbeWriter`], so file I/O never blocks the network tasks.

use std::path::PathBuf;
use std::thread::JoinHandle;

use tokio::sync::mpsc::UnboundedReceiver;
use tokio_util::sync::CancellationToken;

use crate::util::AlignedSbeWriter;
use crate::util::dtos::challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain};

/// Must match the [`AlignedSbeReader`](crate::util::AlignedSbeReader)'s
/// fixed chunk size, or the output cannot be read back.
pub const CHUNK_SIZE: usize = 64 << 20;

pub enum OutMsg {
    Snapshot(SnapshotDomain),
    Update(PriceLevelUpdateDomain),
}

#[derive(Debug, Default)]
pub struct WriterStats {
    pub snapshots: u64,
    pub updates: u64,
}

/// Write every message until all senders are dropped, then flush and close
/// the file. If writing fails (e.g. the disk is full), `on_error` is
/// cancelled so the collector shuts down instead of collecting into the void.
pub fn spawn_writer(
    path: PathBuf,
    rx: UnboundedReceiver<OutMsg>,
    on_error: CancellationToken,
) -> anyhow::Result<JoinHandle<anyhow::Result<WriterStats>>> {
    let writer = AlignedSbeWriter::create(&path, CHUNK_SIZE)?;
    Ok(std::thread::Builder::new()
        .name("sbe-writer".to_owned())
        .spawn(move || {
            let result = write_all(writer, rx);
            if let Err(e) = &result {
                tracing::error!("output failed, shutting down: {e:#}");
                on_error.cancel();
            }
            result
        })?)
}

fn write_all(
    mut writer: AlignedSbeWriter,
    mut rx: UnboundedReceiver<OutMsg>,
) -> anyhow::Result<WriterStats> {
    let mut stats = WriterStats::default();
    while let Some(msg) = rx.blocking_recv() {
        match msg {
            OutMsg::Snapshot(snapshot) => {
                writer.write_msg(&snapshot)?;
                stats.snapshots += 1;
            }
            OutMsg::Update(update) => {
                writer.write_msg(&update)?;
                stats.updates += 1;
            }
        }
    }
    writer.flush()?;
    Ok(stats)
}
