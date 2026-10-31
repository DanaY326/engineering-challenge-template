//! Read a collector output file back with [`AlignedSbeReader`] and check it:
//! every update must follow a snapshot for its symbol, and timestamps per
//! symbol must not go backwards.
//!
//! Usage: `inspect <file> [--print N]`

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use engineering_challenge::collector::book::{Book, LevelChange};
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let path = args.next().ok_or_else(|| anyhow::anyhow!("usage: inspect <file> [--print N]"))?;
    let print: usize = match (args.next().as_deref(), args.next()) {
        (Some("--print"), Some(n)) => n.parse()?,
        _ => 0,
    };

    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&path)?;
    let mut books: HashMap<String, Book> = HashMap::new();
    let mut last_ts: HashMap<String, DateTime<Utc>> = HashMap::new();
    let (mut snapshots, mut updates, mut orphans, mut backwards, mut crossed) = (0u64, 0u64, 0u64, 0u64, 0u64);
    let mut first: Option<DateTime<Utc>> = None;
    let mut last: Option<DateTime<Utc>> = None;
    let mut n = 0usize;

    while let Some(msg) = reader.read_msg()? {
        let (symbol, ts) = match msg {
            AnyMessage::Snapshot(decoded) => {
                let s = SnapshotDomain::try_from_decoder(decoded)?;
                if n < print {
                    println!(
                        "SNAPSHOT {} recv={} exch={} bids={} asks={} best={:?}/{:?}",
                        s.symbol, s.received_at, s.exchange_timestamp, s.bids.len(), s.asks.len(),
                        s.bids.first().map(|l| l.price_level), s.asks.first().map(|l| l.price_level),
                    );
                }
                let bids: Vec<(i64, i64)> = s.bids.iter().map(|l| (l.price_level, l.quantity)).collect();
                let asks: Vec<(i64, i64)> = s.asks.iter().map(|l| (l.price_level, l.quantity)).collect();
                let book = Book::from_levels(&bids, &asks);
                if let (Some(b), Some(a)) = (book.best_bid(), book.best_ask()) && b >= a {
                    crossed += 1;
                }
                books.insert(s.symbol.clone(), book);
                snapshots += 1;
                (s.symbol, s.received_at)
            }
            AnyMessage::PriceLevelUpdate(decoded) => {
                let u = PriceLevelUpdateDomain::try_from_decoder(decoded)?;
                if n < print {
                    println!(
                        "UPDATE   {} recv={} exch={} {} {} -> {}",
                        u.symbol, u.backtest_timestamp, u.exchange_timestamp, u.side, u.price_level, u.quantity
                    );
                }
                match books.get_mut(&u.symbol) {
                    Some(book) => {
                        book.apply(u.side, u.price_level, LevelChange::Absolute(u.quantity))?;
                    }
                    None => orphans += 1,
                }
                updates += 1;
                (u.symbol, u.backtest_timestamp)
            }
            _ => anyhow::bail!("unknown message"),
        };
        if let Some(prev) = last_ts.insert(symbol, ts) && ts < prev {
            backwards += 1;
        }
        first = first.min(Some(ts)).or(Some(ts));
        last = last.max(Some(ts));
        n += 1;
    }

    println!("messages: {n} ({snapshots} snapshots, {updates} updates) across {} symbols", books.len());
    println!("time range: {first:?} .. {last:?}");
    println!("updates before any snapshot for their symbol: {orphans}");
    println!("per-symbol timestamps going backwards: {backwards}");
    println!("crossed snapshots: {crossed}");
    Ok(())
}
