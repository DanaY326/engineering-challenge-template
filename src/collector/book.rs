//! An in-memory L2 orderbook for a single symbol.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

use crate::Side;
use crate::util::dtos::challenge_orderbook::{
    SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};

/// How an exchange describes a change to a price level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LevelChange {
    /// Add this (possibly negative) amount to the level (Kalshi).
    Delta(i64),
    /// The level's new absolute size; zero removes it (e.g. replaying our
    /// own output, whose updates carry absolute quantities).
    Absolute(i64),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Book {
    pub bids: BTreeMap<i64, i64>,
    pub asks: BTreeMap<i64, i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NegativeLevel {
    pub side: Side,
    pub price: i64,
    pub quantity: i64,
}

impl std::fmt::Display for NegativeLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} level {} would have negative quantity {}",
            self.side, self.price, self.quantity
        )
    }
}

impl std::error::Error for NegativeLevel {}

impl Book {
    /// Build a book from `(price, quantity)` levels. Non-positive quantities
    /// are dropped and duplicate prices are summed.
    pub fn from_levels(bids: &[(i64, i64)], asks: &[(i64, i64)]) -> Self {
        let mut book = Self::default();
        for &(price, qty) in bids {
            *book.bids.entry(price).or_default() += qty;
        }
        for &(price, qty) in asks {
            *book.asks.entry(price).or_default() += qty;
        }
        book.bids.retain(|_, q| *q > 0);
        book.asks.retain(|_, q| *q > 0);
        book
    }

    fn side_mut(&mut self, side: Side) -> &mut BTreeMap<i64, i64> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    /// Apply a level change and return the level's new absolute quantity.
    ///
    /// A delta that would take a level negative means our book has diverged
    /// from the exchange's. The book is left untouched and the caller should
    /// resync.
    pub fn apply(
        &mut self,
        side: Side,
        price: i64,
        change: LevelChange,
    ) -> Result<i64, NegativeLevel> {
        let levels = self.side_mut(side);
        let current = levels.get(&price).copied().unwrap_or(0);
        let quantity = match change {
            LevelChange::Delta(delta) => current + delta,
            LevelChange::Absolute(quantity) => quantity,
        };
        if quantity < 0 {
            return Err(NegativeLevel {
                side,
                price,
                quantity,
            });
        }
        if quantity == 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, quantity);
        }
        Ok(quantity)
    }

    pub fn best_bid(&self) -> Option<i64> {
        self.bids.keys().next_back().copied()
    }

    pub fn best_ask(&self) -> Option<i64> {
        self.asks.keys().next().copied()
    }

    /// Bids are written best (highest) first, asks best (lowest) first.
    pub fn to_snapshot(
        &self,
        symbol: &str,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    ) -> SnapshotDomain {
        SnapshotDomain {
            received_at,
            exchange_timestamp,
            bids: self
                .bids
                .iter()
                .rev()
                .map(|(&price_level, &quantity)| SnapshotBidsEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(&price_level, &quantity)| SnapshotAsksEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            symbol: symbol.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_delta_and_absolute() {
        let mut book = Book::from_levels(&[(400_000, 10)], &[(600_000, 5)]);
        assert_eq!(book.apply(Side::Buy, 400_000, LevelChange::Delta(-4)), Ok(6));
        assert_eq!(book.apply(Side::Buy, 400_000, LevelChange::Delta(-6)), Ok(0));
        assert!(book.bids.is_empty());
        assert_eq!(
            book.apply(Side::Sell, 600_000, LevelChange::Absolute(9)),
            Ok(9)
        );
        assert!(
            book.apply(Side::Sell, 700_000, LevelChange::Delta(-1))
                .is_err()
        );
        assert!(!book.asks.contains_key(&700_000));
    }

    #[test]
    fn snapshot_orders_best_first() {
        let book = Book::from_levels(&[(1, 1), (3, 1)], &[(7, 1), (5, 1)]);
        let ts = DateTime::from_timestamp_micros(0).unwrap();
        let snap = book.to_snapshot("X", ts, ts);
        assert_eq!(snap.bids[0].price_level, 3);
        assert_eq!(snap.asks[0].price_level, 5);
    }
}
