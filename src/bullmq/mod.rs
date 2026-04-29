//! Wire-format types matching BullMQ's on-the-wire JSON / MessagePack shapes.
//!
//! These types exist purely to serialize/deserialize at the Redis boundary.
//! They mirror BullMQ's JavaScript field names (including cryptic abbreviations
//! like `kl`, `cpof`, `de`, `fpof`, `idof`, `rdof`, `rjk`, `ic`).
//!
//! User-facing code never sees these types — domain types live elsewhere and
//! are converted to/from the wire shape only at lua-script invocation sites.

pub(crate) mod move_to_active;
pub(crate) mod move_to_finished;
pub(crate) mod options;
pub(crate) mod rate_limiter;
pub(crate) mod scheduler;
