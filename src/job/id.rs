use nutype::nutype;

/// A struct which wraps a String and holds guarantees to be a valid
/// (Redis compatible) ID for Jobs.
#[nutype(
    validate(not_empty, predicate = |s: &str| !s.contains(":")),
    sanitize(trim),
    derive(AsRef, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)
)]
pub struct JobId(String);
