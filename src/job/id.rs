use nutype::nutype;

#[nutype(
    validate(predicate = |s: &str| !s.contains(":")),
    sanitize(trim),
    derive(PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)
)]
pub struct JobId(String);
