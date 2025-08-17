use nutype::nutype;

#[nutype(
    validate(finite),
    sanitize(with = |raw: f32| raw.clamp(0.0, 100.0)),
    derive(PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize),
)]
pub struct Progress(f32);
