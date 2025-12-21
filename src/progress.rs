use nutype::nutype;

/// A progress type, which is forced to be between 0.0 and 100.0 at runtime.
/// Construct it with
/// ```
/// use bullrs::ProgressPercent;
/// ProgressPercent::try_new(12.3456).unwrap();
/// ```
#[nutype(
    validate(finite),
    sanitize(with = |raw: f32| raw.clamp(0.0, 100.0)),
    derive(PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Clone, Copy),
)]
pub struct ProgressPercent(f32);
