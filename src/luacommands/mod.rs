use lazy_static::lazy_static;
use redis::{RedisResult, Script, aio::ConnectionLike};

mod add_delayed_job;
mod add_standard_job;
mod move_stalled_jobs_to_wait;
mod move_to_active;
mod move_to_finished;
mod add_log;

pub use add_delayed_job::AddDelayedJob;
pub use add_log::AddLog;
pub use add_standard_job::AddStandardJob;
pub use move_stalled_jobs_to_wait::MoveStalledJobsToWait;
pub use move_to_active::{MoveToActive, MoveToActiveResult, MoveToActiveReturn, RateLimiter};
pub use move_to_finished::{KeepJobsConfig, MoveToFinished, MoveToFinishedOptions};

macro_rules! load_script {
    ($filename:expr) => {
        Script::new(include_str!(concat!(env!("OUT_DIR"), "/lua/", $filename)))
    };
}

lazy_static! {
    static ref ADD_DELAYED_JOB: Script = load_script!("addDelayedJob-6.lua");
    static ref ADD_LOG: Script = load_script!("addLog-2.lua");
    static ref ADD_STANDARD_JOB: Script = load_script!("addStandardJob-9.lua");
    static ref MOVE_STALLED_JOBS_TO_WAIT: Script = load_script!("moveStalledJobsToWait-8.lua");
    static ref MOVE_TO_ACTIVE: Script = load_script!("moveToActive-11.lua");
    static ref MOVE_TO_FINISHED: Script = load_script!("moveToFinished-14.lua");
    static ref UPDATE_DATA: Script = load_script!("updateData-1.lua");
}

pub trait InvokeLuaScript {
    type Return;

    async fn call(self: Self, con: &mut impl ConnectionLike) -> RedisResult<Self::Return>;
}
