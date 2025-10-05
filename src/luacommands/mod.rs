use std::error::Error;

use lazy_static::lazy_static;
use redis::{
    aio::ConnectionLike, FromRedisValue, RedisError, RedisResult, Script, ScriptInvocation,
};

mod add_delayed_job;
mod add_log;
mod add_prioritized_job;
mod add_standard_job;
mod move_stalled_jobs_to_wait;
mod move_to_active;
mod move_to_finished;
mod obliterate;
mod pause;
mod update_progress;

pub use add_delayed_job::AddDelayedJob;
pub use add_log::AddLog;
pub use add_prioritized_job::{AddPrioritizedJob, AddPrioritizedJobOk};
pub use add_standard_job::AddStandardJob;
pub use move_stalled_jobs_to_wait::MoveStalledJobsToWait;
pub use move_to_active::{MoveToActive, MoveToActiveOk, RateLimiter};
pub use move_to_finished::{KeepJobsConfig, MoveToFinished, MoveToFinishedOptions};
pub use obliterate::{Obliterate, ObliterateOk, ObliterateErr};
pub use pause::{Pause, PauseAction};
pub use update_progress::UpdateProgess;

macro_rules! load_script {
    ($filename:expr) => {
        Script::new(include_str!(concat!(env!("OUT_DIR"), "/lua/", $filename)))
    };
}

lazy_static! {
    static ref ADD_DELAYED_JOB: Script = load_script!("addDelayedJob-6.lua");
    static ref ADD_LOG: Script = load_script!("addLog-2.lua");
    static ref ADD_PRIORITIZED_JOB: Script = load_script!("addPrioritizedJob-9.lua");
    static ref ADD_STANDARD_JOB: Script = load_script!("addStandardJob-9.lua");
    static ref MOVE_STALLED_JOBS_TO_WAIT: Script = load_script!("moveStalledJobsToWait-8.lua");
    static ref MOVE_TO_ACTIVE: Script = load_script!("moveToActive-11.lua");
    static ref MOVE_TO_FINISHED: Script = load_script!("moveToFinished-14.lua");
    static ref OBLITERATE: Script = load_script!("obliterate-2.lua");
    static ref PAUSE: Script = load_script!("pause-7.lua");
    static ref UPDATE_DATA: Script = load_script!("updateData-1.lua");
    static ref UPDATE_PROGRESS: Script = load_script!("updateProgress-3.lua");
}

pub trait InvokeLuaScript {
    type RedisOutput: FromRedisValue;
    type DomainOk;
    type DomainErr: Error + From<RedisError>;

    fn generate_invocation(&self) -> Result<ScriptInvocation<'static>, Self::DomainErr>;

    fn map_redis_error(&self, error: RedisError) -> Self::DomainErr {
        error.into()
    }

    fn map_value(&self, value: Self::RedisOutput) -> Result<Self::DomainOk, Self::DomainErr>;

    async fn call(&self, con: &mut impl ConnectionLike) -> Result<Self::DomainOk, Self::DomainErr> {
        let invocation = self.generate_invocation()?;
        let redis_res: Self::RedisOutput = invocation
            .invoke_async(con)
            .await
            .map_err(|e| self.map_redis_error(e))?;
        self.map_value(redis_res)
    }
}
