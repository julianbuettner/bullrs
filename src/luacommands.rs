use lazy_static::lazy_static;
use redis::{RedisResult, Script, aio::ConnectionLike};

mod move_stalles_jobs_to_wait;

pub use move_stalles_jobs_to_wait::MoveStalledJobsToWait;

macro_rules! load_script {
    ($filename:expr) => {
        Script::new(include_str!(concat!(env!("OUT_DIR"), "/lua/", $filename)))
    };
}

lazy_static! {
    pub static ref ADD_LOG: Script = load_script!("addLog-2.lua");
    pub static ref ADD_STANDARD_JOB: Script = load_script!("addStandardJob-8.lua");
    pub static ref UPDATE_DATA: Script = load_script!("updateData-1.lua");
    static ref MOVE_STALLED_JOBS_TO_WAIT: Script = load_script!("moveStalledJobsToWait-9.lua");
}

pub trait InvokeLuaScript {
    type Return;

    async fn call(self: Self, con: &mut impl ConnectionLike) -> RedisResult<Self::Return>;
}
