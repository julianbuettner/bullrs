use redis::Value;

use crate::{
    error::IsFinishedError,
    luacommands::{IS_FINISHED, InvokeLuaScript},
    queue::QueueName,
};

pub struct IsFinished<'a> {
    pub queue: &'a QueueName,
    pub job_id: &'a str,
}

pub enum IsFinishedOk {
    /// Job is not yet finished
    NotFinished,
    /// Job completed successfully with a return value
    Completed(String),
    /// Job failed with a reason
    Failed(String),
    /// Job key is missing
    Missing,
}

impl<'a> InvokeLuaScript for IsFinished<'a> {
    type RedisOutput = Value;
    type DomainOk = IsFinishedOk;
    type DomainErr = IsFinishedError;

    fn generate_invocation(&self) -> Result<redis::ScriptInvocation<'static>, Self::DomainErr> {
        let mut invoc = IS_FINISHED.prepare_invoke();
        invoc
            .key(self.queue.completed())
            .key(self.queue.failed())
            .key(self.queue.job(self.job_id))
            .arg(self.job_id)
            .arg("1");
        Ok(invoc)
    }

    fn map_value(&self, value: Self::RedisOutput) -> Result<Self::DomainOk, Self::DomainErr> {
        match &value {
            Value::Array(arr) if !arr.is_empty() => {
                let status = match &arr[0] {
                    Value::Int(n) => *n,
                    _ => panic!("isFinished script returned unexpected status type"),
                };
                let val = arr
                    .get(1)
                    .and_then(|v| match v {
                        Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
                        Value::SimpleString(s) => Some(s.clone()),
                        _ => None,
                    })
                    .unwrap_or_default();

                match status {
                    0 => Ok(IsFinishedOk::NotFinished),
                    1 => Ok(IsFinishedOk::Completed(val)),
                    2 => Ok(IsFinishedOk::Failed(val)),
                    -1 => Ok(IsFinishedOk::Missing),
                    _ => panic!("isFinished script returned unexpected status: {status}"),
                }
            }
            Value::Int(0) => Ok(IsFinishedOk::NotFinished),
            Value::Int(1) => Ok(IsFinishedOk::Completed(String::new())),
            Value::Int(2) => Ok(IsFinishedOk::Failed(String::new())),
            Value::Int(-1) => Ok(IsFinishedOk::Missing),
            _ => panic!("isFinished script returned unexpected value: {value:?}"),
        }
    }
}
