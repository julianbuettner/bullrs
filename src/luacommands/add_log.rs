use crate::error::AddLogError;

use crate::{
    luacommands::{ADD_LOG, InvokeLuaScript},
    queue::QueueName,
};

pub struct AddLog<'a> {
    pub queue: &'a QueueName,
    pub job_id: &'a str,
    pub log_line: &'a str,
    pub keep_logs: Option<usize>,
}

pub struct AddLogOk {
    pub new_count: usize,
}

impl<'a> InvokeLuaScript for AddLog<'a> {
    type RedisOutput = i64;
    type DomainOk = AddLogOk;
    type DomainErr = AddLogError;

    fn generate_invocation(&self) -> Result<redis::ScriptInvocation<'static>, Self::DomainErr> {
        let keep_logs = self.keep_logs.map(|v| v.to_string()).unwrap_or_default();
        let mut invocation = ADD_LOG.prepare_invoke();
        invocation
            .key(self.queue.job(self.job_id))
            .key(self.queue.job_logs(self.job_id))
            .arg(self.job_id)
            .arg(self.log_line)
            .arg(keep_logs);
        Ok(invocation)
    }

    fn map_value(&self, value: Self::RedisOutput) -> Result<Self::DomainOk, Self::DomainErr> {
        match value {
            0..=i64::MAX => Ok(AddLogOk {
                new_count: value as usize,
            }),
            -1 => Err(AddLogError::JobNotFound {
                job_id: self.job_id.into(),
                queue_name: self.queue.clone(),
            }),
            i64::MIN..-1 => {
                panic!(
                    "as we have control over the lua script, we should \
                    know this never happends"
                )
            }
        }
    }
}
