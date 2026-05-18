use std::fmt::Debug;

use redis::AsyncCommands;
use serde::de::Error as _;
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    Repeat, SchedulerId, SchedulerInfo, SchedulerTemplate, SchedulerWindow,
    error::{
        AddJobErr, AddJobSchedulerError, BasicRedisError, ObliterateError, PauseResumeError,
        RemoveJobSchedulerError,
    },
    job::{JobJoinHandle, JobOptions},
    luacommands::{
        ADD_DELAYED_JOB, ADD_PRIORITIZED_JOB, ADD_STANDARD_JOB, AddDelayedJob, AddJobScheduler,
        AddJobSchedulerOk, AddPrioritizedJob, AddStandardJob, GetJobScheduler, InvokeLuaScript,
        Obliterate, ObliterateOk, Pause, PauseAction, RemoveJobScheduler,
    },
    queue::Queue,
    scheduler::compute_next_millis,
    worker::{Worker, WorkerArgs},
};

fn map_add_job_value(value: redis::Value) -> Result<String, AddJobErr> {
    match value {
        redis::Value::Int(-5) => Err(AddJobErr::MissingParentKey),
        redis::Value::BulkString(s) => Ok(String::from_utf8_lossy(&s).into()),
        redis::Value::SimpleString(s) => Ok(s),
        x => Err(redis::RedisError::from((
            redis::ErrorKind::ResponseError,
            "Unexpected response from add-job lua script in pipeline",
            format!("Response was {x:?}"),
        ))
        .into()),
    }
}

fn is_noscript(v: &redis::Value) -> bool {
    matches!(v, redis::Value::ServerError(e) if e.code() == "NOSCRIPT")
}

async fn send_pipeline(
    con: &mut impl redis::aio::ConnectionLike,
    cmds: &[redis::Cmd],
) -> Result<Vec<redis::Value>, AddJobErr> {
    let mut pipe = redis::pipe();
    for cmd in cmds {
        pipe.add_command(cmd.clone());
    }
    // req_packed_commands returns raw per-command Values including ServerError,
    // so individual NOSCRIPT failures don't abort the whole batch.
    con.req_packed_commands(&pipe, 0, cmds.len())
        .await
        .map_err(Into::into)
}

/// Run an EVALSHA pipeline. If any commands return NOSCRIPT, loads the three
/// add-job scripts and retries only the failed positions so successful jobs
/// are not re-submitted.
async fn bulk_evalsha_pipeline(
    con: &mut impl redis::aio::ConnectionLike,
    cmds: &[redis::Cmd],
) -> Result<Vec<redis::Value>, AddJobErr> {
    let raw = send_pipeline(con, cmds).await?;

    let failed: Vec<usize> = raw
        .iter()
        .enumerate()
        .filter(|(_, v)| is_noscript(v))
        .map(|(i, _)| i)
        .collect();

    if failed.is_empty() {
        return Ok(raw);
    }

    ADD_STANDARD_JOB.prepare_invoke().load_async(con).await?;
    ADD_DELAYED_JOB.prepare_invoke().load_async(con).await?;
    ADD_PRIORITIZED_JOB.prepare_invoke().load_async(con).await?;

    let retry_cmds: Vec<redis::Cmd> = failed.iter().map(|&i| cmds[i].clone()).collect();
    let retry_raw = send_pipeline(con, &retry_cmds).await?;

    let mut merged = raw;
    for (&idx, val) in failed.iter().zip(retry_raw) {
        merged[idx] = val;
    }
    Ok(merged)
}

impl<D, R> Queue<D, R>
where
    R: Debug + Clone + Send + DeserializeOwned + 'static,
{
    /// Create a worker instance for processing jobs of this queue.
    pub fn worker(&self, worker_args: WorkerArgs) -> Worker<D, R>
    where
        R: Send + Clone + Debug + DeserializeOwned + 'static,
        D: Send + Sync + DeserializeOwned + Debug + 'static,
    {
        if self.pool.status().max_size < worker_args.parallel_connections * 2 {
            self.pool.resize(worker_args.parallel_connections * 2);
        }
        Worker::new(self.pool.clone(), self.name.clone(), worker_args)
    }

    /// Stop processing jobs of a queue.
    pub async fn pause(&self) -> Result<(), PauseResumeError> {
        let p = Pause {
            queue: &self.name,
            action: PauseAction::Pause,
        };
        let mut con = self.pool.get().await?;
        p.call(&mut con).await?;
        Ok(())
    }

    /// Continue processing jobs of a queue.
    pub async fn resume(&self) -> Result<(), PauseResumeError> {
        let p = Pause {
            queue: &self.name,
            action: PauseAction::Resume,
        };
        let mut con = self.pool.get().await?;
        p.call(&mut con).await?;
        Ok(())
    }

    /// Add a one-off job with the given options.
    ///
    /// Repeating jobs are configured via [`Self::upsert_job_scheduler`].
    pub async fn add_with(
        &self,
        job_name: &str,
        data: &D,
        job_options: &JobOptions,
    ) -> Result<JobJoinHandle<D, R>, AddJobErr>
    where
        D: Serialize,
    {
        let mut con = self.pool.get().await?;
        let job_id = if job_options.delay.is_some() {
            AddDelayedJob {
                queue: &self.name,
                job_name,
                data,
                job_options,
            }
            .call(&mut con)
            .await?
        } else if job_options.priority.is_some() {
            AddPrioritizedJob {
                queue: &self.name,
                job_name,
                data,
                job_options,
            }
            .call(&mut con)
            .await?
        } else {
            AddStandardJob {
                queue: &self.name,
                job_name,
                data,
                job_options,
            }
            .call(&mut con)
            .await?
        };

        Ok(JobJoinHandle::new(
            self.name.clone(),
            self.pool.clone(),
            job_id,
            self.event_system.clone(),
        ))
    }

    /// Add a one-off job with default options.
    pub async fn add(&self, job_name: &str, data: &D) -> Result<JobJoinHandle<D, R>, AddJobErr>
    where
        D: Serialize,
    {
        self.add_with(job_name, data, &JobOptions::default()).await
    }

    /// Add multiple jobs in a single Redis round-trip using EVALSHA pipelining.
    ///
    /// Each tuple is `(job_name, data, options)` — the same arguments as
    /// [`Self::add_with`]. All jobs are dispatched in one pipeline. If the
    /// scripts are not yet cached by Redis (e.g. after a restart), they are
    /// loaded automatically and only the failed commands are retried.
    ///
    /// Returns one [`JobJoinHandle`] per input job in the same order.
    pub async fn add_bulk<'a>(
        &self,
        jobs: &[(&'a str, &'a D, &'a JobOptions)],
    ) -> Result<Vec<JobJoinHandle<D, R>>, AddJobErr>
    where
        D: Serialize,
    {
        if jobs.is_empty() {
            return Ok(Vec::new());
        }

        // Serialize all job payloads and build EVALSHA commands up front.
        let cmds: Vec<redis::Cmd> = jobs
            .iter()
            .map(|&(job_name, data, job_options)| {
                if job_options.delay.is_some() {
                    AddDelayedJob {
                        queue: &self.name,
                        job_name,
                        data,
                        job_options,
                    }
                    .evalsha_cmd()
                } else if job_options.priority.is_some() {
                    AddPrioritizedJob {
                        queue: &self.name,
                        job_name,
                        data,
                        job_options,
                    }
                    .evalsha_cmd()
                } else {
                    AddStandardJob {
                        queue: &self.name,
                        job_name,
                        data,
                        job_options,
                    }
                    .evalsha_cmd()
                }
            })
            .collect::<Result<_, _>>()?;

        let mut con = self.pool.get().await?;
        let values = bulk_evalsha_pipeline(&mut *con, &cmds).await?;

        values
            .into_iter()
            .map(|v| {
                map_add_job_value(v).map(|job_id| {
                    JobJoinHandle::new(
                        self.name.clone(),
                        self.pool.clone(),
                        job_id,
                        self.event_system.clone(),
                    )
                })
            })
            .collect()
    }

    /// Create or update a job scheduler.
    ///
    /// `scheduler_id` is a unique identifier for this scheduler.
    /// `repeat` is the repetition rule (`Every` interval or `Cron` pattern).
    /// `window` constrains start / end / limit for the scheduler.
    /// `template` describes the name / data / options of every produced job.
    pub async fn upsert_job_scheduler(
        &self,
        scheduler_id: &SchedulerId,
        repeat: &Repeat,
        window: &SchedulerWindow,
        template: SchedulerTemplate<'_, D>,
    ) -> Result<AddJobSchedulerOk, AddJobSchedulerError>
    where
        D: Serialize,
    {
        let now = chrono::Utc::now();
        let next_millis = compute_next_millis(repeat, now)
            .map_err(|e| AddJobSchedulerError::SerializationFailed(serde_json::Error::custom(e)))?;

        let mut con = self.pool.get().await?;
        let cmd = AddJobScheduler {
            queue: &self.name,
            scheduler_id,
            next_millis,
            repeat,
            window,
            delayed_opts: template.opts,
            template,
            producer_key: None,
        };
        cmd.call(&mut con).await
    }

    /// Remove a scheduler and its pending next job.
    pub async fn remove_job_scheduler(
        &self,
        scheduler_id: &SchedulerId,
    ) -> Result<(), RemoveJobSchedulerError> {
        let mut con = self.pool.get().await?;
        let cmd = RemoveJobScheduler {
            queue: &self.name,
            scheduler_id,
        };
        cmd.call(&mut con).await
    }

    /// List all job schedulers for this queue.
    pub async fn get_job_schedulers(&self) -> Result<Vec<SchedulerInfo>, BasicRedisError> {
        let mut con = self.pool.get().await?;
        let ids: Vec<String> = con.zrange(self.name.repeat(), 0, -1).await?;
        let mut result = Vec::with_capacity(ids.len());
        for raw_id in ids {
            let Ok(id) = SchedulerId::try_new(raw_id) else {
                continue;
            };
            let cmd = GetJobScheduler {
                queue: &self.name,
                scheduler_id: &id,
            };
            if let Some(info) = cmd.call(&mut con).await.ok().flatten() {
                result.push(info);
            }
        }
        Ok(result)
    }

    /// Wipe this queue from existence.
    pub async fn obliterate(self) -> Result<(), ObliterateError> {
        self.pause().await?;
        let mut con = self.pool.get().await?;
        loop {
            let ob = Obliterate {
                queue: &self.name,
                batch_size: 1000,
                force: true,
            };
            let res = ob.call(&mut con).await?;
            match res {
                ObliterateOk::Progress => (),
                ObliterateOk::Obliterated => break,
            }
        }
        Ok(())
    }
}
