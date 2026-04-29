use std::fmt::Debug;

use redis::AsyncCommands;
use serde::{Serialize, de::DeserializeOwned};
use serde::de::Error as _;

use crate::{
    JobSchedulerInfo, JobSchedulerTemplate,
    error::{AddJobErr, AddJobSchedulerError, BasicRedisError, ObliterateError, PauseResumeError, RemoveJobSchedulerError},
    job::{JobJoinHandle, JobOptions},
    luacommands::{
        AddDelayedJob, AddJobScheduler, AddJobSchedulerOk, AddPrioritizedJob, AddStandardJob,
        GetJobScheduler, GetJobSchedulerOk, InvokeLuaScript, Obliterate, ObliterateOk, Pause,
        PauseAction, RemoveJobScheduler,
    },
    queue::Queue,
    scheduler::{RepeatOptions, compute_next_millis},
    JobSchedulerOpts,
    worker::{Worker, WorkerArgs},
};

impl<D, R> Queue<D, R>
where
    R: Debug + Clone + Send + DeserializeOwned + 'static,
{
    /// Create a worker instance for processing jobs of this queue.
    /// This will immediately start preloading jobs to be processed.
    ///
    /// Note that for picking up jobs, D must implement [serde::DeserializeOwned]
    /// and R must implement [serde::Serialize].
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

    /// Stop processing jobs of a queue. Jobs already picked up will be
    /// continued until completed or failed and delayed. Keep in mind,
    /// that workers might pre-pull a few jobs depending on concurrency
    /// settings. Those will still be processed as well.
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

    /// Add a job with given options.
    /// Go to [JobOptions] for documentation on the available options and default values.
    pub async fn add_with(
        &self,
        job_name: &str,
        data: &D,
        job_options: &JobOptions,
    ) -> Result<JobJoinHandle<D, R>, AddJobErr>
    where
        D: Serialize,
    {
        // If repeat options are present, create / update a job scheduler instead of a one-off job.
        if let Some(ref repeat) = job_options.repeat {
            let template = JobSchedulerTemplate {
                name: job_name,
                data,
                opts: job_options,
            };
            let ok = self
                .upsert_job_scheduler(
                    &format!("{}:{}", self.name.as_str(), job_name),
                    repeat,
                    template,
                )
                .await
                .map_err(|e| match e {
                    AddJobSchedulerError::JobIdCollision => AddJobErr::SchedulerJobIdCollision,
                    AddJobSchedulerError::JobSlotsBusy => AddJobErr::SchedulerJobSlotsBusy,
                    AddJobSchedulerError::SerializationFailed(err) => AddJobErr::SerializationFailed(err),
                    AddJobSchedulerError::RedisError(err) => AddJobErr::RedisError(err),
                    AddJobSchedulerError::PoolError(err) => AddJobErr::PoolError(err),
                })?;
            return Ok(JobJoinHandle::new(
                self.name.clone(),
                self.pool.clone(),
                ok.job_id,
                self.event_system.clone(),
            ));
        }

        let mut con = self.pool.get().await?;
        let job_id = if job_options.delay.is_some() {
            let c = AddDelayedJob {
                queue: &self.name,
                job_name,
                data,
                job_options,
            };
            c.call(&mut con).await?
        } else if job_options.priority.is_some() {
            let c = AddPrioritizedJob {
                queue: &self.name,
                job_name,
                data,
                job_options,
            };
            c.call(&mut con).await?
        } else {
            let c = AddStandardJob {
                queue: &self.name,
                job_name,
                data,
                job_options,
            };
            c.call(&mut con).await?
        };

        Ok(JobJoinHandle::new(
            self.name.clone(),
            self.pool.clone(),
            job_id,
            self.event_system.clone(),
        ))
    }

    /// Add a job with default options. See documentation of [JobOptions] for default values.
    pub async fn add(&self, job_name: &str, data: &D) -> Result<JobJoinHandle<D, R>, AddJobErr>
    where
        D: Serialize,
    {
        let job_options: JobOptions = Default::default();
        self.add_with(job_name, data, &job_options).await
    }

    /// Create or update a job scheduler.
    ///
    /// `scheduler_id` is a unique identifier for this scheduler (e.g. `"daily-report"`).
    /// `repeat` describes the repetition rule (`every` ms or a cron `pattern`).
    /// `template` optionally overrides the default job name / data / options for every
    /// produced job.
    ///
    /// Returns the id and delay of the first (or updated) delayed job.
    pub async fn upsert_job_scheduler(
        &self,
        scheduler_id: &str,
        repeat: &RepeatOptions,
        template: JobSchedulerTemplate<'_, D>,
    ) -> Result<AddJobSchedulerOk, AddJobSchedulerError>
    where
        D: Serialize,
    {
        let now = chrono::Utc::now();
        let next_millis = compute_next_millis(repeat, now).map_err(|e| {
            AddJobSchedulerError::SerializationFailed(serde_json::Error::custom(e))
        })?;

        let scheduler_opts = JobSchedulerOpts {
            name: template.name.into(),
            tz: repeat.tz.clone(),
            pattern: repeat.pattern.clone(),
            end_date: repeat.end_date.map(|dt| dt.timestamp_millis()),
            every: repeat.every,
            offset: repeat.offset,
            start_date: repeat.start_date.map(|dt| dt.timestamp_millis()),
            limit: repeat.limit,
        };

        let mut con = self.pool.get().await?;
        let cmd = AddJobScheduler {
            queue: &self.name,
            job_scheduler_id: scheduler_id,
            next_millis,
            scheduler_opts: &scheduler_opts,
            template_data: template.data,
            template_opts: template.opts,
            delayed_opts: template.opts,
            producer_key: None,
        };
        cmd.call(&mut con).await
    }

    /// Remove a scheduler and its pending next job.
    pub async fn remove_job_scheduler(
        &self,
        scheduler_id: &str,
    ) -> Result<(), RemoveJobSchedulerError> {
        let mut con = self.pool.get().await?;
        let cmd = RemoveJobScheduler {
            queue: &self.name,
            job_scheduler_id: scheduler_id,
        };
        cmd.call(&mut con).await
    }

    /// List all job schedulers for this queue.
    pub async fn get_job_schedulers(&self) -> Result<Vec<JobSchedulerInfo>, BasicRedisError> {
        let mut con = self.pool.get().await?;
        let ids: Vec<String> = con.zrange(self.name.repeat(), 0, -1).await?;
        let mut result = Vec::with_capacity(ids.len());
        for id in ids {
            let cmd = GetJobScheduler {
                queue: &self.name,
                scheduler_id: &id,
            };
            if let Some(GetJobSchedulerOk { fields, next_millis }) =
                cmd.call(&mut con).await.ok().flatten()
            {
                let mut info = JobSchedulerInfo {
                    id,
                    name: None,
                    tz: None,
                    pattern: None,
                    every: None,
                    end_date: None,
                    start_date: None,
                    limit: None,
                    offset: None,
                    iteration_count: None,
                    next_millis,
                };
                let mut it = fields.into_iter();
                while let Some((k, v)) = it.next() {
                    match k.as_str() {
                        "name" => info.name = Some(v),
                        "tz" => info.tz = Some(v),
                        "pattern" => info.pattern = Some(v),
                        "every" => info.every = v.parse().ok(),
                        "endDate" => info.end_date = v.parse().ok(),
                        "startDate" => info.start_date = v.parse().ok(),
                        "limit" => info.limit = v.parse().ok(),
                        "offset" => info.offset = v.parse().ok(),
                        "ic" => info.iteration_count = v.parse().ok(),
                        _ => {}
                    }
                }
                result.push(info);
            }
        }
        Ok(result)
    }

    /**
    Wipe this queue from existence.
    This means all jobs, pending, done, failed, etc, as well as all markers and meta information.
    It makes multiple trips to Redis, as it is unfeasable of deleting potentially millions of jobs in a single lua script execution.
    */
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
