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
        AddDelayedJob, AddJobScheduler, AddJobSchedulerOk, AddPrioritizedJob, AddStandardJob,
        GetJobScheduler, InvokeLuaScript, Obliterate, ObliterateOk, Pause, PauseAction,
        RemoveJobScheduler,
    },
    queue::Queue,
    scheduler::compute_next_millis,
    worker::{Worker, WorkerArgs},
};

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

    /// Add multiple jobs using a single connection from the pool.
    ///
    /// Each tuple is `(job_name, data, options)` — the same arguments as
    /// [`Self::add_with`]. Jobs are enqueued in order. If any job fails, the
    /// jobs enqueued before the failure are **not** rolled back.
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

        let mut con = self.pool.get().await?;
        let mut handles = Vec::with_capacity(jobs.len());

        for &(job_name, data, job_options) in jobs {
            let job_id = if job_options.delay.is_some() {
                AddDelayedJob {
                    queue: &self.name,
                    job_name,
                    data,
                    job_options,
                }
                .call(&mut *con)
                .await?
            } else if job_options.priority.is_some() {
                AddPrioritizedJob {
                    queue: &self.name,
                    job_name,
                    data,
                    job_options,
                }
                .call(&mut *con)
                .await?
            } else {
                AddStandardJob {
                    queue: &self.name,
                    job_name,
                    data,
                    job_options,
                }
                .call(&mut *con)
                .await?
            };

            handles.push(JobJoinHandle::new(
                self.name.clone(),
                self.pool.clone(),
                job_id,
                self.event_system.clone(),
            ));
        }

        Ok(handles)
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
