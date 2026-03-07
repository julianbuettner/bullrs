use std::fmt::Debug;

use serde::{Serialize, de::DeserializeOwned};

use crate::{
    error::{AddJobErr, ObliterateError, PauseResumeError},
    job::{JobJoinHandle, JobOptions},
    luacommands::{
        AddDelayedJob, AddPrioritizedJob, AddStandardJob, InvokeLuaScript, Obliterate,
        ObliterateOk, Pause, PauseAction,
    },
    queue::Queue,
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

        let event_rx = self.event_system.subscribe();
        Ok(JobJoinHandle::new(
            self.name.clone(),
            self.pool.clone(),
            job_id,
            event_rx,
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
