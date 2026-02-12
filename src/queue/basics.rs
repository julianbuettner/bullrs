use serde::{Serialize, de::DeserializeOwned};

use crate::{
    error::{AddJobErr, ObliterateError, PauseResumeError},
    job::JobOptions,
    luacommands::{
        AddDelayedJob, AddPrioritizedJob, AddStandardJob, InvokeLuaScript, Obliterate,
        ObliterateOk, Pause, PauseAction,
    },
    queue::Queue,
    worker::{Worker, WorkerArgs},
};

impl<D, R> Queue<D, R> {
    /// Get a worker for processing jobs of this queue
    pub fn worker(&self, worker_args: WorkerArgs) -> Worker<D, R>
    where
        R: Send + 'static,
        D: Send + Sync + DeserializeOwned + std::fmt::Debug + 'static,
    {
        if self.pool.status().max_size < worker_args.parallel_connections * 2 {
            self.pool.resize(worker_args.parallel_connections * 2);
        }
        Worker::new(self.pool.clone(), self.name.clone(), worker_args)
    }

    /// Stop processing jobs of a queue. Jobs already picked up will be
    /// continued until completed or failed and delayed. Keep in mind,
    /// that workers might pre-pull a few jobs depending on concurrency
    /// settings. Those will be processed too.
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

    pub async fn add_with(
        &self,
        job_name: &str,
        data: &D,
        job_options: &JobOptions,
    ) -> Result<String, AddJobErr>
    where
        D: Serialize,
    {
        let mut con = self.pool.get().await?;
        if job_options.delay.is_some() {
            let c = AddDelayedJob {
                queue: &self.name,
                job_name,
                data,
                job_options,
            };
            return c.call(&mut con).await;
        }
        if job_options.priority.is_some() {
            let c = AddPrioritizedJob {
                queue: &self.name,
                job_name,
                data,
                job_options,
            };
            return c.call(&mut con).await;
        }
        let c = AddStandardJob {
            queue: &self.name,
            job_name,
            data,
            job_options,
        };
        c.call(&mut con).await
    }

    pub async fn add(&self, job_name: &str, data: &D) -> Result<String, AddJobErr>
    where
        D: Serialize,
    {
        let job_options: JobOptions = Default::default();
        self.add_with(job_name, data, &job_options).await
    }

    /// Completely remove everything about this queue.
    /// It makes multiple trips to Redis, as it is unfeasable of
    /// deleting potentially millions of jobs in a single lua
    /// script execution.
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
