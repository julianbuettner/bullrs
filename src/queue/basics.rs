use anyhow::bail;
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    job::JobOptions,
    luacommands::{
        AddDelayedJob, AddPrioritizedJob, AddPrioritizedJobOk, AddStandardJob, InvokeLuaScript,
        Obliterate, ObliterateReturn, Pause, PauseAction,
    },
    queue::Queue,
    worker::{Worker, WorkerArgs},
};

impl<D, R> Queue<D, R> {
    /// Get a worker for processing jobs of this queue
    pub fn worker(&self, worker_args: WorkerArgs) -> Worker<D, R>
    where
        R: Send + 'static,
        D: Send + DeserializeOwned + std::fmt::Debug + 'static,
    {
        if self.pool.status().max_size < worker_args.parallel_connections * 2 {
            self.pool.resize(worker_args.parallel_connections * 2);
        }
        Worker::new(self.pool.clone(), self.name.clone(), worker_args)
    }

    pub async fn pause(&self) {
        let p = Pause {
            queue: &self.name,
            action: PauseAction::Pause,
        };
        let mut con = self.pool.get().await.expect("TODO");
        p.call(&mut con).await.expect("TODO");
    }
    pub async fn resume(&self) {
        let p = Pause {
            queue: &self.name,
            action: PauseAction::Resume,
        };
        let mut con = self.pool.get().await.expect("TODO");
        p.call(&mut con).await.expect("TODO");
    }

    pub async fn add_with(
        &self,
        job_name: &str,
        data: &D,
        job_options: &JobOptions,
    ) -> anyhow::Result<String>
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
            return Ok(c.call(&mut con).await?);
        }
        if job_options.priority.is_some() {
            let c = AddPrioritizedJob {
                queue: &self.name,
                job_name,
                data,
                job_options,
            };
            match c.call(&mut con).await {
                Ok(AddPrioritizedJobOk::JobId(job_id)) => return Ok(job_id),
                Ok(AddPrioritizedJobOk::MissingParentKey) => bail!("Bad."),
                Err(e) => bail!("Bad too {e:?}"),
            }
        }
        let c = AddStandardJob {
            queue: &self.name,
            job_name,
            data,
            job_options,
        };
        Ok(c.call(&mut con).await?)
    }

    pub async fn add(&self, job_name: &str, data: &D) -> anyhow::Result<String>
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
    pub async fn obliterate(self) {
        self.pause().await;
        let mut con = self.pool.get().await.expect("TODO");
        loop {
            let ob = Obliterate {
                queue: &self.name,
                batch_size: 1000,
                force: true,
            };
            let res = ob.call(&mut con).await.expect("TODO");
            match res {
                ObliterateReturn::Progress => (),
                ObliterateReturn::Obliterated => break,
                ObliterateReturn::ActiveJobs => panic!("ActiveJobs"),
                ObliterateReturn::NotPaused => {
                    panic!("Should have been paused")
                }
            }
        }
    }
}
