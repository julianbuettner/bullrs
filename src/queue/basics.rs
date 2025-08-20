use serde::{Serialize, de::DeserializeOwned};

use crate::{
    job::JobOptions,
    luacommands::{AddDelayedJob, AddStandardJob, InvokeLuaScript, Pause, PauseAction},
    queue::Queue,
    worker::{Worker, WorkerArgs},
};

impl<D, R> Queue<D, R> {
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
        let c = AddStandardJob {
            queue: &self.name,
            job_name,
            data,
            job_options,
        };
        Ok(c.call(&mut con).await?)
    }

    pub async fn add(&mut self, job_name: &str, data: &D) -> anyhow::Result<String>
    where
        D: Serialize,
    {
        let job_options: JobOptions = Default::default();
        self.add_with(job_name, data, &job_options).await
    }
}
