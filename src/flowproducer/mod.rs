use deadpool_redis::Pool;

use crate::PreparedFlowJob;

#[derive(Clone)]
pub struct FlowProducer {
    pool: Pool,
}

pub struct FlowJob {
    job: PreparedFlowJob,
    children: Vec<FlowJob>,
}

impl FlowProducer {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    pub async fn add(&self, fj: &[FlowJob]) -> Result<(), ()> {
        let mut flatten = Vec::new();
        fj.iter()
            .for_each(|j| j.collect_children_first(&mut flatten));
        let _con = self.pool.get().await.map_err(|_| ())?;
        Ok(())
    }
}

impl FlowJob {
    pub fn new(job: PreparedFlowJob) -> Self {
        Self {
            job,
            children: Vec::new(),
        }
    }
    pub fn add(&mut self, child: FlowJob) {
        self.children.push(child);
    }
    pub fn with(mut self, child: FlowJob) -> FlowJob {
        self.children.push(child);
        self
    }

    fn collect_children_first<'a>(&'a self, v: &mut Vec<&'a PreparedFlowJob>) {
        for child in &self.children {
            child.collect_children_first(v);
        }
        v.push(&self.job);
    }
}
