mod stream_reader;

use std::{collections::HashMap, fmt::Debug};

use deadpool_redis::Pool;
use redis::Value;
use serde::de::DeserializeOwned;
use tokio::{sync::broadcast, task::JoinHandle};
use tracing::{Instrument, Level, span, warn};

use crate::{QueueName, worker::shutdown_switch::ShutdownSwitch};
use stream_reader::extract_string;

#[derive(Debug, Clone)]
pub enum QueueEvent<R: Debug + Clone> {
    Waiting {
        job_id: String,
        prev: Option<String>,
    },
    Active {
        job_id: String,
        prev: Option<String>,
    },
    Completed {
        job_id: String,
        return_value: R,
        prev: Option<String>,
    },
    Failed {
        job_id: String,
        failed_reason: Option<String>,
        prev: Option<String>,
    },
    Delayed {
        job_id: String,
        delay: Option<String>,
    },
    Stalled {
        job_id: String,
    },
    Progress {
        job_id: String,
        data: Option<String>,
    },
    Added {
        job_id: String,
        name: Option<String>,
    },
    Removed {
        job_id: String,
        prev: Option<String>,
    },
    Drained,
    Cleaned {
        count: Option<String>,
    },
    Paused,
    Resumed,
    WaitingChildren {
        job_id: String,
        prev: Option<String>,
    },
    RetriesExhausted {
        job_id: String,
        attempts_made: Option<String>,
    },
    Duplicated {
        job_id: String,
    },
    Debounced {
        job_id: String,
    },
    Deduplicated {
        job_id: String,
    },
}

impl<R: Debug + Clone + DeserializeOwned> QueueEvent<R> {
    pub(crate) fn parse(fields: &HashMap<String, Value>) -> Option<Self> {
        let event = fields.get("event").and_then(extract_string)?;
        let job_id = || fields.get("jobId").and_then(extract_string);
        let prev = || fields.get("prev").and_then(extract_string);

        let parsed = match event.as_str() {
            "waiting" => QueueEvent::Waiting {
                job_id: job_id()?,
                prev: prev(),
            },
            "active" => QueueEvent::Active {
                job_id: job_id()?,
                prev: prev(),
            },
            "completed" => {
                let job_id = job_id()?;
                let raw = fields.get("returnvalue").and_then(extract_string)?;
                let return_value: R = match serde_json::from_str(&raw) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(
                            job_id = %job_id,
                            "Failed to deserialize return value: {e}"
                        );
                        return None;
                    }
                };
                QueueEvent::Completed {
                    job_id,
                    return_value,
                    prev: prev(),
                }
            }
            "failed" => QueueEvent::Failed {
                job_id: job_id()?,
                failed_reason: fields.get("failedReason").and_then(extract_string),
                prev: prev(),
            },
            "delayed" => QueueEvent::Delayed {
                job_id: job_id()?,
                delay: fields.get("delay").and_then(extract_string),
            },
            "stalled" => QueueEvent::Stalled { job_id: job_id()? },
            "progress" => QueueEvent::Progress {
                job_id: job_id()?,
                data: fields.get("data").and_then(extract_string),
            },
            "added" => QueueEvent::Added {
                job_id: job_id()?,
                name: fields.get("name").and_then(extract_string),
            },
            "removed" => QueueEvent::Removed {
                job_id: job_id()?,
                prev: prev(),
            },
            "drained" => QueueEvent::Drained,
            "cleaned" => QueueEvent::Cleaned {
                count: fields.get("count").and_then(extract_string),
            },
            "paused" => QueueEvent::Paused,
            "resumed" => QueueEvent::Resumed,
            "waiting-children" => QueueEvent::WaitingChildren {
                job_id: job_id()?,
                prev: prev(),
            },
            "retries-exhausted" => QueueEvent::RetriesExhausted {
                job_id: job_id()?,
                attempts_made: fields.get("attemptsMade").and_then(extract_string),
            },
            "duplicated" => QueueEvent::Duplicated { job_id: job_id()? },
            "debounced" => QueueEvent::Debounced { job_id: job_id()? },
            "deduplicated" => QueueEvent::Deduplicated { job_id: job_id()? },
            other => {
                warn!("Unknown event type: {other}");
                return None;
            }
        };

        Some(parsed)
    }
}

pub(crate) struct EventSystem<R: Debug + Clone> {
    event_tx: broadcast::Sender<QueueEvent<R>>,
    _task_handle: JoinHandle<()>,
    _shutdown_switch: ShutdownSwitch,
}

impl<R> EventSystem<R>
where
    R: Debug + Clone + Send + 'static + DeserializeOwned,
{
    pub fn new(pool: Pool, queue_name: QueueName) -> Self {
        let (event_tx, _) = broadcast::channel(256);
        let shutdown_switch = ShutdownSwitch::new();

        let events_span = span!(Level::TRACE, "events", queue = queue_name.as_str());
        let task_handle = tokio::spawn(
            stream_reader::listen_to_events(
                pool,
                queue_name,
                shutdown_switch.clone(),
                event_tx.clone(),
            )
            .instrument(events_span),
        );

        Self {
            event_tx,
            _task_handle: task_handle,
            _shutdown_switch: shutdown_switch,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<QueueEvent<R>> {
        self.event_tx.subscribe()
    }
}
