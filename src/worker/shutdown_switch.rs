use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::{sync::Notify, time::timeout};

/// A switch, which can be only toggled once to shut it down.
#[derive(Clone)]
pub struct ShutdownSwitch {
    toggle: Arc<AtomicBool>,
    notification: Arc<Notify>,
}

impl ShutdownSwitch {
    pub fn new() -> Self {
        Self {
            toggle: Arc::new(false.into()),
            notification: Default::default(),
        }
    }
    pub fn shutdown(&self) {
        self.toggle.store(true, Ordering::Relaxed);
        self.notification.notify_waiters();
    }
    pub fn running(&self) -> bool {
        !self.toggle.load(Ordering::Relaxed)
    }
    async fn block_until_shutdown(&self) {
        let cooldown = Duration::from_secs(1);
        while self.running() {
            // I am unusre, if a notification could be missed.
            // So we wait for at most 1s
            let _ = timeout(cooldown, self.notification.notified()).await;
        }
    }
    /// Sleeps for the given duration, but returns early if shutdown is triggered.
    pub async fn sleep(&self, duration: Duration) -> ShutdownSleep {
        if !self.running() {
            return ShutdownSleep::Shutdown;
        }
        match timeout(duration, self.block_until_shutdown()).await {
            Ok(_) => ShutdownSleep::Shutdown,
            Err(_) => ShutdownSleep::Slept,
        }
    }
}

pub enum ShutdownSleep {
    /// Worker is being shut down
    Shutdown,
    /// Regular sleep finished
    Slept,
}

impl Drop for ShutdownSwitch {
    fn drop(&mut self) {
        self.shutdown();
    }
}
