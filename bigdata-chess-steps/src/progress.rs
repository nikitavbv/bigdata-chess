// I am not using indicatif for this, because it cannot output progress to non-tty
use {
    std::time::Instant,
    tracing::info,
};

pub struct Progress {
    message: String,
    started_at: Instant,
    reported_at: Instant,
    total_processed: u64,
}

impl Progress {
    pub fn new(message: String) -> Self {
        Self {
            message,
            started_at: Instant::now(),
            reported_at: Instant::now(),
            total_processed: 0,
        }
    }

    pub fn update(&mut self) -> bool {
        self.total_processed += 1;

        let now = Instant::now();
        if (now - self.reported_at).as_millis() >= 10_000 {
            self.reported_at = now;
            let rate = (self.total_processed as f32) / (now - self.started_at).as_secs_f32();
            info!("{}: {} total ({:.2}/second)", self.message, self.total_processed, rate);
            true
        } else {
            false
        }
    }

    pub fn reset(&mut self) {
        self.started_at = Instant::now();
        self.reported_at = Instant::now();
        self.total_processed = 0;
    }
}