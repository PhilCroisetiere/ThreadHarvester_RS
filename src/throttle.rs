use governor::{
    Quota, RateLimiter,
    clock::MonotonicClock,
    state::{InMemoryState, direct::NotKeyed},
};
use std::{num::NonZeroU32, sync::Arc, time::{Duration, SystemTime, UNIX_EPOCH}};
use std::sync::atomic::{AtomicU64, Ordering};


pub type Limiter = Arc<RateLimiter<NotKeyed, InMemoryState, MonotonicClock>>;

pub fn make_limiter(rpm: u32) -> Limiter {
    let q = Quota::per_minute(NonZeroU32::new(rpm.max(1)).unwrap());
    Arc::new(RateLimiter::direct(q))
}

static COOLDOWN_UNTIL: AtomicU64 = AtomicU64::new(0);

pub async fn gate(l: &Limiter) {
    let now = now_secs();
    let until = COOLDOWN_UNTIL.load(Ordering::Relaxed);
    if until > now {
        tokio::time::sleep(Duration::from_secs(until - now)).await;
    }
    l.until_ready().await;
}

pub fn set_cooldown_secs(secs: u64) {
    let until = now_secs() + secs;
    let prev = COOLDOWN_UNTIL.load(Ordering::Relaxed);
    if until > prev {
        COOLDOWN_UNTIL.store(until, Ordering::Relaxed);
    }
}

#[inline]
fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}
