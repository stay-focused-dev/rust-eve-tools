use std::time::Duration;

use crate::ringbuffer::RingBuffer;

const CAP: usize = 20;

#[derive(Debug)]
pub struct RatelimitGroup {
    ratelimits: Vec<Ratelimit>,
}

impl RatelimitGroup {
    pub fn new(data: Vec<Ratelimit>) -> Self {
        RatelimitGroup { ratelimits: data }
    }

    pub fn hit_at(&mut self, at: Duration) -> Option<Duration> {
        let res = self.ratelimits.iter().map(|v| v.can_hit_at(at)).max()?;

        if res == None {
            for r in self.ratelimits.iter_mut() {
                r.hit_at(at);
            }
        }

        res
    }
}

#[derive(Debug)]
struct Slot {
    from: Duration,
    hits: usize,
}

#[derive(Debug)]
pub struct Ratelimit {
    data: RingBuffer<Slot>,
    interval: Duration,
    slot_size: Duration,
    limit: usize,
}

impl Ratelimit {
    pub fn new(interval: Duration, limit: usize) -> Self {
        let slot_size = interval / CAP as u32;

        Ratelimit {
            interval: interval,
            slot_size: slot_size,
            data: RingBuffer::with_capacity(CAP + 1),
            limit: limit,
        }
    }

    fn can_hit_at(&self, at: Duration) -> Option<Duration> {
        let slot_at = self.slot_at(at);

        let mut s = 0;
        for slot in self.data.iter() {
            if slot.from + self.interval < slot_at {
                break;
            }
            s += slot.hits;
            if s >= self.limit {
                return Some(slot.from + self.interval + self.slot_size - at)
            }
        }
        None
    }

    fn hit_at(&mut self, at: Duration) {
        let slot_from = self.slot_at(at);

        if let Some(v) = self.data.last() {
            if v.from == slot_from {
                self.data.set_last(Slot { from: slot_from, hits: v.hits + 1 });
            } else {
                self.data.push(Slot { from: slot_from, hits: 1 });
            }
        } else {
            self.data.push(Slot { from: slot_from, hits: 1 });
        }
    }

    fn slot_at(&self, at: Duration) -> Duration {
        let at = at.as_nanos();
        let slot_size = self.slot_size.as_nanos();

        let slot_from = at - at % slot_size;
        Duration::new((slot_from / 1_000_000_000) as u64, (slot_from % 1_000_000_000) as u32)
    }
}