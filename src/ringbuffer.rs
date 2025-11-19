#[derive(Debug, Clone)]
pub(crate) struct RingBuffer<T> {
    data: Vec<T>,
    end: usize,
}

impl<T> RingBuffer<T> {
    pub fn with_capacity(cap: usize) -> RingBuffer<T> {
        RingBuffer {
            data: Vec::with_capacity(cap),
            end: 0,
        }
    }

    pub fn push(&mut self, value: T) {
        let cap = self.data.capacity();

        if self.data.len() == cap {
            self.data[self.end] = value;
            self.end = (self.end + 1) % cap;
        } else {
            self.data.push(value);
        }
    }

    fn last_index(&self) -> Option<usize> {
        let cap = self.data.capacity();
        let len = self.data.len();

        if len > 0 {
            Some((len + self.end - 1) % cap)
        } else {
            None
        }
    }
    pub fn last(&self) -> Option<&T> {
        self.last_index().and_then(|i| Some(&self.data[i]))
    }

    pub fn set_last(&mut self, value: T) {
        let idx = self.last_index();
        if let Some(i) = idx {
            self.data[i] = value;
        }
    }

    pub fn iter(&self) -> impl Iterator<Item=&T> {
        let d = self.data.split_at(self.end);
        d.0.iter().rev().chain(d.1.iter().rev())
    }
}