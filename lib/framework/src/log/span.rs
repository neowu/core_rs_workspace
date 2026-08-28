use std::borrow::Cow;
use std::time::Instant;

use crate::log::CURRENT_ACTION;
use crate::log::elapsed;
use crate::write_str;

pub struct Span {
    name: &'static str,
    start_time: Instant,
    log_index: usize,
}

#[doc(hidden)]
#[inline]
pub fn __span(name: &'static str, location: &'static str) -> Span {
    let mut log_index: usize = 0;
    let _result = CURRENT_ACTION.try_with(|action| {
        if let Some(action) = action.borrow_mut().as_mut() {
            action.log(&format!("[span:{name}] >"), location);
            log_index = action.logs.len();
        }
    });
    Span { name, start_time: Instant::now(), log_index }
}

impl Span {
    pub fn clear(&self) {
        let _result = CURRENT_ACTION.try_with(|action| {
            if let Some(action) = action.borrow_mut().as_mut() {
                action.logs.truncate(self.log_index);
                if let Some(last) = action.logs.last_mut()
                    && last.ends_with('>')
                {
                    last.push_str(" ...(truncated)");
                }
            }
        });
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        let _result = CURRENT_ACTION.try_with(|action| {
            if let Some(action) = action.borrow_mut().as_mut() {
                let name = self.name;
                let span_elapsed = self.start_time.elapsed();

                let (minutes, seconds, nanos) = elapsed(action.start_time);
                let mut log = String::with_capacity(256);
                write_str!(log, "{minutes:02}:{seconds:02}.{nanos:09} [span:{name}] elapsed={span_elapsed:?} <");
                action.logs.push(log);

                let total_elapsed = action.stats.entry(Cow::Owned(format!("{name}_elapsed"))).or_default();
                *total_elapsed += span_elapsed.as_nanos() as u64;
                let count = action.stats.entry(Cow::Owned(format!("{name}_count"))).or_default();
                *count += 1;
            }
        });
    }
}
