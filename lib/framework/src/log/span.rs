use std::time::Instant;

use crate::log::CURRENT_ACTION;

pub struct Span {
    name: &'static str,
    start_time: Instant,
    elapsed_key: &'static str,
    count_key: &'static str,
    /// Byte offset of the end of the span line, None when the span was created outside an action.
    log_offset: Option<usize>,
}

#[macro_export]
macro_rules! span {
    // a span name becomes a stats key, so it must be a literal: concat! builds both keys with no allocation
    ($name:literal) => {
        $crate::log::__span(
            $name,
            concat!($name, "_elapsed"),
            concat!($name, "_count"),
            concat!(module_path!(), ":", line!()),
        )
    };
}

#[doc(hidden)]
#[inline]
pub fn __span(name: &'static str, elapsed_key: &'static str, count_key: &'static str, location: &'static str) -> Span {
    let mut log_offset = None;
    let _result = CURRENT_ACTION.try_with(|action| {
        if let Some(action) = action.borrow_mut().as_mut() {
            action.log(None, None, Some(location), format_args!("[span:{name}] >"));
            log_offset = Some(action.logs.len() - 1); // the last char of logs is always be '\n', truncate to this point
        }
    });
    Span { name, start_time: Instant::now(), elapsed_key, count_key, log_offset }
}

impl Span {
    /// Rolls the trace back to the start of the span, so a long loop cannot fill the action buffer.
    pub fn clear(&self) {
        let _result = CURRENT_ACTION.try_with(|action| {
            if let Some(action) = action.borrow_mut().as_mut()
                && let Some(offset) = self.log_offset
                && offset <= action.logs.len()
                // generally span is always perfect nested
                // in case a crossed span can hold a stale offset, truncating off a char boundary would panic
                && action.logs.is_char_boundary(offset)
            {
                action.logs.truncate(offset);
                if action.logs.ends_with('>') {
                    action.logs.push_str(" ...(truncated)\n");
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

                action.log(None, None, None, format_args!("[span:{name}] elapsed={span_elapsed:?} <"));

                action.add_stat(self.elapsed_key, span_elapsed.as_nanos() as u64);
                action.add_stat(self.count_key, 1);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use futures::executor::block_on;

    use crate::log::CURRENT_ACTION;
    use crate::log::action::Action;
    use crate::time::DateTime;

    fn with_action<F: FnOnce()>(task: F) -> Action {
        let scope = CURRENT_ACTION.scope(
            RefCell::new(Some(Action::new("id".to_owned(), "test", None, DateTime::now()))),
            async {
                task();
                CURRENT_ACTION.with(|current| current.take().expect("action must be in scope"))
            },
        );
        block_on(scope)
    }

    #[test]
    fn span_accumulates_stats_under_compile_time_keys() {
        let action = with_action(|| {
            let _span1 = span!("db");
            let _span2 = span!("db");
        });

        assert_eq!(action.stats.iter().find(|(key, _)| *key == "db_count").map(|(_, value)| *value), Some(2));
        assert!(action.stats.iter().any(|(key, _)| *key == "db_elapsed"));
    }

    #[test]
    fn clear() {
        let action = with_action(|| {
            let span = span!("long");
            for i in 0..10 {
                span.clear();
                crate::log!("message, i={i}");
            }
        });

        assert!(action.logs.contains("[span:long] > ...(truncated)\n"));
        assert!(!action.logs.contains("message, i=5"));
        assert!(action.logs.contains("message, i=9"));
    }
}
