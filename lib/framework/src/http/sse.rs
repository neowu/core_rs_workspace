use std::collections::VecDeque;
use std::mem;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::task::ready;
use std::time::Instant;

use bytes::Bytes;
use futures::Stream;

use crate::exception::Exception;
use crate::log;
use crate::stats;
use crate::web::Event;

/// Parses `text/event-stream` per the WHATWG spec, an event is dispatched on a blank line.
pub struct EventSource {
    response: Pin<Box<dyn Stream<Item = Result<Bytes, Exception>> + Send>>,
    line: Vec<u8>,
    // CR ends a line, skip the LF of a CRLF split across chunks
    skip_lf: bool,
    events: VecDeque<Event>,
    data: String,
    event_type: Option<String>,
    // persists across events, the value for `last-event-id` on reconnect
    last_id: Option<String>,

    start_time: Instant,
    read_bytes: usize,
    read_entries: usize,
}

impl EventSource {
    pub(super) fn new(response: Pin<Box<dyn Stream<Item = Result<Bytes, Exception>> + Send>>) -> Self {
        EventSource {
            response,
            line: vec![],
            skip_lf: false,
            events: VecDeque::new(),
            data: String::new(),
            event_type: None,
            last_id: None,
            start_time: Instant::now(),
            read_bytes: 0,
            read_entries: 0,
        }
    }

    fn read(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            if byte == b'\n' && mem::take(&mut self.skip_lf) {
                continue;
            }
            if byte == b'\r' || byte == b'\n' {
                self.skip_lf = byte == b'\r';
                // take to borrow self mutably, put back to reuse capacity
                let mut line = mem::take(&mut self.line);
                self.process_line(&String::from_utf8_lossy(&line));
                line.clear();
                self.line = line;
            } else {
                self.skip_lf = false;
                self.line.push(byte);
            }
        }
    }

    fn process_line(&mut self, line: &str) {
        log!("[sse] {line}");
        self.read_bytes += line.len();
        if line.is_empty() {
            self.dispatch();
            return;
        }
        if line.starts_with(':') {
            return;
        }
        let (field, value) = match line.split_once(':') {
            Some((field, value)) => (field, value.strip_prefix(' ').unwrap_or(value)),
            None => (line, ""),
        };
        match field {
            "data" => {
                self.data.push_str(value);
                self.data.push('\n');
            }
            "event" => self.event_type = Some(value.to_owned()),
            "id" if !value.contains('\0') => self.last_id = (!value.is_empty()).then(|| value.to_owned()),
            _ => {}
        }
    }

    fn dispatch(&mut self) {
        let event_type = self.event_type.take();
        if self.data.is_empty() {
            return;
        }
        let mut data = mem::take(&mut self.data);
        data.pop();
        self.events.push_back(Event { id: self.last_id.clone(), event_type, data });
        self.read_entries += 1;
    }
}

impl Drop for EventSource {
    fn drop(&mut self) {
        stats!(
            sse_read_entries = self.read_entries,
            sse_read_bytes = self.read_bytes,
            sse_elapsed = self.start_time.elapsed().as_nanos(),
            sse_count = 1
        );
    }
}

impl Stream for EventSource {
    type Item = Result<Event, Exception>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(event) = self.events.pop_front() {
                return Poll::Ready(Some(Ok(event)));
            }
            // an incomplete event at the end of the stream is discarded
            match ready!(self.response.as_mut().poll_next(cx)) {
                Some(Ok(bytes)) => self.read(&bytes),
                Some(Err(err)) => return Poll::Ready(Some(Err(err))),
                None => return Poll::Ready(None),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::stream;
    use tokio_stream::StreamExt as _;

    use super::*;

    async fn events(chunks: &[&'static str]) -> Vec<(Option<String>, Option<String>, String)> {
        let chunks: Vec<_> = chunks.iter().map(|chunk| Ok(Bytes::from_static(chunk.as_bytes()))).collect();
        let source = EventSource::new(Box::pin(stream::iter(chunks)));
        source.map(|event| event.map(|event| (event.id, event.event_type, event.data)).unwrap()).collect().await
    }

    fn event(id: Option<&str>, event_type: Option<&str>, data: &str) -> (Option<String>, Option<String>, String) {
        (id.map(str::to_owned), event_type.map(str::to_owned), data.to_owned())
    }

    #[tokio::test]
    async fn parse() {
        assert_eq!(events(&["data: a\ndata: b\n\n"]).await, [event(None, None, "a\nb")]);
        // no space after colon, CRLF / CR line endings, CRLF split across chunks
        assert_eq!(events(&["data:a\r", "\ndata:  b\r\r"]).await, [event(None, None, "a\n b")]);
        // comments are ignored, blank lines without data don't dispatch
        assert_eq!(events(&[":\n\n: comment\nevent: x\n\ndata: a\n\n"]).await, [event(None, None, "a")]);
        // id persists across events, event type resets, empty data is still an event
        assert_eq!(
            events(&["id: 1\nevent: list\ndata: a\n\ndata\n\nid\ndata:\n\n"]).await,
            [event(Some("1"), Some("list"), "a"), event(Some("1"), None, ""), event(None, None, "")]
        );
        // trailing data line keeps an empty line, incomplete event at end is discarded
        assert_eq!(events(&["data: a\ndata: \n\ndata: b\n"]).await, [event(None, None, "a\n")]);
    }
}
