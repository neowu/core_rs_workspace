use std::fmt::Debug;
use std::io;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use futures::FutureExt as _;
use http_body::Frame;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio::time::Interval;
use tokio::time::MissedTickBehavior;
use tokio::time::interval_at;
use tokio_util::sync::CancellationToken;
use tokio_util::sync::WaitForCancellationFutureOwned;

use crate::exception::Exception;
use crate::json;
use crate::log;
use crate::metrics::Counter;
use crate::web::panic_message;

// below common LB idle timeouts (ALB / nginx 60s)
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
const BUFFER_SIZE: usize = 16;

/// One server-sent event.
pub struct Event {
    value: String,
}

impl Event {
    /// `data` is split into one `data:` line per line, CR / LF / CRLF, a trailing line break keeps an empty line.
    pub fn new(data: &str) -> Self {
        let mut value = String::with_capacity(data.len() + 8);
        let mut rest = data;
        loop {
            let end = rest.find(['\r', '\n']);
            value.push_str("data: ");
            value.push_str(&rest[..end.unwrap_or(rest.len())]);
            value.push('\n');
            let Some(end) = end else { break };
            let separator = if rest[end..].starts_with("\r\n") { 2 } else { 1 };
            rest = &rest[end + separator..];
        }
        Self { value }
    }

    pub fn json<T>(data: &T) -> Result<Self, Exception>
    where
        T: serde::Serialize + Debug,
    {
        Ok(Self::new(&json::to_json(data)?))
    }

    /// Panics if `id` contains CR, LF or NUL.
    #[must_use]
    pub fn id(mut self, id: &str) -> Self {
        assert!(!id.contains(['\r', '\n', '\0']), "invalid sse event id, id={id:?}");
        self.value.push_str("id: ");
        self.value.push_str(id);
        self.value.push('\n');
        self
    }

    /// Panics if `name` contains CR or LF.
    #[must_use]
    pub fn event(mut self, name: &str) -> Self {
        assert!(!name.contains(['\r', '\n']), "invalid sse event name, name={name:?}");
        self.value.push_str("event: ");
        self.value.push_str(name);
        self.value.push('\n');
        self
    }

    fn to_bytes(&self) -> Bytes {
        log!("[sse] {:?}", self.value);
        let mut bytes = Vec::with_capacity(self.value.len() + 1);
        bytes.extend_from_slice(self.value.as_bytes());
        bytes.push(b'\n');
        Bytes::from(bytes)
    }
}

/// Sends events to one SSE stream, clones can be kept to broadcast, see `Response::sse`.
#[derive(Clone)]
pub struct SseChannel {
    sender: mpsc::Sender<Bytes>,
    written: Arc<Written>,
}

impl SseChannel {
    /// Waits for buffer space, returns `false` if the stream is closed (client disconnected or server shutdown).
    pub async fn send(&self, event: &Event) -> bool {
        let Ok(permit) = self.sender.reserve().await else { return false };
        self.write(permit, event);
        true
    }

    /// Returns `false` if the buffer is full or the stream is closed, for broadcasting without waiting on slow clients.
    pub fn try_send(&self, event: &Event) -> bool {
        let Ok(permit) = self.sender.try_reserve() else { return false };
        self.write(permit, event);
        true
    }

    // encodes and logs only once the event is accepted by the buffer
    fn write(&self, permit: mpsc::Permit<'_, Bytes>, event: &Event) {
        let bytes = event.to_bytes();
        self.written.add(bytes.len());
        permit.send(bytes);
    }

    /// Completes when the stream is closed.
    pub async fn closed(&self) {
        self.sender.closed().await;
    }

    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

#[derive(Default)]
pub(crate) struct Written {
    pub(crate) entries: AtomicU64,
    pub(crate) bytes: AtomicU64,
}

impl Written {
    fn add(&self, length: usize) {
        self.entries.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(length as u64, Ordering::Relaxed);
    }
}

type SseFuture = Pin<Box<dyn Future<Output = Result<(), Exception>> + Send>>;

struct SseTask {
    handler: Box<dyn FnOnce(SseChannel) -> SseFuture + Send>,
    channel: SseChannel,
    ref_ids: Option<Vec<String>>,
    route: Option<(&'static str, &'static str)>,
}

pub(crate) struct SseBody {
    receiver: mpsc::Receiver<Bytes>,
    keepalive: Interval,
    shutdown: Option<Pin<Box<WaitForCancellationFutureOwned>>>,
    // cancelled when the handler finishes or the body is dropped
    stream: CancellationToken,
    stream_closed: Pin<Box<WaitForCancellationFutureOwned>>,
    closed: bool,
    task: Option<SseTask>,
}

impl SseBody {
    pub(crate) fn new<F, Fut>(handler: F) -> Self
    where
        F: FnOnce(SseChannel) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), Exception>> + Send + 'static,
    {
        let (sender, receiver) = mpsc::channel(BUFFER_SIZE);
        let mut keepalive = interval_at(Instant::now() + KEEPALIVE_INTERVAL, KEEPALIVE_INTERVAL);
        keepalive.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let stream = CancellationToken::new();
        let task = SseTask {
            handler: Box::new(move |channel| Box::pin(handler(channel))),
            channel: SseChannel { sender, written: Arc::default() },
            // created inside the controller, refers to the http action
            ref_ids: log::current_action_id().map(|id| vec![id]),
            route: None,
        };
        SseBody {
            receiver,
            keepalive,
            shutdown: None,
            stream_closed: Box::pin(stream.clone().cancelled_owned()),
            stream,
            closed: false,
            task: Some(task),
        }
    }

    pub(crate) const fn route(&mut self, path: &'static str, name: &'static str) {
        if let Some(task) = &mut self.task {
            task.route = Some((path, name));
        }
    }

    /// Spawns the handler as the `sse` action, it is dropped once the stream closes.
    pub(crate) fn start(&mut self, shutdown: &CancellationToken, counter: &Arc<Counter>) {
        self.shutdown = Some(Box::pin(shutdown.clone().cancelled_owned()));
        let Some(task) = self.task.take() else { return };
        let stream = self.stream.clone();
        let shutdown = shutdown.clone();
        let counter = Arc::clone(counter);
        tokio::spawn(log::action("sse", task.ref_ids, async move {
            if let Some((path, name)) = task.route {
                context!(path = path, fn = name);
            }
            let _active = counter.increase();
            let written = Arc::clone(&task.channel.written);
            // invoke inside the async block, a handler may panic before returning its future
            let handler = AssertUnwindSafe(async { (task.handler)(task.channel).await }).catch_unwind();
            let result = tokio::select! {
                biased;
                result = handler => result.unwrap_or_else(|panic| {
                    Err(exception!(format!("handler panicked, error={}", panic_message(panic.as_ref()))))
                }),
                () = stream.cancelled() => Ok(()),
                // also when hyper stops polling the body, e.g. a client not reading on h2 flow control
                () = shutdown.cancelled() => Ok(()),
            };
            stream.cancel();
            stats!(
                sse_write_entries = written.entries.load(Ordering::Relaxed),
                sse_write_bytes = written.bytes.load(Ordering::Relaxed)
            );
            result
        }));
    }

    pub(crate) fn poll_frame(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
        if self.closed {
            return Poll::Ready(None);
        }
        if let Some(shutdown) = &mut self.shutdown
            && shutdown.as_mut().poll(cx).is_ready()
        {
            // sse data is not critical, close on shutdown, the jittered retry spreads client reconnects
            self.closed = true;
            self.receiver.close();
            let retry = 1000 + rand::random::<u16>() % 4000;
            return Poll::Ready(Some(Ok(Frame::data(Bytes::from(format!("retry: {retry}\n\n"))))));
        }
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(bytes)) => {
                self.keepalive.reset();
                return Poll::Ready(Some(Ok(Frame::data(bytes))));
            }
            Poll::Ready(None) => {
                self.closed = true;
                return Poll::Ready(None);
            }
            Poll::Pending => {}
        }
        // the handler finished, queued events are sent above, channel clones kept elsewhere don't hold the stream
        if self.stream_closed.as_mut().poll(cx).is_ready() {
            self.closed = true;
            return Poll::Ready(None);
        }
        if self.keepalive.poll_tick(cx).is_ready() {
            return Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(b":\n\n")))));
        }
        Poll::Pending
    }

    pub(crate) const fn is_end_stream(&self) -> bool {
        self.closed
    }
}

impl Drop for SseBody {
    fn drop(&mut self) {
        self.stream.cancel();
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::future::poll_fn;

    use tokio::sync::oneshot;

    use super::*;

    fn text(event: &Event) -> String {
        String::from_utf8(event.to_bytes().to_vec()).unwrap()
    }

    async fn next(body: &mut SseBody) -> Option<Bytes> {
        poll_fn(|cx| body.poll_frame(cx)).await.map(|frame| frame.unwrap().into_data().unwrap())
    }

    #[test]
    fn event() {
        assert_eq!(text(&Event::new("hello")), "data: hello\n\n");
        assert_eq!(text(&Event::new("")), "data: \n\n");
        assert_eq!(text(&Event::new("a\nb\r\nc\rd\n")), "data: a\ndata: b\ndata: c\ndata: d\ndata: \n\n");
        assert_eq!(text(&Event::new("a\r")), "data: a\ndata: \n\n");
        assert_eq!(text(&Event::new("\r")), "data: \ndata: \n\n");
        assert_eq!(text(&Event::new("a\r\r\nb\n\rc")), "data: a\ndata: \ndata: b\ndata: \ndata: c\n\n");
        assert_eq!(text(&Event::new("x").id("1").event("update")), "data: x\nid: 1\nevent: update\n\n");
        assert_eq!(text(&Event::json(&vec!["a"]).unwrap()), "data: [\"a\"]\n\n");
    }

    #[test]
    #[should_panic(expected = "invalid sse event id")]
    fn invalid_id() {
        let _event = Event::new("x").id("1\n2");
    }

    #[tokio::test]
    async fn shutdown() {
        let (closed_sender, closed) = oneshot::channel::<()>();
        let mut body = SseBody::new(|channel| async move {
            channel.send(&Event::new("a")).await;
            // dropped with the handler when the stream closes
            let _closed_sender = closed_sender;
            pending::<()>().await;
            Ok(())
        });
        let shutdown = CancellationToken::new();
        let counter = Arc::new(Counter::default());
        body.start(&shutdown, &counter);
        assert_eq!(next(&mut body).await.unwrap(), "data: a\n\n");

        shutdown.cancel();
        // dropped without polling the body, e.g. a stalled client
        assert!(closed.await.is_err());
        assert!(next(&mut body).await.unwrap().starts_with(b"retry: "));
        assert!(next(&mut body).await.is_none());
        assert!(body.is_end_stream());
        assert_eq!(counter.max(), 1);
    }

    #[tokio::test]
    async fn buffer_full() {
        let (sender, receiver) = mpsc::channel(BUFFER_SIZE);
        let channel = SseChannel { sender, written: Arc::default() };
        for _ in 0..BUFFER_SIZE {
            assert!(channel.try_send(&Event::new("a")));
        }
        assert!(!channel.try_send(&Event::new("a")));
        assert_eq!(channel.written.entries.load(Ordering::Relaxed), BUFFER_SIZE as u64);

        drop(receiver);
        assert!(!channel.send(&Event::new("a")).await);
    }

    #[tokio::test]
    async fn handler_finished() {
        let (clone_sender, clone) = oneshot::channel();
        let mut body = SseBody::new(|channel| async move {
            channel.send(&Event::new("a")).await;
            // a clone kept elsewhere doesn't hold the stream open
            let _sent = clone_sender.send(channel.clone());
            Ok(())
        });
        body.start(&CancellationToken::new(), &Arc::new(Counter::default()));
        assert_eq!(next(&mut body).await.unwrap(), "data: a\n\n");
        assert!(next(&mut body).await.is_none());
        assert!(!clone.await.unwrap().is_closed());
    }

    #[tokio::test]
    async fn not_started() {
        // e.g. HEAD drops the body before start, the handler never runs
        let _body = SseBody::new(|_channel| async { panic!("must not run") });
    }
}
