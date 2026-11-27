#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) use unix::dump;

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) fn dump() -> String {
    "thread dump is not supported on this platform".to_owned()
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod unix {
    use std::ffi::c_int;
    use std::ffi::c_void;
    use std::mem;
    use std::ptr;
    use std::sync::Mutex;
    use std::sync::Once;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicU8;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::thread;
    use std::time::Duration;
    use std::time::Instant;

    use crate::log::id_generator::Id;
    use crate::log::inflight;
    use crate::string::StringExt as _;
    use crate::write_str;

    // ignored by default, so a stray one is harmless, and nothing in a server uses tcp urgent data
    const SIGNAL: c_int = libc::SIGURG;
    const MAX_FRAMES: usize = 128;
    const MAX_SYMBOL_LEN: usize = 200;
    // a thread that doesn't answer has the signal blocked
    const TIMEOUT: Duration = Duration::from_millis(100);

    // one dump at a time, threads are captured one by one into the statics below
    static DUMP: Mutex<()> = Mutex::new(());
    static INSTALL: Once = Once::new();

    // thread the handler answers for, 0 when none, so a stray or late signal does nothing
    static TARGET: AtomicU64 = AtomicU64::new(0);
    // thread that has finished writing its capture
    static CAPTURED: AtomicU64 = AtomicU64::new(0);
    static FRAMES: [AtomicUsize; MAX_FRAMES] = [const { AtomicUsize::new(0) }; MAX_FRAMES];
    static DEPTH: AtomicUsize = AtomicUsize::new(0);
    static ACTION_ID: [AtomicU8; Id::LEN] = [const { AtomicU8::new(0) }; Id::LEN];
    static HAS_ACTION: AtomicBool = AtomicBool::new(false);

    pub(super) struct Thread {
        // tid on linux, pthread_t on macos
        id: u64,
        name: String,
        // os specific, appended to the header
        info: String,
    }

    struct Capture {
        frames: Vec<usize>,
        action_id: Option<String>,
    }

    // runs on the interrupted thread, on top of whatever it was doing: no allocation, no lock
    extern "C" fn capture(_signal: c_int) {
        let thread = os::current_thread();
        if TARGET.load(Ordering::Acquire) != thread {
            return;
        }
        // SAFETY: errno is thread local, restored so the interrupted code never sees the handler's
        let errno = unsafe { *os::errno() };

        let mut depth = 0;
        // SAFETY: only walks this thread's stack, the closure writes to statics
        unsafe {
            backtrace::trace_unsynchronized(|frame| {
                FRAMES[depth].store(frame.ip() as usize, Ordering::Relaxed);
                depth += 1;
                depth < MAX_FRAMES
            });
        }
        DEPTH.store(depth, Ordering::Relaxed);
        HAS_ACTION.store(inflight::copy_current_id(&ACTION_ID), Ordering::Relaxed);

        // SAFETY: as above
        unsafe {
            *os::errno() = errno;
        }
        CAPTURED.store(thread, Ordering::Release);
    }

    #[allow(clippy::fn_to_numeric_cast_any)]
    fn install() {
        INSTALL.call_once(|| {
            // SAFETY: plain sigaction, SA_RESTART so interrupted syscalls resume where the kernel can
            unsafe {
                let mut action: libc::sigaction = mem::zeroed();
                action.sa_sigaction = capture as extern "C" fn(c_int) as usize;
                action.sa_flags = libc::SA_RESTART;
                libc::sigemptyset(&raw mut action.sa_mask);
                libc::sigaction(SIGNAL, &raw const action, ptr::null_mut());
            }
        });
    }

    fn capture_thread(thread: &Thread) -> Result<Capture, &'static str> {
        CAPTURED.store(0, Ordering::Release);
        TARGET.store(thread.id, Ordering::Release);
        let result = if os::signal(thread.id) { wait_captured(thread.id) } else { Err("exited") };
        TARGET.store(0, Ordering::Release);
        result
    }

    fn wait_captured(thread: u64) -> Result<Capture, &'static str> {
        let start = Instant::now();
        while CAPTURED.load(Ordering::Acquire) != thread {
            if start.elapsed() > TIMEOUT {
                return Err("no response, signal blocked");
            }
            thread::yield_now();
        }
        let depth = DEPTH.load(Ordering::Relaxed).min(MAX_FRAMES);
        let frames = FRAMES.iter().take(depth).map(|frame| frame.load(Ordering::Relaxed)).collect();
        let action_id = HAS_ACTION.load(Ordering::Relaxed).then(|| {
            let id: Vec<u8> = ACTION_ID.iter().map(|byte| byte.load(Ordering::Relaxed)).collect();
            String::from_utf8_lossy(&id).into_owned()
        });
        Ok(Capture { frames, action_id })
    }

    // one line per frame, inlined frames included, without the handler's own frames
    fn write_frames(out: &mut String, frames: &[usize]) {
        let mut lines = Vec::with_capacity(frames.len());
        for &frame in frames {
            let mut resolved = false;
            backtrace::resolve(frame as *mut c_void, |symbol| {
                resolved = true;
                let mut line = String::new();
                match symbol.name() {
                    Some(name) => {
                        write_str!(line, "{name:#}");
                        // generic names run to thousands of chars
                        if line.len() > MAX_SYMBOL_LEN {
                            let len = line.truncate_to_max(MAX_SYMBOL_LEN).len();
                            line.truncate(len);
                            line.push_str("...");
                        }
                    }
                    None => line.push_str("<unknown>"),
                }
                if let (Some(file), Some(lineno)) = (symbol.filename(), symbol.lineno()) {
                    write_str!(line, " ({}:{lineno})", file.display());
                }
                lines.push(line);
            });
            if !resolved {
                lines.push(format!("<unknown> {frame:#x}"));
            }
        }

        // the handler, then the signal trampoline that called it
        let skip = lines.iter().rposition(|line| line.contains("thread::unix::capture")).map_or(0, |index| index + 2);
        for line in lines.iter().skip(skip) {
            write_str!(out, "  {line}\n");
        }
    }

    /// Text dump of every thread: name, the action it is polling if any, and its stack.
    pub(crate) fn dump() -> String {
        let _guard = DUMP.lock().unwrap();
        install();

        // capture everything first, resolving symbols is slow and the stacks should be close in time
        let threads = os::threads();
        let captures: Vec<_> = threads.iter().map(capture_thread).collect();

        // threads with the same stack share one, idle workers are most of a dump
        let mut groups: Vec<(&[usize], String)> = Vec::new();
        let mut out = String::new();
        write_str!(out, "threads={}\n", threads.len());
        for (thread, capture) in threads.iter().zip(&captures) {
            let mut header = format!("# [thread] id={}, name={}{}", thread.id, thread.name, thread.info);
            match capture {
                Ok(capture) => {
                    if let Some(action_id) = &capture.action_id {
                        write_str!(header, ", action={action_id}");
                    }
                    header.push('\n');
                    match groups.iter_mut().find(|(frames, _)| *frames == capture.frames) {
                        Some((_, headers)) => headers.push_str(&header),
                        None => groups.push((&capture.frames, header)),
                    }
                }
                Err(reason) => write_str!(out, "\n{header}\n  ({reason})\n"),
            }
        }
        for (frames, headers) in &groups {
            out.push('\n');
            out.push_str(headers);
            write_frames(&mut out, frames);
        }
        // the parsed debug info is large and not needed until the next dump
        backtrace::clear_symbol_cache();
        out
    }

    #[cfg(target_os = "linux")]
    mod os {
        use std::ffi::c_int;
        use std::fs;
        use std::time::Duration;

        use super::SIGNAL;
        use super::Thread;

        pub(super) fn threads() -> Vec<Thread> {
            let Ok(tasks) = fs::read_dir("/proc/self/task") else {
                return Vec::new();
            };
            // SAFETY: plain sysconf
            let clock_ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) }.max(1) as u64;
            let mut threads: Vec<Thread> = tasks
                .filter_map(|task| task.ok()?.file_name().to_str()?.parse::<u64>().ok())
                .map(|id| {
                    let name = fs::read_to_string(format!("/proc/self/task/{id}/comm")).unwrap_or_default();
                    let stat = fs::read_to_string(format!("/proc/self/task/{id}/stat")).unwrap_or_default();
                    Thread { id, name: name.trim().to_owned(), info: info(&stat, clock_ticks) }
                })
                .collect();
            threads.sort_by_key(|thread| thread.id);
            threads
        }

        // `pid (comm) state ..`, comm may have spaces and parentheses, utime and stime are fields 14 and 15
        fn info(stat: &str, clock_ticks: u64) -> String {
            let Some((_, fields)) = stat.rsplit_once(')') else {
                return String::new();
            };
            let fields: Vec<&str> = fields.split_ascii_whitespace().collect();
            let ticks = |index: usize| fields.get(index).and_then(|value| value.parse::<u64>().ok()).unwrap_or(0);
            let cpu = Duration::from_millis((ticks(11) + ticks(12)) * 1000 / clock_ticks);
            format!(", state={}, cpu={cpu:?}", fields.first().copied().unwrap_or("?"))
        }

        pub(super) fn current_thread() -> u64 {
            // SAFETY: a plain syscall, async signal safe
            unsafe { libc::gettid() as u64 }
        }

        pub(super) fn signal(thread: u64) -> bool {
            // SAFETY: a thread that has exited is ESRCH, the tid is scoped to this process
            unsafe { libc::syscall(libc::SYS_tgkill, libc::getpid(), thread as libc::pid_t, SIGNAL) == 0 }
        }

        pub(super) fn errno() -> *mut c_int {
            // SAFETY: always valid for the calling thread
            unsafe { libc::__errno_location() }
        }

        #[cfg(test)]
        mod tests {
            #[test]
            fn info() {
                let stat = "42 (tokio (rt) w) R 1 42 42 0 -1 4194368 10 0 0 0 150 50 0 0 20 0 8 0 100 0 0";
                assert_eq!(super::info(stat, 100), ", state=R, cpu=2s");
                assert_eq!(super::info("", 100), "");
            }
        }
    }

    // for local development, a thread exiting between the listing and the signal is not handled
    #[cfg(target_os = "macos")]
    mod os {
        use std::ffi::CStr;
        use std::ffi::c_int;
        use std::ptr;

        use super::SIGNAL;
        use super::Thread;

        unsafe extern "C" {
            static mach_task_self_: libc::mach_port_t;
            fn mach_port_deallocate(task: libc::mach_port_t, name: libc::mach_port_t) -> libc::kern_return_t;
        }

        pub(super) fn threads() -> Vec<Thread> {
            let mut threads = Vec::new();
            // SAFETY: the list and each thread port are owned by the caller and released below
            unsafe {
                let task = mach_task_self_;
                let mut list: libc::thread_act_array_t = ptr::null_mut();
                let mut count: libc::mach_msg_type_number_t = 0;
                if libc::task_threads(task, &raw mut list, &raw mut count) != 0 {
                    return threads;
                }
                for index in 0..count as usize {
                    let port = *list.add(index);
                    let pthread = libc::pthread_from_mach_thread_np(port);
                    mach_port_deallocate(task, port);
                    // not a pthread, cannot be signalled
                    if pthread == 0 {
                        continue;
                    }
                    let mut name = [0; 64];
                    libc::pthread_getname_np(pthread, name.as_mut_ptr(), name.len());
                    let name = CStr::from_ptr(name.as_ptr()).to_string_lossy().into_owned();
                    threads.push(Thread { id: pthread as u64, name, info: String::new() });
                }
                libc::vm_deallocate(task, list as libc::vm_address_t, count as usize * size_of::<libc::thread_act_t>());
            }
            threads
        }

        pub(super) fn current_thread() -> u64 {
            // SAFETY: async signal safe
            unsafe { libc::pthread_self() as u64 }
        }

        pub(super) fn signal(thread: u64) -> bool {
            // SAFETY: the pthread was just listed
            unsafe { libc::pthread_kill(thread as libc::pthread_t, SIGNAL) == 0 }
        }

        pub(super) fn errno() -> *mut c_int {
            // SAFETY: always valid for the calling thread
            unsafe { libc::__error() }
        }
    }

    #[cfg(test)]
    mod tests {
        use std::pin::pin;
        use std::sync::mpsc;
        use std::sync::mpsc::Receiver;
        use std::thread;

        use super::dump;
        use crate::log::action::Action;
        use crate::log::id_generator::next_id;
        use crate::log::inflight::ActionCell;
        use crate::time::DateTime;

        #[inline(never)]
        fn blocked_in_test(release: &Receiver<()>) {
            let _released = release.recv();
        }

        // the thread's header and the stack of its group
        fn section<'a>(dump: &'a str, name: &str) -> (&'a str, &'a str) {
            dump.split("\n\n")
                .find_map(|section| {
                    let header = section.lines().find(|line| line.starts_with("# [thread]") && line.contains(name))?;
                    Some((header, section))
                })
                .unwrap_or_else(|| panic!("thread must be in dump, name={name}, dump={dump}"))
        }

        #[test]
        fn dump_blocked_thread_and_its_action() {
            let (ready, started) = mpsc::channel();
            let (release, released) = mpsc::channel();
            let id = next_id(0);
            let thread = thread::Builder::new()
                .name("dump-blocked".to_owned())
                .spawn(move || {
                    let action = Action::new(id, "test", None, DateTime::now());
                    let cell = pin!(ActionCell::new(action));
                    cell.as_ref().poll(|| {
                        ready.send(()).expect("test must be waiting");
                        blocked_in_test(&released);
                    });
                })
                .expect("thread must start");
            started.recv().expect("thread must be running");

            let dump = thread::Builder::new()
                .name("dump-caller".to_owned())
                .spawn(dump)
                .expect("thread must start")
                .join()
                .expect("dump must not panic");
            release.send(()).expect("thread must be blocked");
            thread.join().expect("thread must not panic");

            assert!(dump.starts_with("threads="), "dump={dump}");
            let (header, blocked) = section(&dump, "name=dump-blocked");
            assert!(header.ends_with(&format!(", action={id}")), "{header}");
            assert!(blocked.contains("blocked_in_test"), "{blocked}");
            assert!(!blocked.contains("thread::unix::capture"), "{blocked}");

            // the calling thread answers its own signal
            let (header, caller) = section(&dump, "name=dump-caller");
            assert!(!header.contains(", action="), "{header}");
            assert!(caller.contains("thread::unix::dump"), "{caller}");
        }
    }
}
