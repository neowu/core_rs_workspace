use std::ffi::CStr;
use std::io::Error;
use std::sync::OnceLock;

pub fn hostname() -> &'static str {
    static HOSTNAME: OnceLock<&'static str> = OnceLock::new();
    HOSTNAME.get_or_init(|| __hostname().leak())
}

fn __hostname() -> String {
    let mut buf = [0_u8; 256];
    let ret = unsafe { libc::gethostname(buf.as_mut_ptr().cast::<libc::c_char>(), buf.len()) };
    assert!(ret == 0, "failed to get hostname, error={}", Error::last_os_error());
    // ensure NUL-terminated (POSIX doesn't guarantee it on truncation)
    if let Some(last) = buf.last_mut() {
        *last = 0;
    }
    let hostname = unsafe { CStr::from_ptr(buf.as_ptr().cast::<libc::c_char>()) };
    hostname.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use crate::network::hostname;

    #[test]
    fn hostname_normal() {
        assert!(!hostname().is_empty());
    }
}
