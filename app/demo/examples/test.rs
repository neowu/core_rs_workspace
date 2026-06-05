use std::net::ToSocketAddrs;

#[tokio::main]
async fn main() {
    // let path = PathBuf::from("../../conf.md");
    let x = format!("{}:0", hostname()).to_socket_addrs().unwrap();
    for addr in x {
        println!("IP Address: {}", addr.ip());
    }
}

use std::ffi::CStr;

fn hostname() -> &'static str {
    let mut buf = [0u8; 256];
    let ret = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
    if ret != 0 {
        panic!("failed to get hostname, error={}", std::io::Error::last_os_error());
    }
    // ensure NUL-terminated (POSIX doesn't guarantee it on truncation)
    buf[buf.len() - 1] = 0;
    let hostname = unsafe { CStr::from_ptr(buf.as_ptr() as *const libc::c_char) };
    hostname.to_string_lossy().into_owned().leak()
}
