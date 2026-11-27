use std::env;
use std::process::Command;

/// The probe links the compiler's own libraries, so it is tied to the sysroot of the toolchain that builds it.
fn main() {
    let rustc = env::var("RUSTC").expect("cargo sets RUSTC");
    let output = Command::new(rustc).args(["--print", "sysroot"]).output().expect("rustc runs");
    let sysroot = String::from_utf8(output.stdout).expect("sysroot is utf-8");
    let sysroot = sysroot.trim();
    println!("cargo::rustc-env=PROBE_SYSROOT={sysroot}");
    println!("cargo::rustc-link-arg=-Wl,-rpath,{sysroot}/lib");
}
