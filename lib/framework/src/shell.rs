use tokio::process::Command;

use crate::exception::Exception;

pub async fn run(command: &str) -> Result<String, Exception> {
    let _span = span!("shell");
    let output = Command::new("sh").arg("-c").arg(command).output().await?;
    log!("status = {:?}", output.status.code());
    let stdout = String::from_utf8_lossy(&output.stdout);
    log!("stdout = {}", stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    log!("stderr = {}", stderr);
    if output.status.success() {
        Ok(stdout.to_string())
    } else {
        Err(exception!(format!("command failed, status={}", output.status.code().unwrap_or(-1))))
    }
}
