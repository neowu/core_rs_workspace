use std::env::current_exe;
use std::path::Path;
use std::path::PathBuf;

use crate::exception;
use crate::exception::CoreRsResult;

pub fn asset_path(path: &str) -> CoreRsResult<PathBuf> {
    let exe_path = current_exe()?;
    let asset_path = find_asset_path(&exe_path, path);
    if asset_path.exists() {
        Ok(asset_path)
    } else {
        Err(exception!(
            message = format!(
                "asset not found, asset={}, exe={}",
                asset_path.to_string_lossy(),
                exe_path.to_string_lossy()
            )
        ))
    }
}

#[cfg(debug_assertions)]
fn find_asset_path(exe_path: &Path, path: &str) -> PathBuf {
    let asset_path = exe_path.with_file_name(path);
    if asset_path.exists() {
        return asset_path;
    }
    if let Some(bin_name) = exe_path.file_name() {
        let bin_name = bin_name.to_string_lossy();
        if exe_path
            .to_string_lossy()
            .ends_with(format!("/target/debug/{bin_name}").as_str())
        {
            let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_default();
            let asset_path = PathBuf::from(manifest_dir).join(path);
            if asset_path.exists() {
                tracing::info!(
                    "load assert from source code folder, asset={}",
                    asset_path.to_string_lossy()
                );
                return asset_path;
            }
        }
    }
    asset_path
}

#[cfg(not(debug_assertions))]
fn find_asset_path(exe_path: &Path, path: &str) -> PathBuf {
    exe_path.with_file_name(path)
}
