use std::env::current_exe;
use std::path::Path;
use std::path::PathBuf;

use crate::exception;
use crate::exception::Exception;

pub fn asset_path(path: &str) -> Result<PathBuf, Exception> {
    let exe_path = current_exe()?;
    let asset_path = find_asset_path(&exe_path, path);
    if asset_path.exists() {
        Ok(asset_path)
    } else {
        Err(exception!(
            message =
                format!("asset not found, asset={}, exe={}", asset_path.to_string_lossy(), exe_path.to_string_lossy())
        ))
    }
}

#[cfg(debug_assertions)]
fn find_asset_path(exe_path: &Path, path: &str) -> PathBuf {
    let asset_path = exe_path.with_file_name(path);
    if asset_path.exists() {
        return asset_path;
    }

    // determine asset path by bin path, the current dir is different with IDE and command in terminal
    if let Some(bin_name) = exe_path.file_name() {
        let bin_name = bin_name.to_string_lossy();
        let postfix = format!("/target/debug/{bin_name}");
        let exe_path = exe_path.to_string_lossy();
        if exe_path.ends_with(&postfix) {
            let workspace_path = &exe_path[..exe_path.len() - postfix.len()];

            let mut dev_asset_path = PathBuf::from(format!("{workspace_path}/app/{bin_name}/{path}"));

            if !dev_asset_path.exists() {
                dev_asset_path = PathBuf::from(format!("{workspace_path}/{bin_name}/{path}"));
            }
            if dev_asset_path.exists() {
                tracing::info!("load assert from source code folder, asset={}", dev_asset_path.to_string_lossy());
                return dev_asset_path;
            }
        }
    }
    asset_path
}

#[cfg(not(debug_assertions))]
fn find_asset_path(exe_path: &Path, path: &str) -> PathBuf {
    exe_path.with_file_name(path)
}
