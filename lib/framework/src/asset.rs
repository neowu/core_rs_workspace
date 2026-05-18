use std::env::current_exe;
use std::path::PathBuf;

use tracing::info;

use crate::exception;
use crate::exception::Exception;

#[macro_export]
macro_rules! asset_path {
    ($path:expr) => {
        $crate::asset::__resolve(env!("CARGO_MANIFEST_DIR"), $path)
    };
}

#[doc(hidden)]
pub fn __resolve(manifest_dir: &str, path: &str) -> Result<PathBuf, Exception> {
    let exe_path = current_exe()?;
    let asset_path = exe_path.with_file_name(path);
    if asset_path.exists() {
        info!("load asset from exe path, asset={}", asset_path.to_string_lossy());
        return Ok(asset_path);
    }

    #[cfg(debug_assertions)]
    {
        let dev_asset_path = PathBuf::from(manifest_dir).join(path);
        if dev_asset_path.exists() {
            info!("load asset from source code folder, asset={}", dev_asset_path.to_string_lossy());
            return Ok(dev_asset_path);
        }
    }

    Err(exception!(format!(
        "asset not found, asset={}, exe={}",
        asset_path.to_string_lossy(),
        exe_path.to_string_lossy()
    )))
}
