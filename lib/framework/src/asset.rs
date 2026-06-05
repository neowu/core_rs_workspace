use std::env::current_exe;
use std::path::PathBuf;

use crate::console;

// only used on startup, panic if path not found,
// all asset path must be determined and resolve during startup
#[macro_export]
macro_rules! asset_path {
    ($path:expr) => {
        $crate::asset::__resolve(env!("CARGO_MANIFEST_DIR"), $path)
    };
}

#[doc(hidden)]
pub fn __resolve(manifest_dir: &str, path: &str) -> PathBuf {
    let exe_path = current_exe().expect("cannot get current exe path");
    let asset_path = exe_path.with_file_name(path);
    if asset_path.exists() {
        console!("load asset from exe path, asset={}", asset_path.to_string_lossy());
        return asset_path;
    }

    #[cfg(debug_assertions)]
    {
        let dev_asset_path = PathBuf::from(manifest_dir).join(path);
        if dev_asset_path.exists() {
            console!("load asset from source code folder, asset={}", dev_asset_path.to_string_lossy());
            return dev_asset_path;
        }
    }

    panic!("asset not found, asset={}, exe={}", asset_path.to_string_lossy(), exe_path.to_string_lossy());
}
