use std::path::PathBuf;

use crate::exception::CoreRsResult;

pub trait PathBufExt {
    fn into_absolute_path(self) -> CoreRsResult<PathBuf>;
}

impl PathBufExt for PathBuf {
    fn into_absolute_path(self) -> CoreRsResult<PathBuf> {
        if self.is_absolute() {
            return Ok(self);
        }
        let current_dir = std::env::current_dir()
            .map_err(|err| exception!(message = "failed to get current directory", source = err))?;
        Ok(current_dir.join(self))
    }
}
