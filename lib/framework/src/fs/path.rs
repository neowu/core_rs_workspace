use std::path::Path;

use crate::exception::Exception;

pub trait PathExt {
    fn file_extension(&self) -> Result<&str, Exception>;
}

impl PathExt for Path {
    fn file_extension(&self) -> Result<&str, Exception> {
        self.extension()
            .ok_or_else(|| exception!(format!("file must have extension, path={}", self.display())))?
            .to_str()
            .ok_or_else(|| exception!(format!("path is invalid, path={}", self.display())))
    }
}
