use std::fs;
use std::io;
use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use http::HeaderValue;
use http::header;
use percent_encoding::percent_decode_str;
use tokio::task::spawn_blocking;

use crate::exception::Exception;
use crate::exception::error_code;
use crate::log::Severity;
use crate::web::request::Request;
use crate::web::response::Response;

pub(crate) async fn serve_dir(prefix: &'static str, root: Arc<Path>, request: Request) -> Result<Response, Exception> {
    let relative = request.path().strip_prefix(prefix).unwrap_or_default();
    let Some(path) = resolve(&root, relative) else {
        return Err(not_found(request.path()));
    };
    serve(path, &request).await
}

pub(crate) async fn serve_file(file: Arc<Path>, request: Request) -> Result<Response, Exception> {
    serve(file.to_path_buf(), &request).await
}

async fn serve(path: PathBuf, request: &Request) -> Result<Response, Exception> {
    let content_type = content_type(&path);
    // open and stat in one blocking task, tokio::fs would hop to the blocking pool per call
    let (file, length) = match spawn_blocking(move || open(&path)).await? {
        Ok(result) => result,
        Err(err)
            if matches!(
                err.kind(),
                ErrorKind::NotFound | ErrorKind::NotADirectory | ErrorKind::IsADirectory | ErrorKind::InvalidInput
            ) =>
        {
            return Err(not_found(request.path()));
        }
        Err(err) => return Err(err.into()),
    };
    Ok(Response::file(file, length).header(header::CONTENT_TYPE, HeaderValue::from_static(content_type)))
}

// regular files only, directories / devices / fifos are not found
fn open(path: &Path) -> io::Result<(fs::File, u64)> {
    let file = fs::File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(ErrorKind::NotFound.into());
    }
    Ok((file, metadata.len()))
}

// rejects any empty or dot segment (".", "..", hidden files), absolute path, backslash or nul after decoding,
// instead of normalizing
fn resolve(root: &Path, relative: &str) -> Option<PathBuf> {
    let decoded = percent_decode_str(relative).decode_utf8().ok()?;
    let mut path = root.to_path_buf();
    if !decoded.is_empty() {
        let dir = decoded.strip_suffix('/').unwrap_or(&decoded);
        for segment in dir.split('/') {
            if segment.is_empty() || segment.starts_with('.') || segment.contains(['\\', '\0']) {
                return None;
            }
            path.push(segment);
        }
    }
    if decoded.is_empty() || decoded.ends_with('/') {
        path.push("index.html");
    }
    Some(path)
}

fn not_found(path: &str) -> Exception {
    exception!(format!("file not found, path={path}"), severity = Severity::Warn, code = error_code::NOT_FOUND)
}

const CONTENT_TYPES: &[(&str, &str)] = &[
    ("html", "text/html; charset=utf-8"),
    ("htm", "text/html; charset=utf-8"),
    ("css", "text/css; charset=utf-8"),
    ("js", "text/javascript; charset=utf-8"),
    ("mjs", "text/javascript; charset=utf-8"),
    ("json", "application/json"),
    ("map", "application/json"),
    ("txt", "text/plain; charset=utf-8"),
    ("csv", "text/csv; charset=utf-8"),
    ("xml", "application/xml"),
    ("svg", "image/svg+xml"),
    ("png", "image/png"),
    ("jpg", "image/jpeg"),
    ("jpeg", "image/jpeg"),
    ("gif", "image/gif"),
    ("webp", "image/webp"),
    ("avif", "image/avif"),
    ("ico", "image/x-icon"),
    ("woff", "font/woff"),
    ("woff2", "font/woff2"),
    ("ttf", "font/ttf"),
    ("otf", "font/otf"),
    ("wasm", "application/wasm"),
    ("pdf", "application/pdf"),
    ("zip", "application/zip"),
    ("gz", "application/gzip"),
    ("mp4", "video/mp4"),
    ("webm", "video/webm"),
    ("mp3", "audio/mpeg"),
];

fn content_type(path: &Path) -> &'static str {
    path.extension()
        .and_then(|extension| extension.to_str())
        .and_then(|extension| CONTENT_TYPES.iter().find(|(ext, _)| ext.eq_ignore_ascii_case(extension)))
        .map_or("application/octet-stream", |(_, content_type)| content_type)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_path() {
        let root = Path::new("/www");
        assert_eq!(resolve(root, "a/b.js"), Some(PathBuf::from("/www/a/b.js")));
        assert_eq!(resolve(root, "a%20b.js"), Some(PathBuf::from("/www/a b.js")));
        assert_eq!(resolve(root, ""), Some(PathBuf::from("/www/index.html")));
        assert_eq!(resolve(root, "docs/"), Some(PathBuf::from("/www/docs/index.html")));
        assert_eq!(resolve(root, "../etc/passwd"), None);
        assert_eq!(resolve(root, "a/../../etc/passwd"), None);
        assert_eq!(resolve(root, "%2E%2E/etc/passwd"), None);
        assert_eq!(resolve(root, "%2Fetc/passwd"), None);
        assert_eq!(resolve(root, "%FF"), None);
        for path in [
            "/",
            ".",
            "./a.js",
            ".env",
            ".git/config",
            "a/.hidden/b.js",
            "a/./b.js",
            "a/.",
            "a//b.js",
            "a/../b.js",
            "a\\b.js",
            "a%00.js",
            "%2E%2Fa.js",
        ] {
            assert_eq!(resolve(root, path), None, "path={path}");
        }
    }

    #[test]
    fn content_types() {
        assert_eq!(content_type(Path::new("a/index.HTML")), "text/html; charset=utf-8");
        assert_eq!(content_type(Path::new("app.js")), "text/javascript; charset=utf-8");
        assert_eq!(content_type(Path::new("README")), "application/octet-stream");
    }
}
