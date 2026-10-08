use std::fs;
use std::io;
use std::io::ErrorKind;
use std::io::Read as _;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use framework::exception;
use framework::exception::Exception;
use framework::exception::error_code;
use framework::log::Severity;
use http::HeaderValue;
use http::StatusCode;
use http::header;
use percent_encoding::percent_decode_str;
use tokio::task::spawn_blocking;

use crate::request::Request;
use crate::response::Response;

// read in one blocking call, larger files are streamed in chunks
const MAX_INLINE_SIZE: u64 = 1024 * 1024;

enum Content {
    NotModified,
    Full(Bytes),
    Stream(fs::File, u64),
}

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
    let if_modified_since =
        request.header(header::IF_MODIFIED_SINCE).and_then(|value| httpdate::parse_http_date(value).ok());
    let content_type = content_type(&path);

    // open, stat and read in one blocking task, tokio::fs would hop to the blocking pool per call
    let (content, modified) = match spawn_blocking(move || read(&path, if_modified_since)).await? {
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

    let mut response = match content {
        Content::NotModified => Response::empty().status(StatusCode::NOT_MODIFIED),
        Content::Full(bytes) => Response::bytes(bytes, HeaderValue::from_static(content_type)),
        Content::Stream(file, length) => {
            Response::file(file, length).header(header::CONTENT_TYPE, HeaderValue::from_static(content_type))
        }
    };
    if let Some(modified) = modified
        && let Ok(value) = HeaderValue::try_from(httpdate::fmt_http_date(modified))
    {
        response.headers_mut().insert(header::LAST_MODIFIED, value);
    }
    Ok(response)
}

fn read(path: &Path, if_modified_since: Option<SystemTime>) -> io::Result<(Content, Option<SystemTime>)> {
    let file = fs::File::open(path)?;
    let metadata = file.metadata()?;
    if metadata.is_dir() {
        return Err(ErrorKind::IsADirectory.into());
    }
    // http date has second precision
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| UNIX_EPOCH + Duration::from_secs(duration.as_secs()));
    if let Some(modified) = modified
        && let Some(since) = if_modified_since
        && modified <= since
    {
        return Ok((Content::NotModified, Some(modified)));
    }

    let length = metadata.len();
    if length > MAX_INLINE_SIZE {
        return Ok((Content::Stream(file, length), modified));
    }
    let mut buffer = Vec::with_capacity(length as usize);
    file.take(MAX_INLINE_SIZE).read_to_end(&mut buffer)?;
    Ok((Content::Full(Bytes::from(buffer)), modified))
}

// only normal components are allowed, rejects "..", "." and absolute path
fn resolve(root: &Path, relative: &str) -> Option<PathBuf> {
    let decoded = percent_decode_str(relative).decode_utf8().ok()?;
    let mut path = root.to_path_buf();
    for component in Path::new(decoded.as_ref()).components() {
        match component {
            Component::Normal(name) => path.push(name),
            Component::Prefix(_) | Component::RootDir | Component::CurDir | Component::ParentDir => return None,
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
    }

    #[test]
    fn content_types() {
        assert_eq!(content_type(Path::new("a/index.HTML")), "text/html; charset=utf-8");
        assert_eq!(content_type(Path::new("app.js")), "text/javascript; charset=utf-8");
        assert_eq!(content_type(Path::new("README")), "application/octet-stream");
    }
}
