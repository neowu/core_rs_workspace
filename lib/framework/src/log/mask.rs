use memchr::memchr2;

// matched as a json field `"name":"value"` or a header / cookie `name="value"` (Debug quoted),
// header names are always lowercase, add names here when needed
const MASKED_KEYS: &[&str] = &["authorization", "password"];

/// Masks the quoted values of sensitive keys, the trace is expected to carry well formed json and
/// `name={:?}` values.
pub(crate) fn mask_logs(logs: &mut String) {
    let mut masked = String::new();
    let mut kept = logs.as_str(); // not yet copied into masked
    let mut rest = kept;
    while let Some(index) = memchr2(b':', b'=', rest.as_bytes()) {
        let (before, after) = rest.split_at(index);
        let (json, after) = match after.strip_prefix(':') {
            Some(after) => (true, after),
            None => (false, after.strip_prefix('=').unwrap_or(after)),
        };
        if is_masked_key(before, json)
            && let Some(value) = after.trim_ascii_start().strip_prefix('"')
        {
            let (prefix, value) = kept.split_at(kept.len() - value.len());
            let (_, remaining) = value.split_at(value_len(value));
            if masked.capacity() == 0 {
                masked.reserve(logs.len());
            }
            masked.push_str(prefix);
            masked.push_str("**masked**");
            kept = remaining;
            rest = remaining;
        } else {
            rest = after;
        }
    }
    if masked.capacity() > 0 {
        masked.push_str(kept);
        *logs = masked;
    }
}

// json: `"name"` with optional whitespace before the colon, otherwise a whole word `name`
fn is_masked_key(before: &str, json: bool) -> bool {
    if json {
        let Some(key) = before.trim_ascii_end().strip_suffix('"') else { return false };
        MASKED_KEYS.iter().any(|name| key.strip_suffix(name).is_some_and(|key| key.ends_with('"')))
    } else {
        MASKED_KEYS.iter().any(|name| before.strip_suffix(name).is_some_and(|key| key.ends_with(' ')))
    }
}

// up to the closing quote, or the line end when the value was truncated
fn value_len(value: &str) -> usize {
    let mut escaped = false;
    for (index, byte) in value.bytes().enumerate() {
        match byte {
            _ if escaped => escaped = false,
            b'\\' => escaped = true,
            b'"' | b'\n' => return index,
            _ => {}
        }
    }
    value.len()
}

#[cfg(test)]
mod tests {
    use http::HeaderValue;

    use super::mask_logs;

    fn mask(logs: &str) -> String {
        let mut logs = logs.to_owned();
        mask_logs(&mut logs);
        logs
    }

    #[test]
    fn mask_json_fields() {
        let logs = concat!(
            "00:00.000000002 web/body.rs:71 [request] body={\"name\":\"a\",\"password\":\"secret\"}\n",
            "00:00.000000003 [response] body={\"items\":[{\"password\" : \"p1\"},{\"password\":\"p2\"}]}\n",
        );
        assert_eq!(
            mask(logs),
            concat!(
                "00:00.000000002 web/body.rs:71 [request] body={\"name\":\"a\",\"password\":\"**masked**\"}\n",
                "00:00.000000003 [response] body={\"items\":[{\"password\" : \"**masked**\"},{\"password\":\"**masked**\"}]}\n",
            )
        );
    }

    #[test]
    fn mask_headers_and_cookies() {
        // the formats web/server.rs and http.rs log with
        let value = HeaderValue::from_static("Bearer \"x\"");
        let logs = format!(
            "00:00.000000001 [header] authorization={value:?}\n00:00.000000002 [cookie] password={:?}\n",
            "a\"b"
        );
        assert_eq!(
            mask(&logs),
            "00:00.000000001 [header] authorization=\"**masked**\"\n00:00.000000002 [cookie] password=\"**masked**\"\n"
        );
    }

    #[test]
    fn mask_escaped_and_multiline_value() {
        let json = "{\n  \"password\": \"se\\\"c:r=\\\\\",\n  \"url\": \"http://a?b=c\"\n}";
        assert_eq!(mask(json), "{\n  \"password\": \"**masked**\",\n  \"url\": \"http://a?b=c\"\n}");
    }

    #[test]
    fn mask_truncated_value() {
        let logs = "body={\"password\":\"sec...(truncated)\n00:00.000000001 next\n";
        assert_eq!(mask(logs), "body={\"password\":\"**masked**\n00:00.000000001 next\n");
    }

    #[test]
    fn keep_non_matching_keys() {
        for logs in [
            r#"body={"type":"password"}"#,
            r#"body={"note":"say \"password\": x"}"#,
            r#"body={"old_password":"x"}"#,
            r#"body={"password":1}"#,
            r#"body={"password":null}"#,
            r#"[header] proxy-authorization="x""#,
            "[header] authorization=non_quoted_string",
            r#"[header] x=authorization="x""#,
            r#"[context] note="password=\"x\"""#,
            "00:00.000000001 password=x\n",
            "00:00.000000001 [header] authorization=Sensitive\n",
        ] {
            let mut masked = logs.to_owned();
            mask_logs(&mut masked);
            assert_eq!(masked, logs);
            assert_eq!(masked.capacity(), logs.len(), "nothing masked, nothing rebuilt");
        }
    }
}
