//! The deployment's [`HttpPost`] seam (harness-anthropic's one required
//! operation): a minimal HTTP/1.1 client over tokio, TLS from rustls with
//! webpki trust anchors.
//!
//! One connection per request with `connection: close` — the simplest shape
//! that is fully correct for a low-rate model client, and it lets the
//! response be read to EOF and parsed in one pass. Retries, backoff, and the
//! error taxonomy live above this seam in `harness-anthropic`; everything
//! here maps onto `HttpError` and is retried as transport pressure.
//!
//! A plain `http://` base skips TLS, so tests (and a scripted fake API)
//! can point the client at a local listener.

use std::sync::Arc;
use std::time::Duration;

use actor_core::BoxFuture;
use harness_anthropic::HttpError;
use harness_anthropic::HttpPost;
use harness_anthropic::HttpResponse;
use rustls::pki_types::ServerName;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

/// Bound on one whole request, connect to last byte. Generous: a large
/// completion can take minutes; the model-side retry policy sits above.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

/// An HTTP/1.1 POST client for one base URL.
pub struct HttpsPost {
    host: String,
    port: u16,
    tls: Option<(TlsConnector, ServerName<'static>)>,
}

impl HttpsPost {
    /// Build a client for `base`: `https://host[:port]` or, for local fakes,
    /// `http://host[:port]`. No path suffix.
    pub fn new(base: &str) -> Result<HttpsPost, String> {
        let (tls, rest) = if let Some(rest) = base.strip_prefix("https://") {
            (true, rest)
        } else if let Some(rest) = base.strip_prefix("http://") {
            (false, rest)
        } else {
            return Err(format!("unsupported url (expected http(s)://host): {base}"));
        };
        let rest = rest.trim_end_matches('/');
        if rest.is_empty() || rest.contains('/') {
            return Err(format!("expected a bare host, got: {base}"));
        }
        let (host, port) = match rest.rsplit_once(':') {
            Some((host, port)) => (
                host.to_string(),
                port.parse().map_err(|e| format!("bad port: {e}"))?,
            ),
            None => (rest.to_string(), if tls { 443 } else { 80 }),
        };
        let tls = tls
            .then(|| {
                let roots = rustls::RootCertStore {
                    roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
                };
                let config = rustls::ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth();
                let name = ServerName::try_from(host.clone())
                    .map_err(|e| format!("bad server name: {e}"))?;
                Ok::<_, String>((TlsConnector::from(Arc::new(config)), name))
            })
            .transpose()?;
        Ok(HttpsPost { host, port, tls })
    }
}

impl HttpPost for HttpsPost {
    fn post(
        &self,
        path: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
    ) -> BoxFuture<'static, Result<HttpResponse, HttpError>> {
        let mut request = format!(
            "POST {path} HTTP/1.1\r\nhost: {}\r\nconnection: close\r\ncontent-length: {}\r\n",
            self.host,
            body.len()
        )
        .into_bytes();
        for (name, value) in headers {
            request.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
        }
        request.extend_from_slice(b"\r\n");
        request.extend_from_slice(&body);
        let host = self.host.clone();
        let port = self.port;
        let tls = self.tls.clone();
        Box::pin(async move {
            tokio::time::timeout(REQUEST_TIMEOUT, exchange(host, port, tls, request))
                .await
                .map_err(|_| HttpError("request timed out".to_string()))?
        })
    }
}

/// Connect, send the request, read the response to EOF, parse.
async fn exchange(
    host: String,
    port: u16,
    tls: Option<(TlsConnector, ServerName<'static>)>,
    request: Vec<u8>,
) -> Result<HttpResponse, HttpError> {
    let tcp = TcpStream::connect((host.as_str(), port))
        .await
        .map_err(|e| HttpError(format!("connect {host}:{port}: {e}")))?;
    let raw = match tls {
        Some((connector, name)) => {
            let stream = connector
                .connect(name, tcp)
                .await
                .map_err(|e| HttpError(format!("tls: {e}")))?;
            send_and_drain(stream, &request).await?
        }
        None => send_and_drain(tcp, &request).await?,
    };
    parse_response(&raw)
}

async fn send_and_drain<S>(mut stream: S, request: &[u8]) -> Result<Vec<u8>, HttpError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream
        .write_all(request)
        .await
        .map_err(|e| HttpError(format!("send: {e}")))?;
    let mut raw = Vec::new();
    match stream.read_to_end(&mut raw).await {
        Ok(_) => {}
        // A peer that closes without a TLS close_notify still delivered the
        // bytes; an actually-truncated body fails in `parse_response`.
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof && !raw.is_empty() => {}
        Err(e) => return Err(HttpError(format!("receive: {e}"))),
    }
    Ok(raw)
}

/// Parse a complete HTTP/1.1 response: status line, headers, then the body
/// by content-length, chunked encoding, or read-to-close.
fn parse_response(raw: &[u8]) -> Result<HttpResponse, HttpError> {
    let header_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| HttpError("malformed response: no header terminator".to_string()))?;
    let head = std::str::from_utf8(&raw[..header_end])
        .map_err(|e| HttpError(format!("malformed response head: {e}")))?;
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .ok_or_else(|| HttpError(format!("malformed status line: {status_line}")))?;
    let mut content_length: Option<usize> = None;
    let mut chunked = false;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            content_length = Some(
                value
                    .parse()
                    .map_err(|e| HttpError(format!("bad content-length: {e}")))?,
            );
        } else if name.eq_ignore_ascii_case("transfer-encoding")
            && value.to_ascii_lowercase().contains("chunked")
        {
            chunked = true;
        }
    }
    let rest = &raw[header_end + 4..];
    let body = if chunked {
        dechunk(rest)?
    } else if let Some(length) = content_length {
        if rest.len() < length {
            return Err(HttpError(format!(
                "truncated body: {} of {length} bytes",
                rest.len()
            )));
        }
        rest[..length].to_vec()
    } else {
        rest.to_vec()
    };
    Ok(HttpResponse { status, body })
}

/// Decode a chunked body: `<hex-size>[;ext]\r\n<bytes>\r\n` …, `0`-chunk ends.
fn dechunk(mut rest: &[u8]) -> Result<Vec<u8>, HttpError> {
    let mut body = Vec::new();
    loop {
        let line_end = rest
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or_else(|| HttpError("truncated chunk header".to_string()))?;
        let size_str = std::str::from_utf8(&rest[..line_end])
            .map_err(|e| HttpError(format!("bad chunk header: {e}")))?;
        let size_str = size_str.split(';').next().unwrap_or_default().trim();
        let size = usize::from_str_radix(size_str, 16)
            .map_err(|e| HttpError(format!("bad chunk size {size_str:?}: {e}")))?;
        rest = &rest[line_end + 2..];
        if size == 0 {
            return Ok(body);
        }
        if rest.len() < size + 2 {
            return Err(HttpError("truncated chunk".to_string()));
        }
        body.extend_from_slice(&rest[..size]);
        rest = &rest[size + 2..];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_content_length_and_chunked_bodies() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 5\r\n\r\nhellotrailing-junk";
        let response = parse_response(raw).expect("parses");
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"hello");

        let raw = b"HTTP/1.1 429 Too Many\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nwait\r\n3;ext=1\r\n...\r\n0\r\n\r\n";
        let response = parse_response(raw).expect("parses");
        assert_eq!(response.status, 429);
        assert_eq!(response.body, b"wait...");

        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort";
        assert!(parse_response(raw).is_err(), "truncated body is an error");
    }

    /// A local fake serving one canned response per connection, over plain
    /// `http://` — the same path a scripted Messages API fake would use.
    #[tokio::test]
    async fn posts_to_a_local_server_and_reads_the_reply() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let served = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut request = vec![0u8; 4096];
            let n = socket.read(&mut request).await.expect("read");
            socket
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                .await
                .expect("write");
            String::from_utf8_lossy(&request[..n]).into_owned()
        });
        let client = HttpsPost::new(&format!("http://127.0.0.1:{}", addr.port())).expect("client");
        let response = client
            .post(
                "/v1/messages",
                &[("x-api-key".to_string(), "sk-test".to_string())],
                b"{\"hi\":1}".to_vec(),
            )
            .await
            .expect("post");
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"ok");
        let request = served.await.expect("server");
        assert!(request.starts_with("POST /v1/messages HTTP/1.1\r\n"));
        assert!(request.contains("x-api-key: sk-test"));
        assert!(request.contains("content-length: 8"));
        assert!(request.ends_with("{\"hi\":1}"));
    }
}
