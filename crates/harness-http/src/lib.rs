//! The hand-rolled HTTP/1.1 client substrate shared by the tree's deployment
//! binaries — the TUI's gateway client and the standalone node's model-seam
//! socket. Deliberately minimal (no `reqwest`/`hyper`): one connection per
//! request with `connection: close`, TLS from rustls with webpki trust
//! anchors, and an incremental [`Reader`] that serves both shapes of call —
//! read the whole reply (unary), or pull a live chunked stream (SSE).
//!
//! This is plumbing, not a seam: the model adapters' seam is
//! `harness::http::HttpPost`, and one implementation of it (the standalone's)
//! is built on this crate. Protocol decisions — retry taxonomies, SSE frame
//! vocabulary, auth headers — belong to the callers.

use std::sync::Arc;

use rustls::pki_types::ServerName;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

/// Any byte stream the client talks HTTP over — a bare `TcpStream` or a
/// rustls `TlsStream` on top of one.
pub trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// One `http://` / `https://` base URL, parsed once: the host and port to
/// dial, and the TLS client state when the scheme asks for it.
#[derive(Clone)]
pub struct Origin {
    host: String,
    port: u16,
    tls: Option<(TlsConnector, ServerName<'static>)>,
}

impl Origin {
    /// Parse `base`: `https://host[:port]` or, for local fakes and loopback
    /// demos, `http://host[:port]`. No path suffix.
    pub fn parse(base: &str) -> Result<Origin, String> {
        let (tls, rest) = if let Some(rest) = base.strip_prefix("https://") {
            (true, rest)
        } else if let Some(rest) = base.strip_prefix("http://") {
            (false, rest)
        } else {
            return Err(format!("unsupported url (expected http(s)://host): {base}"));
        };
        let rest = rest.trim_end_matches('/');
        if rest.is_empty() || rest.contains('/') {
            return Err(format!("expected a bare host[:port], got: {base}"));
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
        Ok(Origin { host, port, tls })
    }

    /// The host, as the `host:` request header must carry it.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Open a transport connection (TLS when the scheme asked for it).
    pub async fn connect(&self) -> Result<Box<dyn Io>, String> {
        let tcp = TcpStream::connect((self.host.as_str(), self.port))
            .await
            .map_err(|e| format!("connect {}:{}: {e}", self.host, self.port))?;
        match &self.tls {
            Some((connector, name)) => {
                let stream = connector
                    .connect(name.clone(), tcp)
                    .await
                    .map_err(|e| format!("tls: {e}"))?;
                Ok(Box::new(stream))
            }
            None => Ok(Box::new(tcp)),
        }
    }

    /// Frame an HTTP/1.1 request against this origin. `connection: close`
    /// keeps a unary reply a single read-to-EOF; a streaming server sends the
    /// body chunked and closes when done. Auth and content headers are the
    /// caller's, passed in `headers`.
    pub fn request(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Vec<u8> {
        let mut request = format!(
            "{method} {path} HTTP/1.1\r\nhost: {}\r\nconnection: close\r\ncontent-length: {}\r\n",
            self.host,
            body.len()
        )
        .into_bytes();
        for (name, value) in headers {
            request.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
        }
        request.extend_from_slice(b"\r\n");
        request.extend_from_slice(body);
        request
    }
}

/// A buffered reader over one connection: HTTP header parsing, then either a
/// whole-body read (unary) or a chunked stream (SSE).
pub struct Reader {
    stream: Box<dyn Io>,
    /// Bytes read off the socket but not yet consumed.
    buf: Vec<u8>,
    chunked: bool,
    content_length: Option<usize>,
}

impl Reader {
    pub fn new(stream: Box<dyn Io>) -> Reader {
        Reader {
            stream,
            buf: Vec::new(),
            chunked: false,
            content_length: None,
        }
    }

    /// Send the framed request.
    pub async fn write_all(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.stream
            .write_all(bytes)
            .await
            .map_err(|e| format!("send: {e}"))
    }

    /// Read and parse the status line and headers, leaving the body bytes (any
    /// already buffered) in `buf`. Returns the status code and records whether
    /// the body is chunked / its content-length.
    pub async fn read_headers(&mut self) -> Result<u16, String> {
        let head = loop {
            if let Some(end) = find(&self.buf, b"\r\n\r\n") {
                let head: Vec<u8> = self.buf.drain(..end + 4).collect();
                break head;
            }
            if !self.fill().await? {
                return Err("connection closed before headers completed".to_string());
            }
        };
        let head = String::from_utf8_lossy(&head);
        let mut lines = head.split("\r\n");
        let status_line = lines.next().unwrap_or_default();
        let status: u16 = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.parse().ok())
            .ok_or_else(|| format!("malformed status line: {status_line}"))?;
        for line in lines {
            let Some((name, value)) = line.split_once(':') else {
                continue;
            };
            let value = value.trim();
            if name.eq_ignore_ascii_case("transfer-encoding")
                && value.to_ascii_lowercase().contains("chunked")
            {
                self.chunked = true;
            } else if name.eq_ignore_ascii_case("content-length") {
                self.content_length = value.parse().ok();
            }
        }
        Ok(status)
    }

    /// Read the whole response body (chunked, content-length, or to-EOF).
    pub async fn read_body(&mut self) -> Result<Vec<u8>, String> {
        if self.chunked {
            let mut body = Vec::new();
            while let Some(chunk) = self.next_chunk().await? {
                body.extend_from_slice(&chunk);
            }
            Ok(body)
        } else if let Some(len) = self.content_length {
            while self.buf.len() < len {
                if !self.fill().await? {
                    return Err(format!("truncated body: {} of {len} bytes", self.buf.len()));
                }
            }
            Ok(self.buf.drain(..len).collect())
        } else {
            self.drain_to_end().await
        }
    }

    /// Read whatever remains until the peer closes (best-effort; for error
    /// bodies and content-length-less replies).
    pub async fn drain_to_end(&mut self) -> Result<Vec<u8>, String> {
        while self.fill().await? {}
        Ok(std::mem::take(&mut self.buf))
    }

    /// The next decoded chunk of a chunked body, or `None` at the terminating
    /// zero-length chunk.
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, String> {
        let header = match self.read_line().await? {
            Some(line) => line,
            None => return Ok(None),
        };
        let size_str = header.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_str, 16)
            .map_err(|e| format!("bad chunk size {size_str:?}: {e}"))?;
        if size == 0 {
            return Ok(None);
        }
        // The chunk's `size` bytes, then the trailing CRLF.
        while self.buf.len() < size + 2 {
            if !self.fill().await? {
                return Err("truncated chunk".to_string());
            }
        }
        let data: Vec<u8> = self.buf.drain(..size).collect();
        self.buf.drain(..2); // CRLF
        Ok(Some(data))
    }

    /// Read up to and consuming the next CRLF, returning the line without it.
    async fn read_line(&mut self) -> Result<Option<String>, String> {
        loop {
            if let Some(end) = find(&self.buf, b"\r\n") {
                let line: Vec<u8> = self.buf.drain(..end + 2).collect();
                return Ok(Some(String::from_utf8_lossy(&line[..end]).into_owned()));
            }
            if !self.fill().await? {
                return Ok(None);
            }
        }
    }

    /// Pull more bytes from the socket into `buf`; `false` at EOF.
    async fn fill(&mut self) -> Result<bool, String> {
        let mut tmp = [0u8; 8192];
        let n = match self.stream.read(&mut tmp).await {
            Ok(n) => n,
            // A peer that closes without a TLS close_notify still delivered
            // its bytes; a body actually cut short fails at the framing layer
            // (content-length / chunk accounting), not here.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => 0,
            Err(e) => return Err(format!("receive: {e}")),
        };
        if n == 0 {
            return Ok(false);
        }
        self.buf.extend_from_slice(&tmp[..n]);
        Ok(true)
    }
}

/// The first index of `needle` in `haystack`, or `None`.
fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn read_response(raw: &[u8]) -> Result<(u16, Vec<u8>), String> {
        let (client, mut server) = tokio::io::duplex(64 * 1024);
        server.write_all(raw).await.expect("write");
        drop(server); // close, so read-to-EOF terminates
        let mut reader = Reader::new(Box::new(client));
        let status = reader.read_headers().await?;
        let body = reader.read_body().await?;
        Ok((status, body))
    }

    #[tokio::test]
    async fn parses_content_length_and_chunked_bodies() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 5\r\n\r\nhellotrailing-junk";
        let (status, body) = read_response(raw).await.expect("parses");
        assert_eq!(status, 200);
        assert_eq!(body, b"hello");

        let raw = b"HTTP/1.1 429 Too Many\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nwait\r\n3;ext=1\r\n...\r\n0\r\n\r\n";
        let (status, body) = read_response(raw).await.expect("parses");
        assert_eq!(status, 429);
        assert_eq!(body, b"wait...");

        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort";
        assert!(
            read_response(raw).await.is_err(),
            "truncated body is an error"
        );
    }

    #[test]
    fn origins_parse_and_reject() {
        assert!(Origin::parse("https://api.example.com").is_ok());
        assert!(Origin::parse("http://127.0.0.1:8080").is_ok());
        assert!(Origin::parse("ftp://x").is_err());
        assert!(Origin::parse("https://host/path").is_err());
        let origin = Origin::parse("http://gw:8080/").expect("parses");
        assert_eq!(origin.host(), "gw");
        let framed = origin.request("GET", "/v1/sessions", &[("accept", "json")], b"");
        let text = String::from_utf8(framed).expect("utf8");
        assert!(text.starts_with("GET /v1/sessions HTTP/1.1\r\nhost: gw\r\n"));
        assert!(text.contains("accept: json\r\n"));
    }
}
