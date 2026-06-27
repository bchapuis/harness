//! The gateway client: a minimal HTTP/1.1 client that speaks the `harness-gateway`
//! REST/SSE surface (`docs/multi-tenant-acp-design.md`).
//!
//! Two shapes of call:
//!
//! - **Request-response** (`list_sessions`, `fetch_records`, `cancel`) — send,
//!   read the whole reply, parse the JSON. The same one-connection-per-request,
//!   `connection: close` style as the standalone deployment's model seam.
//! - **Streaming** (`open_prompt`) — submit a turn with `Accept: text/event-stream`
//!   and hand back an [`Events`] iterator that yields the run's records live off
//!   the chunked SSE body, so the TUI renders as the agent works.
//!
//! TLS (for an `https://` gateway behind no ingress) reuses rustls with webpki
//! trust anchors; a plain `http://` base — the loopback demo — skips it.

use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::ServerName;
use serde::Deserialize;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use harness::Record;
use harness::RunOutcome;
use harness::Seq;

/// One of a tenant's sessions, as `/v1/sessions` lists it: the unprefixed
/// session id and its optional human label.
#[derive(Clone, Debug, Deserialize)]
pub struct SessionEntry {
    pub session: String,
    #[serde(default)]
    pub label: Option<String>,
}

/// A parsed SSE frame from a streamed run: a `records` batch, the terminal
/// `outcome`, an `error`, or the stream's `end` marker.
#[derive(Debug)]
pub enum Event {
    Records(Vec<(Seq, Record)>),
    Outcome(RunOutcome),
    Error(String),
    End,
}

/// Any byte stream the client talks HTTP over — a bare `TcpStream` or a rustls
/// `TlsStream` on top of one.
trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// Bound on a whole request-response exchange. The streaming prompt is *not*
/// bound by this — a run can take minutes — only the unary calls are.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// An HTTP/1.1 client pinned to one gateway base URL, carrying the tenant token
/// every request authenticates with.
#[derive(Clone)]
pub struct GatewayClient {
    host: String,
    port: u16,
    token: String,
    tls: Option<(TlsConnector, ServerName<'static>)>,
}

impl GatewayClient {
    /// Build a client for `base` (`http://host[:port]` or `https://host[:port]`)
    /// acting as the tenant `token` names.
    pub fn new(base: &str, token: impl Into<String>) -> Result<Arc<GatewayClient>, String> {
        let (https, rest) = if let Some(rest) = base.strip_prefix("https://") {
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
            None => (rest.to_string(), if https { 443 } else { 80 }),
        };
        let tls = https
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
        Ok(Arc::new(GatewayClient {
            host,
            port,
            token: token.into(),
            tls,
        }))
    }

    /// List this tenant's sessions of `kind`.
    pub async fn list_sessions(&self, kind: &str) -> Result<Vec<SessionEntry>, String> {
        let path = format!("/v1/sessions?kind={}", encode(kind));
        let body = self.unary("GET", &path, None).await?;
        #[derive(Deserialize)]
        struct Resp {
            sessions: Vec<SessionEntry>,
        }
        let resp: Resp = parse(&body)?;
        Ok(resp.sessions)
    }

    /// Read a page of a session's committed records, from sequence `from`.
    pub async fn fetch_records(
        &self,
        kind: &str,
        session: &str,
        from: u64,
    ) -> Result<Vec<(Seq, Record)>, String> {
        let path = format!(
            "/v1/{}/{}/records?from={from}",
            encode(kind),
            encode(session)
        );
        let body = self.unary("GET", &path, None).await?;
        #[derive(Deserialize)]
        struct Resp {
            records: Vec<(Seq, Record)>,
        }
        let resp: Resp = parse(&body)?;
        Ok(resp.records)
    }

    /// Cancel an in-flight run (idempotent).
    pub async fn cancel(&self, kind: &str, session: &str, turn: &str) -> Result<(), String> {
        let path = format!(
            "/v1/{}/{}/cancel?turn={}",
            encode(kind),
            encode(session),
            encode(turn)
        );
        self.unary("POST", &path, Some(Vec::new())).await?;
        Ok(())
    }

    /// Submit a turn and stream the run as SSE. `from` is where the live watch
    /// starts — pass the session's current head so the stream carries only the
    /// new records, not the whole history again.
    pub async fn open_prompt(
        &self,
        kind: &str,
        session: &str,
        turn: &str,
        content: &str,
        from: u64,
    ) -> Result<Events, String> {
        let path = format!(
            "/v1/{}/{}/prompt?from={from}",
            encode(kind),
            encode(session)
        );
        let body = serde_json::json!({ "turn": turn, "content": content }).to_string();
        let request = self.build_request(
            "POST",
            &path,
            &[
                ("content-type", "application/json"),
                ("accept", "text/event-stream"),
            ],
            Some(body.into_bytes()),
        );
        let stream = self.connect().await?;
        let mut reader = Reader::new(stream);
        reader.write_all(&request).await?;
        let status = reader.read_headers().await?;
        if status != 200 {
            let body = reader.drain_to_end().await.unwrap_or_default();
            return Err(format!(
                "gateway returned {status}: {}",
                String::from_utf8_lossy(&body)
            ));
        }
        Ok(Events {
            reader,
            decoded: Vec::new(),
            done: false,
        })
    }

    /// A request-response call: connect, send, read the whole reply, return the
    /// body bytes on a 2xx — or a descriptive error otherwise.
    async fn unary(
        &self,
        method: &str,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<Vec<u8>, String> {
        let request =
            self.build_request(method, path, &[("content-type", "application/json")], body);
        let exchange = async {
            let stream = self.connect().await?;
            let mut reader = Reader::new(stream);
            reader.write_all(&request).await?;
            let status = reader.read_headers().await?;
            let body = reader.read_body().await?;
            Ok::<_, String>((status, body))
        };
        let (status, body) = tokio::time::timeout(REQUEST_TIMEOUT, exchange)
            .await
            .map_err(|_| format!("request to {path} timed out"))??;
        if (200..300).contains(&status) {
            Ok(body)
        } else {
            Err(format!(
                "gateway returned {status}: {}",
                String::from_utf8_lossy(&body)
            ))
        }
    }

    /// Frame an HTTP/1.1 request. `connection: close` keeps unary replies a
    /// single read-to-EOF; the streaming prompt overrides nothing here — the
    /// gateway streams the body chunked regardless and closes at the run's end.
    fn build_request(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: Option<Vec<u8>>,
    ) -> Vec<u8> {
        let body = body.unwrap_or_default();
        let mut request = format!(
            "{method} {path} HTTP/1.1\r\nhost: {}\r\nconnection: close\r\n\
             authorization: Bearer {}\r\ncontent-length: {}\r\n",
            self.host,
            self.token,
            body.len()
        )
        .into_bytes();
        for (name, value) in headers {
            request.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
        }
        request.extend_from_slice(b"\r\n");
        request.extend_from_slice(&body);
        request
    }

    /// Open a transport connection to the gateway (TLS when configured).
    async fn connect(&self) -> Result<Box<dyn Io>, String> {
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
}

/// The live SSE record stream of a submitted turn. Pull [`Events::next`] until
/// it returns `None`; each yield is one gateway SSE frame.
pub struct Events {
    reader: Reader,
    /// SSE bytes decoded out of the chunked body but not yet split into frames.
    decoded: Vec<u8>,
    done: bool,
}

impl Events {
    /// The next SSE frame, or `None` once the stream closes.
    pub async fn next(&mut self) -> Option<Event> {
        if self.done {
            return None;
        }
        loop {
            // A blank line (`\n\n`) terminates one SSE frame.
            if let Some(end) = find(&self.decoded, b"\n\n") {
                let frame: Vec<u8> = self.decoded.drain(..end + 2).collect();
                if let Some(event) = parse_frame(&frame[..end]) {
                    return Some(event);
                }
                continue; // a keep-alive comment or empty frame; keep reading.
            }
            match self.reader.next_chunk().await {
                Ok(Some(bytes)) => self.decoded.extend_from_slice(&bytes),
                Ok(None) => {
                    self.done = true;
                    return None;
                }
                Err(e) => {
                    self.done = true;
                    return Some(Event::Error(e));
                }
            }
        }
    }
}

/// Decode one SSE frame's `event:`/`data:` fields into an [`Event`]. Lines
/// starting `:` are comments (keep-alives); a frame with no recognised event is
/// `None`.
fn parse_frame(frame: &[u8]) -> Option<Event> {
    let text = std::str::from_utf8(frame).ok()?;
    let mut name = "message";
    let mut data = String::new();
    for line in text.split('\n') {
        if let Some(rest) = line.strip_prefix("event:") {
            name = rest.trim();
        } else if let Some(rest) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(rest.strip_prefix(' ').unwrap_or(rest));
        }
    }
    match name {
        "records" => serde_json::from_str(&data).ok().map(Event::Records),
        "outcome" => serde_json::from_str(&data).ok().map(Event::Outcome),
        "error" => Some(Event::Error(data)),
        "end" => Some(Event::End),
        _ => None,
    }
}

/// A buffered reader over the connection: HTTP header parsing, then either a
/// whole-body read (unary) or a chunked stream (SSE).
struct Reader {
    stream: Box<dyn Io>,
    /// Bytes read off the socket but not yet consumed.
    buf: Vec<u8>,
    chunked: bool,
    content_length: Option<usize>,
}

impl Reader {
    fn new(stream: Box<dyn Io>) -> Reader {
        Reader {
            stream,
            buf: Vec::new(),
            chunked: false,
            content_length: None,
        }
    }

    async fn write_all(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.stream
            .write_all(bytes)
            .await
            .map_err(|e| format!("send: {e}"))
    }

    /// Read and parse the status line and headers, leaving the body bytes (any
    /// already buffered) in `buf`. Returns the status code and records whether
    /// the body is chunked / its content-length.
    async fn read_headers(&mut self) -> Result<u16, String> {
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
    async fn read_body(&mut self) -> Result<Vec<u8>, String> {
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
    async fn drain_to_end(&mut self) -> Result<Vec<u8>, String> {
        while self.fill().await? {}
        Ok(std::mem::take(&mut self.buf))
    }

    /// The next decoded chunk of a chunked body, or `None` at the terminating
    /// zero-length chunk.
    async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, String> {
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
        let n = self
            .stream
            .read(&mut tmp)
            .await
            .map_err(|e| format!("receive: {e}"))?;
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

/// Parse a JSON body, mapping a decode failure to a readable error.
fn parse<T: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<T, String> {
    serde_json::from_slice(body)
        .map_err(|e| format!("decode response: {e}: {}", String::from_utf8_lossy(body)))
}

/// Percent-encode a path/query segment's reserved bytes. The session and kind
/// ids the TUI sends are a narrow charset, but a label or stray character must
/// not break the request line.
fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for byte in s.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~' | b'/') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    use harness::Completion;
    use harness::RecordBody;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    /// Frame `pieces` as an HTTP/1.1 chunked response — one chunk per piece, so a
    /// caller can split an SSE event across a chunk boundary on purpose.
    fn chunked_response(pieces: &[&str]) -> Vec<u8> {
        let mut out =
            b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n"
                .to_vec();
        for piece in pieces {
            out.extend_from_slice(format!("{:x}\r\n", piece.len()).as_bytes());
            out.extend_from_slice(piece.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        out.extend_from_slice(b"0\r\n\r\n");
        out
    }

    /// Serve one canned response on a fresh loopback port and return its base url.
    async fn serve(response: Vec<u8>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            // Drain the request head so the client's write completes before we reply.
            let mut buf = [0u8; 4096];
            let _ = socket.read(&mut buf).await;
            socket.write_all(&response).await.expect("write");
            socket.shutdown().await.ok();
        });
        format!("http://127.0.0.1:{}", addr.port())
    }

    #[tokio::test]
    async fn streams_records_then_outcome_across_a_chunk_boundary() {
        let records = vec![(
            Seq::new(1),
            Record {
                at_nanos: 0,
                body: RecordBody::WorkspaceReset,
            },
        )];
        let records_json = serde_json::to_string(&records).unwrap();
        let outcome: RunOutcome = Ok(Completion::new("the sum is 55", 42));
        let outcome_json = serde_json::to_string(&outcome).unwrap();

        // One SSE body, split into two transport chunks mid-event, so the decoder
        // must accumulate across the boundary before it sees the blank-line frame
        // terminator.
        let sse = format!(
            "event: records\ndata: {records_json}\n\nevent: outcome\ndata: {outcome_json}\n\n"
        );
        let (head, tail) = sse.split_at(20);
        let url = serve(chunked_response(&[head, tail])).await;

        let client = GatewayClient::new(&url, "alice").unwrap();
        let mut events = client
            .open_prompt("assistant", "demo", "t-1", "hi", 0)
            .await
            .unwrap();

        match events.next().await {
            Some(Event::Records(got)) => assert_eq!(got, records),
            other => panic!("expected records, got {other:?}"),
        }
        match events.next().await {
            Some(Event::Outcome(Ok(c))) => assert_eq!(c.text(), "the sum is 55"),
            other => panic!("expected outcome, got {other:?}"),
        }
        assert!(
            events.next().await.is_none(),
            "stream should end after the body closes"
        );
    }

    #[tokio::test]
    async fn reads_a_content_length_json_body() {
        let body = serde_json::json!({
            "sessions": [{ "session": "demo", "label": "demo" }, { "session": "notes" }]
        })
        .to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        );
        let url = serve(response.into_bytes()).await;

        let client = GatewayClient::new(&url, "alice").unwrap();
        let sessions = client.list_sessions("assistant").await.unwrap();
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].session, "demo");
        assert_eq!(sessions[1].label, None);
    }

    #[tokio::test]
    async fn surfaces_a_non_200_as_an_error() {
        let response =
            b"HTTP/1.1 401 Unauthorized\r\ncontent-length: 21\r\n\r\n{\"error\":\"bad token\"}"
                .to_vec();
        let url = serve(response).await;
        let client = GatewayClient::new(&url, "nope").unwrap();
        let err = client.list_sessions("assistant").await.unwrap_err();
        assert!(err.contains("401"), "{err}");
    }
}
