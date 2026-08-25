//! The deployment's [`HttpPost`] seam (`harness::http`'s one required
//! operation), served by the shared [`harness_http`] substrate: one
//! connection per request with `connection: close`, TLS from rustls with
//! webpki trust anchors.
//!
//! Retries, backoff, and the error taxonomy live above this seam in
//! `harness::http`; everything here maps onto `HttpError` and is retried as
//! transport pressure.
//!
//! A plain `http://` base skips TLS, so tests (and a scripted fake API)
//! can point the client at a local listener.

use std::time::Duration;

use actor_core::BoxFuture;
use harness::HttpError;
use harness::HttpPost;
use harness::HttpResponse;
use harness_http::Origin;
use harness_http::Reader;

/// Bound on one whole request, connect to last byte. Generous: a large
/// completion can take minutes; the model-side retry policy sits above.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

/// An HTTP/1.1 POST client for one base URL.
pub struct HttpsPost {
    origin: Origin,
}

impl HttpsPost {
    /// Build a client for `base`: `https://host[:port]` or, for local fakes,
    /// `http://host[:port]`. No path suffix.
    pub fn new(base: &str) -> Result<HttpsPost, String> {
        Ok(HttpsPost {
            origin: Origin::parse(base)?,
        })
    }
}

impl HttpPost for HttpsPost {
    fn post(
        &self,
        path: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
    ) -> BoxFuture<'static, Result<HttpResponse, HttpError>> {
        let headers: Vec<(&str, &str)> = headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        let request = self.origin.request("POST", path, &headers, &body);
        let origin = self.origin.clone();
        Box::pin(async move {
            let exchange = async {
                let stream = origin.connect().await?;
                let mut reader = Reader::new(stream);
                reader.write_all(&request).await?;
                let status = reader.read_headers().await?;
                let body = reader.read_body().await?;
                Ok::<_, String>(HttpResponse { status, body })
            };
            tokio::time::timeout(REQUEST_TIMEOUT, exchange)
                .await
                .map_err(|_| HttpError("request timed out".to_string()))?
                .map_err(HttpError)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

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
