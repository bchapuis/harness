//! Length-delimited framing of transport messages on a byte stream (spec §7).
//!
//! Each message goes out as a `u32` big-endian length prefix followed by the
//! codec-encoded [`Wire`] bytes. Reading enforces a maximum size and surfaces a
//! malformed or oversized message as an [`io::Error`]; the caller tears down the
//! **association**, not the node (spec §7). The handshake [`Hello`] preamble uses
//! the same framing, so the whole exchange is uniform.
//!
//! [`Wire`] is the transport's own envelope: it carries either an actor
//! [`Frame`] (handed up to the cluster) or an [`Endpoints`](Wire::Endpoints)
//! table (consumed by the transport itself for address discovery, spec §9.3).

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use actor_cluster::Frame;
use actor_core::NodeId;
use actor_serialization::Codec;
use actor_serialization::decode;
use actor_serialization::encode;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

/// Upper bound on a single encoded message. A larger length prefix is rejected
/// as malformed before any allocation, so a hostile or corrupt peer cannot force
/// a huge buffer (spec §7, §15).
pub const MAX_FRAME_LEN: u32 = 16 * 1024 * 1024;

/// The association handshake preamble (spec §7.1): protocol version, the
/// sender's node identity and the address peers should dial it back on, the
/// codec it speaks, and a shared cluster secret. The receiver rejects the
/// association on any mismatch (§7, §15) and learns the advertised address.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Hello {
    pub proto_version: u32,
    pub node: NodeId,
    pub advertised: SocketAddr,
    pub codec_name: String,
    pub cluster_secret: String,
}

/// A transport-level message on an established association. Actor traffic is a
/// [`Frame`]; [`Endpoints`](Wire::Endpoints) carries a `(node, address)` table
/// for dynamic address discovery — the transport consumes it and never hands it
/// up to the cluster (spec §9.3).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Wire {
    Frame(Frame),
    Endpoints(Vec<(NodeId, SocketAddr)>),
}

/// Write `bytes` as a length-delimited record. Errors propagate so the caller
/// can drop the association.
async fn write_record<W>(stream: &mut W, bytes: &[u8]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let len = u32::try_from(bytes.len())
        .ok()
        .filter(|&n| n <= MAX_FRAME_LEN)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "frame exceeds maximum size"))?;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(bytes).await?;
    stream.flush().await
}

/// Read one length-delimited record, enforcing [`MAX_FRAME_LEN`].
async fn read_record<R>(stream: &mut R) -> io::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame length prefix exceeds maximum size",
        ));
    }
    let mut buf = vec![0u8; len as usize];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Serialize and write one [`Wire`] message with `codec`.
pub async fn write_wire<W>(stream: &mut W, codec: &Arc<dyn Codec>, msg: &Wire) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let bytes = encode(codec.as_ref(), msg)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    write_record(stream, &bytes).await
}

/// Read and decode one [`Wire`] message with `codec`. A decode failure is an
/// `InvalidData` error: the association is malformed and must be torn down.
pub async fn read_wire<R>(stream: &mut R, codec: &Arc<dyn Codec>) -> io::Result<Wire>
where
    R: AsyncRead + Unpin,
{
    let bytes = read_record(stream).await?;
    decode::<Wire>(codec.as_ref(), &bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

/// Write the handshake preamble. Always JSON-framed independent of the
/// negotiated codec, so the `codec_name` field can be compared before any
/// codec-specific decoding happens.
pub async fn write_hello<W>(stream: &mut W, hello: &Hello) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let bytes = serde_json_to_vec(hello)?;
    write_record(stream, &bytes).await
}

/// Read the handshake preamble.
pub async fn read_hello<R>(stream: &mut R) -> io::Result<Hello>
where
    R: AsyncRead + Unpin,
{
    let bytes = read_record(stream).await?;
    serde_json_from_slice(&bytes)
}

// The handshake is framed with a fixed codec (the bundled JSON codec) rather
// than the negotiated one, because codec agreement is itself part of what the
// handshake establishes. We go through the `Codec` trait to avoid a direct
// serde_json dependency in this crate.
fn serde_json_to_vec<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
    let codec = actor_serialization::JsonCodec;
    encode(&codec, value).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

fn serde_json_from_slice<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> io::Result<T> {
    let codec = actor_serialization::JsonCodec;
    decode::<T>(&codec, bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use actor_cluster::CallId;
    use actor_core::ActorId;
    use actor_core::Path;

    fn json() -> Arc<dyn Codec> {
        Arc::new(actor_serialization::JsonCodec)
    }

    #[tokio::test]
    async fn frame_round_trips_over_a_duplex_stream() {
        let codec = json();
        let msg = Wire::Frame(Frame::Envelope {
            recipient: ActorId::new(NodeId::new(7), Path::new("/user/greeter"), 1),
            manifest: "demo.Greet".to_string(),
            correlation: Some(CallId(42)),
            payload: b"{\"name\":\"world\"}".to_vec(),
        });

        let (mut a, mut b) = tokio::io::duplex(64 * 1024);
        write_wire(&mut a, &codec, &msg).await.unwrap();
        let got = read_wire(&mut b, &codec).await.unwrap();

        match got {
            Wire::Frame(Frame::Envelope {
                recipient,
                manifest,
                correlation,
                payload,
            }) => {
                assert_eq!(recipient.node, NodeId::new(7));
                assert_eq!(manifest, "demo.Greet");
                assert_eq!(correlation, Some(CallId(42)));
                assert_eq!(payload, b"{\"name\":\"world\"}");
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[tokio::test]
    async fn endpoints_message_round_trips() {
        let codec = json();
        let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let msg = Wire::Endpoints(vec![(NodeId::new(2), addr)]);
        let (mut a, mut b) = tokio::io::duplex(4096);
        write_wire(&mut a, &codec, &msg).await.unwrap();
        match read_wire(&mut b, &codec).await.unwrap() {
            Wire::Endpoints(table) => assert_eq!(table, vec![(NodeId::new(2), addr)]),
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[tokio::test]
    async fn hello_round_trips() {
        let hello = Hello {
            proto_version: 1,
            node: NodeId::new(3),
            advertised: "127.0.0.1:9003".parse().unwrap(),
            codec_name: "json".to_string(),
            cluster_secret: "s3cr3t".to_string(),
        };
        let (mut a, mut b) = tokio::io::duplex(4096);
        write_hello(&mut a, &hello).await.unwrap();
        let got = read_hello(&mut b).await.unwrap();
        assert_eq!(got.node, NodeId::new(3));
        assert_eq!(got.advertised, hello.advertised);
        assert_eq!(got.codec_name, "json");
        assert_eq!(got.cluster_secret, "s3cr3t");
    }

    #[tokio::test]
    async fn an_oversized_length_prefix_is_rejected_without_allocating() {
        // A length prefix beyond the cap must error on read, not attempt a huge
        // allocation — a hostile peer cannot wedge the node (spec §7, §15).
        let (mut a, mut b) = tokio::io::duplex(64);
        a.write_all(&(MAX_FRAME_LEN + 1).to_be_bytes())
            .await
            .unwrap();
        a.flush().await.unwrap();
        let err = read_wire(&mut b, &json()).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
