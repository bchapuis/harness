//! Integration: the mutual-TLS association handshake (spec §7.1, §15). A cluster
//! whose nodes share TLS trust, the cluster secret, and the allowlist talks over
//! encrypted connections; mismatched trust, secret, or allowlist rejects the
//! association — surfacing to callers as `Unreachable`, never a hang.

mod support;

use std::collections::BTreeSet;
use std::time::Duration;

use actor_core::ActorSystem;
use actor_core::CallError;
use actor_core::NodeId;
use actor_runtime::TcpCluster;

use support::Greet;
use support::Greeter;
use support::TlsAuthority;
use support::tcp_config;

/// Drive a remote ask from A to a greeter on B, bounded so a rejected (and thus
/// never-answered) association shows up as a failure rather than a hung test.
async fn ask_across(sys_a: &TcpCluster, sys_b: &TcpCluster) -> Result<String, CallError> {
    let greeter = sys_b.spawn(Greeter::<TcpCluster>::new("Hello"));
    let remote = sys_a.resolve::<Greeter<TcpCluster>>(greeter.id().clone());
    tokio::time::timeout(
        Duration::from_secs(5),
        remote.ask(Greet {
            name: "world".into(),
        }),
    )
    .await
    .expect("ask must not hang")
}

#[tokio::test]
async fn mutual_tls_association_carries_traffic() {
    // One authority: both nodes present its cert and trust its CA.
    let auth = TlsAuthority::generate();
    let tls = auth.tls_config();
    let with_tls = |node, peers| {
        let mut cfg = tcp_config(node, peers);
        cfg.tls = Some(tls.clone());
        cfg
    };

    let (sys_a, sys_b) = support::two_nodes_with(with_tls, with_tls).await;
    assert_eq!(
        ask_across(&sys_a, &sys_b).await,
        Ok("Hello, world!".to_string())
    );
}

#[tokio::test]
async fn a_wrong_cluster_secret_is_rejected() {
    let auth = TlsAuthority::generate();
    let tls = auth.tls_config();
    let make = |secret: &'static str| {
        let tls = tls.clone();
        move |node, peers| {
            let mut cfg = tcp_config(node, peers);
            cfg.tls = Some(tls.clone());
            cfg.cluster_secret = secret.to_string();
            cfg
        }
    };

    let (sys_a, sys_b) = support::two_nodes_with(make("cluster-one"), make("cluster-two")).await;
    assert_eq!(
        ask_across(&sys_a, &sys_b).await,
        Err(CallError::Unreachable)
    );
}

#[tokio::test]
async fn an_untrusted_certificate_is_rejected() {
    // Two independent authorities: neither node trusts the other's CA, so the
    // TLS handshake itself fails.
    let auth_a = TlsAuthority::generate();
    let auth_b = TlsAuthority::generate();
    let tls_a = auth_a.tls_config();
    let tls_b = auth_b.tls_config();

    let (sys_a, sys_b) = support::two_nodes_with(
        move |node, peers| {
            let mut cfg = tcp_config(node, peers);
            cfg.tls = Some(tls_a.clone());
            cfg
        },
        move |node, peers| {
            let mut cfg = tcp_config(node, peers);
            cfg.tls = Some(tls_b.clone());
            cfg
        },
    )
    .await;
    assert_eq!(
        ask_across(&sys_a, &sys_b).await,
        Err(CallError::Unreachable)
    );
}

#[tokio::test]
async fn a_node_outside_the_allowlist_is_rejected() {
    // Trust and secret match, but A's allowlist omits node B (id 2).
    let auth = TlsAuthority::generate();
    let tls = auth.tls_config();
    let tls_for = tls.clone();

    let (sys_a, sys_b) = support::two_nodes_with(
        move |node, peers| {
            let mut cfg = tcp_config(node, peers);
            cfg.tls = Some(tls_for.clone());
            cfg.allowlist = Some(BTreeSet::from([NodeId::new(1)])); // only itself
            cfg
        },
        move |node, peers| {
            let mut cfg = tcp_config(node, peers);
            cfg.tls = Some(tls.clone());
            cfg
        },
    )
    .await;
    // B replies over its own dialed connection to A; A's accept-side handshake
    // rejects B (not allowlisted), so the reply never associates.
    assert_eq!(
        ask_across(&sys_a, &sys_b).await,
        Err(CallError::Unreachable)
    );
}
