# fc-rootfs: microVM assets for the `firecracker` Native tier

`build.sh` produces everything `harness-sandbox --features firecracker`
needs at runtime, into `out/` (gitignored):

| Asset | What | Source |
|---|---|---|
| `fc-agent` | the guest agent (`guest/fc-agent`), static musl, **built and tested** in `rust:alpine` | this repo |
| `rootfs.ext4` | alpine + `/sbin/fc-agent`, assembled with `mkfs.ext4 -d` (no root, no loop mounts) | `alpine` image |
| `vmlinux` | an uncompressed, vsock-enabled kernel | the `firecracker-ci` S3 bucket |
| `firecracker` | the VMM | GitHub releases |

Docker is the only requirement: **building runs fine on macOS**. *Running*
the assets requires Linux with `/dev/kvm` — Firecracker has no emulation
fallback, and Docker Desktop's VM does not expose KVM (Apple Silicon nested
virtualization needs M3+). The split is deliberate:

- everywhere (incl. macOS): `cargo test -p harness-sandbox --features
  firecracker` exercises the wire protocol, the tar sync, and the conduct
  paths against a fake guest; `guest/fc-agent && cargo test` exercises the
  real agent over `--uds`;
- Linux + KVM (CI's `firecracker` job, or any Linux box): the same command
  additionally boots real microVMs through `tests/firecracker.rs`, reading
  the assets from `out/` (override with `HARNESS_FC_ASSETS=<dir>`).

Knobs, all env: `ARCH` (`x86_64`/`aarch64`, defaults to the docker engine's),
`ALPINE`, `FC_VERSION`, `CI_VERSION`, `KERNEL` — see the script header.
Cross-arch builds work through docker's `--platform` emulation.

The agent and `crates/harness-sandbox/src/firecracker.rs` are the two ends
of one wire protocol (framed JSON + tar over vsock, documented in both);
change them together.
