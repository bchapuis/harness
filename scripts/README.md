# scripts

Developer entry points that are not the front door. The two demos the README
advertises stay at the repository root, because what a newcomer types first
should be visible first:

```sh
./demo-agent.sh      # a three-node cluster + the HTTP gateway, driven by curl
./demo-machine.sh    # the same cluster hosting a machine you SSH into
```

Everything else lives here, named `<verb>-<subject>.sh` so the verb sorts first
and the family is obvious at a glance:

- [`smoke-agent.sh`](smoke-agent.sh) — the `demo-agent.sh` story against a canned fake Messages API. No API key, no docker: three nodes, a prompt over the gateway, a records read, then a node kill and a same-turn resume.
- [`bench-machine-cost.sh`](bench-machine-cost.sh) — what a machine `create` costs, decomposed into the cold cluster's fixed cost and the path itself. Sweep image sizes to separate the two.

Two conventions apply elsewhere in the tree and are deliberately left alone:

- **A script that builds one artifact sits next to that artifact and is called `build.sh`** — `guest/fc-rootfs`, `guest/machine-rootfs`, `guest/machine-docker`, `guest/qjs-runner`. The directory supplies the subject, so the filename only needs the verb. CI cache keys hash these paths.
- **A script that drives one deployment sits with its manifests** — `k8s/deploy.sh`, next to the `harness.yaml` it applies and the `README.md` that explains the manual walkthrough it automates.

Every script here `cd`s to the workspace root on entry, so it runs correctly from
any working directory.
