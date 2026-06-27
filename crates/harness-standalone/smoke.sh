#!/bin/bash
# End-to-end smoke test for harness-standalone: three node processes against a
# fake Messages API, fronted by the harness-gateway (the public HTTP edge, joined
# as a cluster client). One prompt over HTTP, a records read, then a node kill and
# a same-turn resume — driven entirely through the gateway, which routes around
# the dead node. Run from the workspace root.
set -u
cd "$(dirname "$0")/../.."

DATA=$(mktemp -d)
BIN=target/debug/harness-standalone
GATEWAY_BIN=target/debug/harness-gateway
GATEWAY_ID=100
PIDS=()
cleanup() { kill "${PIDS[@]}" 2>/dev/null; wait 2>/dev/null; }
trap cleanup EXIT

# A canned Messages API: every completion is a plain final message.
python3 - > "$DATA/fake-api.log" 2>&1 <<'EOF' &
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
class H(BaseHTTPRequestHandler):
    def do_POST(self):
        self.rfile.read(int(self.headers.get("content-length", 0)))
        body = json.dumps({
            "content": [{"type": "text", "text": "smoke-ok"}],
            "usage": {"input_tokens": 10, "output_tokens": 5},
        }).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass
HTTPServer(("127.0.0.1", 7600), H).serve_forever()
EOF
PIDS+=($!)

export ANTHROPIC_API_KEY=sk-smoke
NODE_PIDS=()
for i in 1 2 3; do
  # --sandbox local is the unconfined mode, chosen deliberately: this smoke
  # run's only input is the canned fake API above — trusted by construction
  # (sandbox spec §3.4) — and `local` keeps the script dependency-free.
  # --client admits the gateway (id $GATEWAY_ID) as a non-voting member.
  "$BIN" node --id "$i" --data "$DATA/data" --api-url http://127.0.0.1:7600 \
    --sandbox local --client "$GATEWAY_ID=127.0.0.1" \
    > "$DATA/node$i.log" 2>&1 &
  NODE_PIDS+=($!)
  PIDS+=($!)
done

# The gateway joins as client id $GATEWAY_ID and serves HTTP on 8080. No
# --auth-tokens, so the bearer token IS the tenant (loopback insecure mode).
"$GATEWAY_BIN" --bind 127.0.0.1:8080 --node-id "$GATEWAY_ID" --nodes 3 \
  > "$DATA/gateway.log" 2>&1 &
PIDS+=($!)

python3 - <<'EOF'
import json, sys, time, urllib.request, urllib.error

BASE = "http://127.0.0.1:8080"
HDRS = {"Authorization": "Bearer demo", "Content-Type": "application/json"}

def http(method, path, body=None, timeout=70, attempts=40):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, headers=HDRS, method=method)
    last = None
    for _ in range(attempts):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except (urllib.error.URLError, OSError) as e:
            last = e
            time.sleep(1)
    sys.exit(f"no answer on {method} {path}: {last}")

prompt = {"turn": "t-1", "content": "hello", "within_secs": 60}
out = http("POST", "/v1/assistant/demo/prompt", prompt)["outcome"]
assert "Ok" in out, out
print("PROMPT:", out["Ok"]["content"])

records = http("GET", "/v1/assistant/demo/records?from=0&limit=100")["records"]
bodies = [r["body"] if isinstance(r["body"], str) else next(iter(r["body"]))
          for _, r in records]
print("RECORDS:", bodies)
assert "TurnSubmitted" in bodies and "RunEnded" in bodies, bodies
EOF
[ $? -eq 0 ] || exit 1

echo "KILL node 1 (pid ${NODE_PIDS[0]})"
kill "${NODE_PIDS[0]}"

python3 - <<'EOF'
import json, sys, time, urllib.request, urllib.error

BASE = "http://127.0.0.1:8080"
HDRS = {"Authorization": "Bearer demo", "Content-Type": "application/json"}

def post(path, body, timeout=70):
    req = urllib.request.Request(BASE + path, data=json.dumps(body).encode(),
                                 headers=HDRS, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())

# The same turn id through the gateway: dedups to the recorded outcome (or
# re-attaches), once placement routes around the dead node.
prompt = {"turn": "t-1", "content": "hello", "within_secs": 60}
deadline = time.time() + 60
while True:
    try:
        out = post("/v1/assistant/demo/prompt", prompt)["outcome"]
        if "Ok" in out:
            print("RESUME:", out["Ok"]["content"])
            break
        print("retrying after:", out)
    except (urllib.error.URLError, OSError) as e:
        print("retrying after:", e)
    if time.time() > deadline:
        sys.exit("resume did not complete within 60s")
    time.sleep(2)

# And a brand-new turn lands on the degraded cluster too.
out = post("/v1/assistant/demo/prompt",
           {"turn": "t-2", "content": "again", "within_secs": 60})["outcome"]
assert "Ok" in out, out
print("NEW TURN:", out["Ok"]["content"])
EOF
status=$?
echo "--- node 2 log tail ---"
tail -5 "$DATA/node2.log"
echo "--- gateway log tail ---"
tail -5 "$DATA/gateway.log"
exit $status
