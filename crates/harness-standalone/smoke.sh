#!/bin/bash
# End-to-end smoke test for harness-standalone: three node processes against
# a fake Messages API, one prompt, a tail, then a node kill and a same-turn
# resume from a surviving node. Run from the workspace root.
set -u
cd "$(dirname "$0")/../.."

DATA=$(mktemp -d)
BIN=target/debug/harness-standalone
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
  "$BIN" node --id "$i" --data "$DATA/data" --api-url http://127.0.0.1:7600 \
    > "$DATA/node$i.log" 2>&1 &
  NODE_PIDS+=($!)
  PIDS+=($!)
done

python3 - <<'EOF'
import json, socket, sys, time

def request(port, obj, timeout=70, attempts=30):
    last = None
    for _ in range(attempts):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=timeout) as s:
                f = s.makefile("rw")
                f.write(json.dumps(obj) + "\n")
                f.flush()
                return json.loads(f.readline())
        except OSError as e:
            last = e
            time.sleep(1)
    sys.exit(f"no answer on {port}: {last}")

prompt = {"type": "prompt", "kind": "assistant", "session": "demo",
          "turn": "t-1", "content": "hello", "within_secs": 60}

out = request(7501, {"id": 1, "op": prompt})["body"]
assert out.get("type") == "outcome" and "Ok" in out["outcome"], out
print("PROMPT:", out["outcome"]["Ok"]["content"])

records = request(7501, {"id": 2, "op": {"type": "tail", "kind": "assistant",
    "session": "demo", "from": 0, "limit": 100}})["body"]["records"]
bodies = [r["body"] if isinstance(r["body"], str) else next(iter(r["body"]))
          for _, r in records]
print("TAIL:", bodies)
assert "TurnSubmitted" in bodies and "RunEnded" in bodies, bodies
EOF
[ $? -eq 0 ] || exit 1

echo "KILL node 1 (pid ${NODE_PIDS[0]})"
kill "${NODE_PIDS[0]}"

python3 - <<'EOF'
import json, socket, sys, time

def request(port, obj, timeout=70):
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as s:
        f = s.makefile("rw")
        f.write(json.dumps(obj) + "\n")
        f.flush()
        return json.loads(f.readline())

# The same turn id through a surviving node: dedups to the recorded outcome
# (or re-attaches), once placement routes around the dead node.
prompt = {"type": "prompt", "kind": "assistant", "session": "demo",
          "turn": "t-1", "content": "hello", "within_secs": 60}
deadline = time.time() + 60
while True:
    try:
        out = request(7502, {"id": 3, "op": prompt})["body"]
        if out.get("type") == "outcome" and "Ok" in out["outcome"]:
            print("RESUME:", out["outcome"]["Ok"]["content"])
            break
        print("retrying after:", out)
    except OSError as e:
        print("retrying after:", e)
    if time.time() > deadline:
        sys.exit("resume did not complete within 60s")
    time.sleep(2)

# And a brand-new turn lands on the degraded cluster too.
out = request(7502, {"id": 4, "op": {"type": "prompt", "kind": "assistant",
    "session": "demo", "turn": "t-2", "content": "again", "within_secs": 60}})["body"]
assert out.get("type") == "outcome" and "Ok" in out["outcome"], out
print("NEW TURN:", out["outcome"]["Ok"]["content"])
EOF
status=$?
echo "--- node 2 log tail ---"
tail -5 "$DATA/node2.log"
exit $status
