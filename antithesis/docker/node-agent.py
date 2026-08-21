#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Supervisor and control agent for one Cassandra node in the Antithesis harness.

Why this exists
---------------
Antithesis node faults -- including node termination -- are disabled by default. Three things the
property catalog depends on are restart-shaped:

  * a-epoch-monotonic-per-node's cross-restart case (the CASSANDRA-19384 regression)
  * b-sequence-resumable-after-crash, where TCM_implementation.md explicitly invites killing the
    node "an arbitrary number of times during streaming"
  * node replacement, which requires restarting the JVM with
    -Dcassandra.replace_address_first_boot=<addr>. No JMX call can do this.

This agent is a *workload action*, not a fault-injection bypass. It is reachable only over the
container network, so Antithesis network faults apply to it exactly as they do to CQL and JMX --
a partition can cut the workload off from it, and the workload has to cope. Everything it does is
something an operator does with systemctl and a config edit; it never touches Cassandra internals
or cluster metadata.

It runs Cassandra as a child process with inherited stdio, so Cassandra's output goes to the
container's stdout where Antithesis collects it.

Endpoints (all respond with JSON):
    GET  /status                      -> {"running": bool, "pid": int|null, "flags": [...], ...}
    POST /start                       -> start if not running; body {"flags": ["-Dfoo=bar"]}
    POST /stop                        -> SIGTERM and wait; ?force=1 sends SIGKILL
    POST /restart                     -> stop then start; body may carry new flags
    POST /wipe-and-restart            -> stop, delete data/commitlog/hints/saved_caches, start
    POST /replace?address=<addr>      -> wipe, then start with replace_address_first_boot

Only the standard library is used: python3 is already in the image for cqlsh.
"""

import http.server
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.parse

CASSANDRA_HOME = os.environ.get("CASSANDRA_HOME", "/opt/cassandra")
AGENT_PORT = int(os.environ.get("NODE_AGENT_PORT", "7788"))
# Directories wiped by /wipe-and-restart and /replace. Keep in sync with the paths written into
# cassandra.yaml by entrypoint-cassandra.sh.
#
# There is no separate metadata directory: TCM persists its log in the system_cluster_metadata
# keyspace, which lives under the data directories, so wiping data also wipes the local log. That is
# what makes wipe-and-restart force a node back through Startup/Discovery.
STATE_DIRS = [
    os.environ.get("CASSANDRA_DATA_DIR", "/var/lib/cassandra/data"),
    os.environ.get("CASSANDRA_COMMITLOG_DIR", "/var/lib/cassandra/commitlog"),
    os.environ.get("CASSANDRA_HINTS_DIR", "/var/lib/cassandra/hints"),
    os.environ.get("CASSANDRA_SAVED_CACHES_DIR", "/var/lib/cassandra/saved_caches"),
    os.environ.get("CASSANDRA_ACCORD_JOURNAL_DIR", "/var/lib/cassandra/accord_journal"),
]

# Serialises every mutating operation. Two concurrent /restart calls must not both spawn a JVM.
_lock = threading.Lock()


def log(msg):
    print("[node-agent] %s" % msg, flush=True)


class Supervisor:
    """Owns the Cassandra child process."""

    def __init__(self):
        self.proc = None
        # Flags applied on every start, from the environment (e.g. -Dcassandra.join_ring=false for
        # a spare node). Persisted across restarts so a restarted spare stays a spare.
        self.base_flags = [f for f in os.environ.get("CASSANDRA_BASE_FLAGS", "").split() if f]
        # One-shot flags for the next start only (e.g. replace_address_first_boot, which must not
        # survive into subsequent boots -- hence "first_boot").
        self.next_flags = []
        self.started_at = None
        self.start_count = 0
        # Preserved after the child is reaped. Without this, an unexpected Cassandra exit becomes
        # invisible the moment the reaper clears self.proc, and the workload can no longer tell a
        # crashed node from a partitioned one.
        self.last_exit_code = None

    def is_running(self):
        return self.proc is not None and self.proc.poll() is None

    def start(self, flags=None):
        if self.is_running():
            return False, "already running"

        flags = list(flags or [])
        all_flags = self.base_flags + self.next_flags + flags
        self.next_flags = []

        env = dict(os.environ)
        # cassandra-env.sh appends JVM_EXTRA_OPTS to JVM_OPTS (conf/cassandra-env.sh:333), which is
        # how arbitrary -D flags reach the daemon without editing any config file.
        extra = env.get("JVM_EXTRA_OPTS", "")
        env["JVM_EXTRA_OPTS"] = (extra + " " + " ".join(all_flags)).strip()

        cmd = [os.path.join(CASSANDRA_HOME, "bin", "cassandra"), "-f"]
        log("starting: %s  (JVM_EXTRA_OPTS=%r)" % (" ".join(cmd), env["JVM_EXTRA_OPTS"]))
        # Inherit stdout/stderr so Cassandra's logs land on container output.
        self.proc = subprocess.Popen(cmd, env=env, stdout=sys.stdout, stderr=sys.stderr)
        self.started_at = time.time()
        self.start_count += 1
        return True, "started pid %d with flags %s" % (self.proc.pid, all_flags)

    def stop(self, force=False, timeout=90):
        if not self.is_running():
            return False, "not running"

        pid = self.proc.pid
        if force:
            log("SIGKILL %d" % pid)
            self.proc.send_signal(signal.SIGKILL)
        else:
            log("SIGTERM %d" % pid)
            self.proc.send_signal(signal.SIGTERM)

        deadline = time.time() + timeout
        while time.time() < deadline:
            code = self.proc.poll()
            if code is not None:
                self.last_exit_code = code
                self.proc = None
                self.started_at = None
                return True, "stopped %d (exit %s)" % (pid, code)
            time.sleep(0.2)

        # A graceful stop that overruns the deadline is escalated rather than left hanging: a
        # half-dead node would make every "did it answer?" check ambiguous, which is exactly the
        # partitioned-vs-down confusion h-all-nodes-compared exists to surface.
        log("stop timed out after %ds, escalating to SIGKILL for %d" % (timeout, pid))
        self.proc.send_signal(signal.SIGKILL)
        self.last_exit_code = self.proc.wait()
        self.proc = None
        self.started_at = None
        return True, "killed %d after graceful stop timed out" % pid

    def wipe_state(self):
        wiped = []
        for d in STATE_DIRS:
            if os.path.isdir(d):
                for entry in os.listdir(d):
                    path = os.path.join(d, entry)
                    if os.path.isdir(path):
                        shutil.rmtree(path, ignore_errors=True)
                    else:
                        try:
                            os.remove(path)
                        except OSError:
                            pass
                wiped.append(d)
        log("wiped %s" % wiped)
        return wiped

    def status(self):
        return {
            "running": self.is_running(),
            "pid": self.proc.pid if self.is_running() else None,
            "uptime_seconds": (time.time() - self.started_at) if self.started_at else None,
            "start_count": self.start_count,
            "base_flags": self.base_flags,
            "pending_flags": self.next_flags,
            "last_exit_code": self.last_exit_code,
            "hostname": os.environ.get("CASSANDRA_LISTEN_ADDRESS", ""),
        }


SUP = Supervisor()


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _respond(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        length = int(self.headers.get("Content-Length") or 0)
        if not length:
            return {}
        try:
            return json.loads(self.rfile.read(length).decode() or "{}")
        except ValueError:
            return {}

    def log_message(self, fmt, *args):
        # Silence per-request logging: the workload polls /status frequently and the noise would
        # bury Cassandra's own output in the triage report.
        pass

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        if path in ("/status", "/"):
            self._respond(200, SUP.status())
        else:
            self._respond(404, {"error": "unknown path %s" % path})

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)
        body = self._body()
        flags = body.get("flags") or []

        with _lock:
            try:
                if path == "/start":
                    ok, msg = SUP.start(flags)
                elif path == "/stop":
                    force = query.get("force", ["0"])[0] not in ("0", "", "false")
                    ok, msg = SUP.stop(force=force)
                elif path == "/restart":
                    force = query.get("force", ["0"])[0] not in ("0", "", "false")
                    SUP.stop(force=force)
                    ok, msg = SUP.start(flags)
                elif path == "/wipe-and-restart":
                    force = query.get("force", ["0"])[0] not in ("0", "", "false")
                    SUP.stop(force=force)
                    wiped = SUP.wipe_state()
                    ok, msg = SUP.start(flags)
                    msg = "%s (wiped %s)" % (msg, wiped)
                elif path == "/replace":
                    # No address => replace-same-address: this node re-bootstraps and takes over its
                    # OWN previous ring position (the disk-loss-rebuild scenario). The old instance
                    # must already be down and marked down by peers, which the workload ensures before
                    # calling. An explicit address still allows replacing a different (dead) node.
                    address = query.get("address", [None])[0]
                    if not address:
                        address = os.environ.get("CASSANDRA_LISTEN_ADDRESS")
                    if not address:
                        self._respond(400, {"error": "replace requires ?address=<broadcast address> "
                                                     "or CASSANDRA_LISTEN_ADDRESS to be set"})
                        return
                    SUP.stop(force=True)
                    wiped = SUP.wipe_state()
                    # A replacement JOINS the ring, so drop join_ring=false permanently: a spare used
                    # as the replacement becomes a full ring member and must not revert to a spare on
                    # any later restart. (No-op for a ring node, whose base_flags never had it.)
                    SUP.base_flags = [f for f in SUP.base_flags if "join_ring=false" not in f]
                    # replace_address_first_boot is deliberately one-shot: it must not persist into
                    # later restarts of this node, or every subsequent boot would try to replace.
                    SUP.next_flags = ["-Dcassandra.replace_address_first_boot=%s" % address]
                    ok, msg = SUP.start(flags)
                    msg = "%s (replacing %s, wiped %s)" % (msg, address, wiped)
                else:
                    self._respond(404, {"error": "unknown path %s" % path})
                    return
            except Exception as exc:  # surfaced to the workload rather than killing the agent
                log("error handling %s: %r" % (path, exc))
                self._respond(500, {"error": repr(exc), "status": SUP.status()})
                return

        self._respond(200, {"ok": ok, "message": msg, "status": SUP.status()})


class ThreadingHTTPServer(http.server.ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    autostart = os.environ.get("NODE_AGENT_AUTOSTART", "1") not in ("0", "false", "")
    if autostart:
        with _lock:
            SUP.start()

    server = ThreadingHTTPServer(("0.0.0.0", AGENT_PORT), Handler)
    log("control agent listening on 0.0.0.0:%d" % AGENT_PORT)

    def shutdown(signum, _frame):
        log("received signal %d, stopping cassandra and exiting" % signum)
        with _lock:
            SUP.stop()
        server.shutdown()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # Reap the child if it exits on its own so /status reports it accurately. A Cassandra process
    # that dies unexpectedly must show as not-running, otherwise the workload cannot tell a dead
    # node from a partitioned one.
    def reaper():
        while True:
            time.sleep(2)
            proc = SUP.proc
            if proc is not None and proc.poll() is not None:
                log("cassandra exited with code %s" % proc.returncode)
                with _lock:
                    if SUP.proc is proc and proc.poll() is not None:
                        SUP.last_exit_code = proc.returncode
                        SUP.proc = None
                        SUP.started_at = None

    threading.Thread(target=reaper, daemon=True).start()
    server.serve_forever()


if __name__ == "__main__":
    main()
