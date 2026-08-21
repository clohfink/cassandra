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
#
# Reads the SDK local-output JSONL and reports property outcomes the way a triage report would:
#   - always / alwaysOrUnreachable: PASS unless it ever evaluated false; a false is a FINDING.
#   - unreachable: PASS unless it was ever hit; a hit is a FINDING.
#   - sometimes: SATISFIED if it was true at least once; otherwise NOT-YET-SEEN (a coverage gap,
#     not a bug -- locally, with no fault injection, several are expected not to fire).

import json
import sys
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/tcm-sdk-local.jsonl"

# message -> {"type": ..., "evals": n, "true": n, "false": n, "false_examples": [...], "registered": bool}
props = defaultdict(lambda: {"type": None, "evals": 0, "true": 0, "false": 0,
                             "false_examples": [], "registered": False})

with open(path) as fh:
    for line in fh:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except ValueError:
            continue
        a = obj.get("antithesis_assert")
        if not a:
            continue
        msg = a.get("message", "<no message>")
        p = props[msg]
        p["type"] = a.get("assert_type", "?")
        # The SDK emits a catalog-registration line with hit=false when an assertion is first
        # encountered, then a hit=true line for each actual evaluation. Only hit=true lines are
        # evaluations; counting registration lines would, for example, make an unreachable that
        # merely exists look like it fired.
        if not a.get("hit", True):
            p["registered"] = True
            continue
        p["evals"] += 1
        cond = a.get("condition", False)
        if cond:
            p["true"] += 1
        else:
            p["false"] += 1
            if len(p["false_examples"]) < 3:
                p["false_examples"].append(a.get("details", {}))

findings = []      # (msg, type, detail-of-first-false-or-hit)
satisfied = []     # sometimes that fired
not_seen = []      # sometimes that never fired
passed = []        # always/unreachable that held

for msg, p in sorted(props.items()):
    t = p["type"]
    if t in ("always", "always_or_unreachable"):
        if p["false"] > 0:
            findings.append((msg, t, p))
        else:
            passed.append((msg, t, p))
    elif t == "unreachable":
        # Assert.unreachable(...) only runs when control actually reaches that line, so any
        # evaluation recorded for it is a hit -- i.e. a state that was declared impossible.
        if p["evals"] > 0:
            findings.append((msg, t, p))
        else:
            passed.append((msg, t, p))
    elif t == "sometimes":
        if p["true"] > 0:
            satisfied.append((msg, t, p))
        else:
            not_seen.append((msg, t, p))
    else:
        # reachable or unknown
        satisfied.append((msg, t, p))


def fmt(p):
    return "evals=%d true=%d false=%d" % (p["evals"], p["true"], p["false"])


print("=" * 78)
print("TCM local run — property outcomes")
print("=" * 78)
print("total assertion evaluations:", sum(p["evals"] for p in props.values()))
print("distinct properties seen:   ", len(props))
print()

print("FINDINGS (safety/impossible-state assertions that did not hold):")
if not findings:
    print("  none — no always/alwaysOrUnreachable evaluated false, no unreachable was hit")
else:
    for msg, t, p in findings:
        print("  [%s] %s  (%s)" % (t.upper(), msg, fmt(p)))
        for ex in p["false_examples"]:
            print("        e.g. %s" % json.dumps(ex))
print()

print("SATISFIED (safety assertions that held every evaluation):")
for msg, t, p in passed:
    print("  [%s] %s  (%s)" % (t, msg, fmt(p)))
print()

print("REACHED (sometimes-conditions that fired at least once):")
for msg, t, p in satisfied:
    print("  [ok] %s  (%s)" % (msg, fmt(p)))
print()

print("NOT REACHED (sometimes-conditions never satisfied — coverage gaps, expected without faults):")
for msg, t, p in not_seen:
    print("  [--] %s  (%s)" % (msg, fmt(p)))
print()

print("=" * 78)
if findings:
    print("RESULT: %d finding(s) — see above." % len(findings))
    sys.exit(1)
else:
    print("RESULT: no property violations. %d reachability condition(s) not yet reached." % len(not_seen))
    sys.exit(0)
