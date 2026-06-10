#!/usr/bin/env bash
#
# update-changelog.sh - finalize CHANGES-NETFLIX.txt for a release tag.
#
# On a version tag (vX.Y.Z) this inserts a new "X.Y.Z (date)" section into
# CHANGES-NETFLIX.txt, directly below the marker line, summarizing every
# PR-referenced commit since the previous version tag.
#
# The section is generated DETERMINISTICALLY from `git log` (so it always works
# and can never invent a PR number). If a Netflix GenAI Model Gateway project is
# configured (GENAI_PROJECT_ID), the raw bullets are additionally sent to an LLM
# to be reworded/grouped - but the LLM result is REJECTED and the raw bullets
# used instead if the set of (#NNN) PR references changes in any way. The LLM can
# only ever improve wording, never alter which PRs are listed.
#
# Usage:
#   .netflix/update-changelog.sh [--tag vX.Y.Z] [--prev vX.Y.Z] [--file PATH]
#                                [--dry-run] [--no-llm] [--no-pr]
#
#   --tag      Release tag to finalize. Default: $ROCKET_TAG (set by Rocket on tag builds).
#   --prev     Previous version tag (range base). Default: nearest vX.Y.Z before --tag.
#   --file     Changelog file. Default: CHANGES-NETFLIX.txt at repo root.
#   --dry-run  Print the generated section and a unified diff; write/commit/PR NOTHING.
#   --no-llm   Skip the Model Gateway call; use raw git subjects only.
#   --no-pr    Write the file in place but do not commit, push, or open a PR.
#
# Env (LLM is optional - without GENAI_PROJECT_ID the script runs deterministically):
#   GENAI_PROJECT_ID    Registered GenAI Model Gateway project id (auth/cost boundary).
#                       Default: nfcassandra. Set --no-llm (or an empty value) to disable.
#   GENAI_MODEL         Model id (default: claude-3-5-sonnet - verify against the GenAI
#                       Supported Models list before relying on it).
#   GENAI_URL           Gateway chat-completions URL (default: prod us-east-1 VIP).
#   GENAI_METATRON_APP  Metatron app for `metatron curl -a` (default: copilotdppython).
#   CHANGELOG_PR_BASE   Base branch for the opened PR (default: cassandra-<major.minor>).
#   GIT_AUTHOR_NAME / GIT_AUTHOR_EMAIL  Commit identity (defaults: Rocket CI / jenkins@netflix.com).
#
set -uo pipefail

MARKER='<!-- CI inserts new release sections directly below this line -->'

# GenAI Model Gateway project id (auth/cost boundary). Defaults to this repo's project
# when unset; an explicitly empty value (GENAI_PROJECT_ID=) disables the LLM polish.
GENAI_PROJECT_ID="${GENAI_PROJECT_ID-nfcassandra}"

TAG="${ROCKET_TAG:-}"
PREV=""
FILE=""
DRY_RUN=""
NO_LLM=""
NO_PR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tag)     TAG="$2"; shift 2 ;;
    --prev)    PREV="$2"; shift 2 ;;
    --file)    FILE="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --no-llm)  NO_LLM=1; shift ;;
    --no-pr)   NO_PR=1; shift ;;
    -h|--help) grep -E '^#( |$)' "$0" | sed 's/^#\( \|$\)//'; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" || { echo "not in a git repo" >&2; exit 2; }
cd "$ROOT"
FILE="${FILE:-CHANGES-NETFLIX.txt}"

[[ -n "$TAG" ]] || { echo "no --tag given and \$ROCKET_TAG is unset" >&2; exit 2; }
[[ "$TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "tag '$TAG' is not vX.Y.Z; nothing to do" >&2; exit 0; }
[[ -f "$FILE" ]] || { echo "$FILE not found" >&2; exit 2; }
grep -qF "$MARKER" "$FILE" || { echo "marker not found in $FILE" >&2; exit 3; }

VER="${TAG#v}"
VER_RE="${VER//./\\.}"

# Idempotency: in CI never insert a section that already exists (this also breaks the
# commit-back -> rebuild loop). In --dry-run we still generate so you can preview it.
if grep -qE "^${VER_RE} \(" "$FILE"; then
  if [[ -n "$DRY_RUN" ]]; then
    echo "note: ${VER} section already exists; generating preview anyway (--dry-run)" >&2
  else
    echo "${VER} already present in $FILE; nothing to do"; exit 0
  fi
fi

# Resolve the range end. On a real tag build the tag exists; for a local preview of a
# not-yet-created tag, fall back to HEAD so you can see what the next release will contain.
if git rev-parse -q --verify "refs/tags/${TAG}" >/dev/null 2>&1; then
  END="$TAG"; PARENT="${TAG}^"
else
  echo "note: tag ${TAG} does not exist yet; previewing against HEAD" >&2
  END="HEAD"; PARENT="HEAD"
fi

# Range base: previous version tag (filtered to vX.Y.Z so unrelated tags like
# cassandra-5.0.6 are ignored).
if [[ -z "$PREV" ]]; then
  PREV="$(git describe --tags --abbrev=0 --match 'v[0-9]*.[0-9]*.[0-9]*' "$PARENT" 2>/dev/null)" \
    || { echo "could not determine the previous version tag before $TAG (pass --prev)" >&2; exit 3; }
fi

DATE="$(git log -1 --format=%cs "$END" 2>/dev/null)"
[[ "$DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || DATE="$(date +%F)"

echo "Generating changelog for ${VER}: range ${PREV}..${END} (date ${DATE})" >&2

# RAW bullets: PR-referenced commit subjects in range, newest-first, as changelog bullets.
RAW="$(git log --no-merges --pretty=format:'%s' "${PREV}..${END}" 2>/dev/null \
        | grep -E '\(#[0-9]+\)' | sed 's/^/  * /')"
[[ -n "$RAW" ]] || RAW="  * (no PR-referenced changes since ${PREV})"

# ----------------------------------------------------------------------------
# llm_polish: stdin = raw bullets, stdout = polished bullets (or raw on any
# problem). Reword/group only; reject the result if the PR-reference set changes.
# ----------------------------------------------------------------------------
llm_polish() {
  local raw; raw="$(cat)"
  if [[ -n "$NO_LLM" || -z "${GENAI_PROJECT_ID:-}" ]]; then printf '%s\n' "$raw"; return 0; fi
  if ! command -v metatron >/dev/null 2>&1; then
    echo "warn: metatron not found; using raw bullets" >&2; printf '%s\n' "$raw"; return 0
  fi

  local url model app rawf reqf respf out
  url="${GENAI_URL:-https://copilotdppython.vip.us-east-1.prod.cloud.netflix.net:7004/v1/chat/completions}"
  model="${GENAI_MODEL:-claude-3-5-sonnet}"
  app="${GENAI_METATRON_APP:-copilotdppython}"
  rawf="$(mktemp)"; reqf="$(mktemp)"; respf="$(mktemp)"
  printf '%s\n' "$raw" > "$rawf"

  GENAI_MODEL="$model" python3 - "$rawf" "$reqf" <<'PY'
import json, os, sys
raw = open(sys.argv[1]).read().strip()
system = (
    "You finalize the changelog for Netflix's internal fork of Apache Cassandra. "
    "Input: one changelog bullet per line, each ending with a PR reference like (#509). "
    "Reword each bullet to be concise and clear; you MAY group related bullets under a "
    "short subheading line ending in a colon (e.g. 'Backups & object store:'). "
    "STRICT RULES: (1) Every (#NNN) reference in the input must appear exactly once in the "
    "output, unchanged - never invent, drop, merge, or renumber a PR reference. "
    "(2) Output ONLY bullet lines (starting with '  * ') and optional subheading lines - "
    "no version header, no date, no preamble, no closing remarks."
)
body = {
    "model": os.environ.get("GENAI_MODEL", "claude-3-5-sonnet"),
    "temperature": 0,
    "messages": [
        {"role": "system", "content": system},
        {"role": "user", "content": raw},
    ],
}
open(sys.argv[2], "w").write(json.dumps(body))
PY

  # metatron curl performs the Metatron mTLS handshake plain curl can't.
  if ! metatron curl -a "$app" -X POST "$url" \
        -H "x-netflix-copilot-project-id: ${GENAI_PROJECT_ID}" \
        -H 'content-type: application/json' \
        --data-binary "@${reqf}" > "$respf" 2>/dev/null; then
    echo "warn: Model Gateway call failed; using raw bullets" >&2
    rm -f "$rawf" "$reqf" "$respf"; printf '%s\n' "$raw"; return 0
  fi

  out="$(python3 - "$respf" <<'PY'
import json, sys
try:
    d = json.load(open(sys.argv[1]))
    print(d["choices"][0]["message"]["content"].strip())
except Exception as e:
    sys.stderr.write("warn: could not parse Model Gateway response: %s\n" % e)
    sys.exit(1)
PY
)" || { rm -f "$rawf" "$reqf" "$respf"; printf '%s\n' "$raw"; return 0; }
  rm -f "$rawf" "$reqf" "$respf"

  # Validate: the set of PR references must be identical, else reject the LLM output.
  local rawset outset
  rawset="$(grep -oE '#[0-9]+' <<<"$raw" | sort -u)"
  outset="$(grep -oE '#[0-9]+' <<<"$out" | sort -u)"
  if [[ "$rawset" != "$outset" ]]; then
    echo "warn: LLM changed the PR-reference set; using raw bullets" >&2
    printf '%s\n' "$raw"; return 0
  fi
  printf '%s\n' "$out"
}

BULLETS="$(printf '%s\n' "$RAW" | llm_polish)"
BLOCK="$(printf '%s (%s)\n%s' "$VER" "$DATE" "$BULLETS")"

# ----------------------------------------------------------------------------
# render: insert $BLOCK on its own paragraph immediately after the marker line
# of the given file; spliced content goes to stdout.
# ----------------------------------------------------------------------------
render() {
  local src="$1" blockf
  blockf="$(mktemp)"; printf '%s\n' "$BLOCK" > "$blockf"
  python3 - "$src" "$MARKER" "$blockf" <<'PY'
import sys
path, marker, blockf = sys.argv[1], sys.argv[2], sys.argv[3]
block = open(blockf).read().rstrip("\n")
lines = open(path).read().split("\n")
idx = next((k for k, l in enumerate(lines) if l.strip() == marker.strip()), None)
if idx is None:
    sys.stderr.write("marker not found in %s\n" % path); sys.exit(3)
out = lines[:idx + 1] + [""] + block.split("\n") + lines[idx + 1:]
sys.stdout.write("\n".join(out))
PY
  local rc=$?
  rm -f "$blockf"
  return $rc
}

# --- dry run: show the section + diff, change nothing ----------------------
if [[ -n "$DRY_RUN" ]]; then
  tmp="$(mktemp)"
  render "$FILE" > "$tmp" || { echo "render failed" >&2; rm -f "$tmp"; exit 3; }
  echo "===== generated section for ${VER} ====="
  printf '%s\n' "$BLOCK"
  echo "===== unified diff (NOT applied) ====="
  diff -u "$FILE" "$tmp"
  rm -f "$tmp"
  exit 0
fi

# --- write in place, optionally without committing -------------------------
if [[ -n "$NO_PR" ]]; then
  tmp="$(mktemp)"
  render "$FILE" > "$tmp" || { echo "render failed" >&2; rm -f "$tmp"; exit 3; }
  cp "$tmp" "$FILE"; rm -f "$tmp"
  echo "Updated $FILE with the ${VER} section (--no-pr: not committing)."
  exit 0
fi

# --- CI path: branch from the release base, splice, commit, push, open PR --
BASE="${CHANGELOG_PR_BASE:-cassandra-$(cut -d. -f1-2 <<<"$VER")}"
BR="chore/changelog-${VER}"

git fetch --quiet origin "$BASE" 2>/dev/null || echo "warn: could not fetch origin/$BASE" >&2
git checkout -B "$BR" "origin/$BASE" 2>/dev/null || git checkout -B "$BR" "$BASE" \
  || { echo "could not check out base branch $BASE" >&2; exit 3; }

# Re-check idempotency against the base branch's copy of the file.
if grep -qE "^${VER_RE} \(" "$FILE"; then
  echo "${VER} already present on $BASE; nothing to do"; exit 0
fi

tmp="$(mktemp)"
render "$FILE" > "$tmp" || { echo "render failed" >&2; rm -f "$tmp"; exit 3; }
cp "$tmp" "$FILE"; rm -f "$tmp"

git config user.email "${GIT_AUTHOR_EMAIL:-jenkins@netflix.com}"
git config user.name  "${GIT_AUTHOR_NAME:-Rocket CI}"
git add "$FILE"
git commit -m "Netflix Changelog ${VER} [skip ci]"
git push -f origin "$BR"

if command -v gh >/dev/null 2>&1; then
  gh pr create --base "$BASE" --head "$BR" \
    --title "Netflix Changelog ${VER}" \
    --body "Auto-generated from \`${PREV}..${TAG}\`. Review/adjust bullet wording and grouping before merging." \
    || echo "warn: gh pr create failed; branch ${BR} is pushed - open the PR manually" >&2
else
  echo "gh not found; branch ${BR} is pushed - open a PR manually." >&2
fi
