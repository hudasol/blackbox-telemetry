# Retrospective

Written after the build, honestly — including things `docs/PROCESS_LOG.md`
already recorded in passing, pulled together here as the direct answer to
"what went wrong, what would you do differently." Nothing here is invented
for effect; every item is traceable to a `PROCESS_LOG.md` entry or to this
session's own acceptance-check audits, cited inline.

## 1. Real mistakes, and the bad assumption behind each one

**The simulator was bundled into the product's own startup command.**
`docs/PLAN.md` said, from the very first commit, that the simulator is
"separate test equipment, not part of the product." The Docker Compose
step built `docker-compose.yml` anyway so that a single `docker compose up
--build` started postgres, redis, api, worker, *and* the simulator
together — a deliberate choice, made explicitly so a bare `docker compose
up` would "demonstrate every documented failure path... without any extra
configuration." That reasoning wasn't wrong on its own terms, but it
traded away the separation the plan had already promised, and nothing
caught it: the Docker Compose step's own acceptance bar was "does `docker
compose up --build` bring up a working system with visible traffic,"
which it honestly did. The bar just never asked "should *only* the
product come up." It stood, uncaught, through the Tests, CI, and README
steps, until an explicit acceptance check ("one command brings the system
up, a separate command starts the simulators") forced the question. Fixed
with a Compose profile (`profiles: ["simulator"]`) so the two commands are
now real. The lesson isn't "profiles are the fix" — it's that a step's own
self-defined acceptance bar can be satisfied while quietly violating a
constraint written three steps earlier, and nothing short of re-checking
against the original plan will catch that.

**Pushes silently diverged from what was tested, more than once.**
This sandbox's raw `git push` is rejected by the egress proxy, so every
push in this build went through the GitHub app's `push_files` call
instead of `git push` — a separate path from the local commit history,
with its own chance to send stale or truncated content. It happened
twice: the README step's push sent a truncated copy of `PROCESS_LOG.md`
(cut off mid-entry), caught the same step by rereading the pushed file;
and, once that raised the question of whether anything *earlier* had the
same problem, a full SHA-by-SHA audit (not a spot check) found
`services/worker/worker/db.py`, `processing.py`, and `redis_stream.py`
had been out of sync with GitHub since the original Worker step commit —
docstrings and comments added locally before that commit never made it
into the push. Diffed line by line before concluding anything: every SQL
statement, function body, and return value was identical, so nothing in
production ever ran different code than what was tested — but "what's in
the repo I reviewed" and "what's actually on GitHub" had quietly split
apart, twice, and only a full-file audit (not spot-checking) surfaced the
second case. The standing rule since: never reconstruct a file's content
for a push from memory, always read it fresh immediately before that
specific push call.

**The worker's consumer-group self-heal gap was found by accident, not
by design.** `ensure_consumer_group` only ever ran once, at worker
startup. An unrelated `pytest` run sharing the same throwaway Redis
instance called `flushdb` mid-demo, deleting the stream and consumer
group out from under a *running* worker — and exposed that a `NOGROUP`
error retried into the bounded backoff forever and never actually
recovered, even once Redis was healthy again. This was a real product
gap (a Redis restart without persistence, or an operator flush, would
have wedged the worker in production), and it was caught by a testing
accident, not by anyone deliberately designing for "what if the stream
disappears mid-run." Fixed and covered by a dedicated test afterward, but
worth being honest that the discovery was luck, not process.

**`redis-py`'s client-side socket timeout is independent of the `BLOCK`
argument, and nothing in the integration tests could have caught it.**
Passing `XREADGROUP ... BLOCK 5000` does not, by itself, make the client
wait 5 seconds — `redis-py` has its own socket-level read timeout,
defaulting to 5s regardless of `BLOCK`, and it fires as a plain
`TimeoutError` that looks exactly like Redis being down. This would have
made the worker crash-loop constantly against any real, occasionally-idle
deployment. No integration test caught it because every test payload is
available immediately — the blocking-wait code path never actually
blocked long enough to hit the bug. Only running real, idle processes and
watching them crash surfaced it. The lesson: a test suite that never lets
a blocking call actually block cannot prove the blocking path works.

**A global exception handler was over-scoped.** The 422 handler hard-coded
`"error": "invalid_telemetry_event"` for every validation failure,
written under the assumption that only the ingest endpoint could ever
422. Once the query endpoints' `history` route gained its own `?limit=`
validation, a bad query parameter there got the same misleading ingest
label. Caught by rereading the code, not by a test — every existing 422
test happened to hit the ingest path.

**The local Python version would have silently invalidated a CI proof.**
This sandbox's bare `python3` is 3.11; the CI workflow targets 3.12.
Verifying the CI job locally with the wrong interpreter would have
"passed" while proving nothing about what the workflow actually runs —
caught only because pip refused every package with "requires a different
Python" on the first attempt, not because it was checked for up front.

**A demo command in the README was wrong the moment it was written.** The
original curl example hard-coded `timestamp_utc` to a fixed value; run
later, it failed the contract's validation. A fixed timestamp is either
already wrong by the time someone runs the example, or will be soon —
fixed by generating it inline (`$(date -u +%Y-%m-%dT%H:%M:%SZ)`) so the
example is correct whenever it's actually run, not just on the day it was
written.

**A query-string encoding bug shipped in test code, not product code,**
while adding `from`/`to` filtering: the first version of the new tests
hand-built the request URL with an f-string embedding `.isoformat()`
datetimes directly. A UTC offset like `+00:00` contains a literal `+`,
which standard form-urlencoded decoding reads back as a space, silently
corrupting the timestamp before it reached the parser. 5 of 7 new tests
failed with unexpected 422s, including one that had been "passing" for
the wrong reason (any 422, not the intended cross-field check). Caught by
investigating the failures rather than accepting green as good enough on
a first pass, and fixed by letting `httpx` do the percent-encoding
instead of hand-building the URL.

**A whole sandbox environment problem was invisible for the first several
build steps.** This sandbox's Docker daemon couldn't start on the day of
the Ingest API step, so local testing through the Query API step used
`redis-server`/`postgresql` directly instead of containers — a real
environmental constraint, correctly documented at the time, but it meant
"one-command reproducibility" (the actual product requirement) went
unverified through Docker for several steps until the Docker Compose step
finally proved it live. A second, related problem persisted afterward:
this sandbox's TLS-intercepting proxy means a plain `docker build` inside
it can't reach PyPI at all, so a genuine from-scratch image build could
only be verified here manually, with a workaround, a few times — not
automatically, and not on every change — until the `docker-smoke` CI job
was added specifically to close that gap on GitHub's normal-network
runners.

## 2. Redesigns

There weren't many, and that's itself worth stating plainly rather than
implying more churn happened than actually did. `docs/PLAN.md`'s five
core decisions (D1–D5 — seq as ordering authority, dedup at Postgres not
ingest, online/offline computed at read time, one `api` service for
ingest+query, Redis Streams over pub/sub) were made before any
implementation code and never changed. The only actual architectural
change across the whole build was the Docker Compose profile split
described above — everything else was bug fixes, coverage gaps, or
process/verification gaps, not a rethink of the system's shape. Whether
that means the upfront design was right, or that too little was
questioned once code existed, is a fair thing to ask; this retrospective
doesn't resolve that for you, but it's flagging it rather than letting
"no redesigns" quietly read as "no reason to have expected any."

## 3. Technical lessons worth carrying into the next build

- A command's own blocking argument (`BLOCK` on `XREADGROUP`) can be
  overridden by a client library's independent default — check the
  client's own timeout behavior, don't assume the wire protocol's option
  is the only one in play.
- `pip install --isolated`'s build-dependency subprocess ignores
  proxy/cert environment variables regardless of what the outer process
  has configured — a container build behind a TLS-intercepting proxy
  needs an explicit `--cert`/`--no-build-isolation` accommodation, and
  that accommodation should never leak into the committed Dockerfile if
  it only exists to work around one sandbox's network.
- A global exception handler that assumes it has one caller will
  eventually get a second one; scope the response by what's actually in
  the request (`loc[0] == "body"`), not by the exception type alone.
- Query strings need real percent-encoding, always — a `+` in a UTC
  offset is a specific, easy-to-miss case of "special characters in query
  parameters," not a one-off fluke.
- Full-file SHA comparison catches what spot-checking a few files misses;
  the cost of comparing all of them is small compared to the cost of a
  silent drift sitting undetected for several build steps.
- An automated proof that goes stale the moment something changes (one
  manual `docker compose up --build`, run once, by hand) is worth much
  less than the same proof running on every push — the value isn't in
  having proven it once, it's in nothing being able to quietly break it
  afterward.
- A step's own acceptance bar is not the same thing as the plan's actual
  requirements — re-check against the plan itself, not just against what
  that step decided to test for.

## 4. What my own pre-release checks missed, honestly

- The Docker Compose step's live verification proved the system comes up
  and produces traffic — it never explicitly tested "does *only* the
  product come up," so the simulator-bundling problem passed every check
  that existed until a tighter, explicitly-stated acceptance bar was
  applied from outside the build steps themselves.
- Push-integrity checking was reactive, not proactive: the first
  full-repo SHA audit only happened after a truncated push was noticed by
  chance, meaning the three worker-file drifts had already been sitting
  unnoticed on GitHub since the original Worker step commit.
- There was no automated, on-every-push proof that a genuine
  from-scratch `docker compose up --build` still works, for most of the
  build — only a manual, sandbox-workaround-assisted proof a few steps
  earlier that could have silently gone stale. This was closed by the
  `docker-smoke` CI job, but only after being explicitly asked to check
  it, not as a step that flagged itself as missing.
- This file didn't exist until asked for directly, and no git tag exists
  yet either — both are things a "what does a finished, shippable
  project look like" self-check should have surfaced on its own, and
  didn't.
- A few numeric defaults (the backoff schedule's `0.5s`/`30s`/`2x`, the
  `10s` asset-offline timeout) are reasonable but not independently
  argued for in the repo beyond "it's configurable, not a buried magic
  number" — flagged in a later audit, not resolved.
- Nothing in this build ever independently confirmed what GitHub Actions
  actually reports for a given commit through an API call — every CI
  claim was verified by re-running the same steps locally and reasoning
  that GitHub's clean-network runners should behave at least as well.
  That's reasonable indirect evidence, not the same thing as checking the
  Actions tab itself.
