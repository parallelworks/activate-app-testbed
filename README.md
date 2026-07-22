# Multi-site client-server deployment pattern

A reusable ACTIVATE workflow for deploying a service on one cluster and
attaching workers on other clusters to it. It is a pattern template, not
an application: the deployment mechanics are the deliverable, and the
embedded server/worker are minimal placeholders you replace with your
own service.

The mechanics it demonstrates:

- a service exposed to the browser through an endpoint session
- workers dispatched to other clusters over `pw ssh`
- cross-site connectivity through SSH tunnels, with a WebSocket-upgrade
  probe that detects and recreates a stale tunnel
- workers on scheduled resources (SLURM/PBS), with the tunnel on the
  submit node and a supervised, streamed connect
- cross-namespace resource addressing (`pw://owner/name` URIs), so the
  server and worker clusters can be owned by, or shared from, different
  users

The placeholder server and worker are small stdlib-only Python scripts,
so the workflow runs on any cluster with `python3` and the pw CLI with
no staged files.

## Publishing

The workflow is published as the marketplace item `app-testbed`, managed
from this repository:

```bash
pw marketplace publish --name "Multi-site App Testbed" --slug app-testbed \
    --repo <this repo URL> --branch main \
    --workflow-yaml workflow.yaml --thumbnail thumbnail.png --readme README.md
```

Users add it to their account with `pw marketplace add-to-account
app-testbed`, which creates a workflow reference named after the added
version (e.g. `marketplace.app-testbed.v1.0`).
`scripts/programmatic/launch-worker.py` targets that reference by
default; pass `--workflow ./workflow.yaml` to run the local file
without publishing.

## Manual launch from a static inputs file

`scripts/manual/worker-inputs.json` is a template inputs file for running the
testbed with the pw CLI alone. Fill in the `resource` blocks for your
clusters (or regenerate the file with
`scripts/programmatic/launch-worker.py --server-host <cluster> --site <cluster> --print-inputs`),
then:

```bash
scripts/manual/run-worker.sh                # uses scripts/manual/worker-inputs.json
scripts/manual/run-worker.sh my-inputs.json # or an explicit file
```

The script submits the run, follows it to completion, and prints the
dispatch log; it refuses to launch while the template placeholders are
still present.

## Programmatic launch

`scripts/programmatic/launch-worker.py` submits a run through the pw CLI and follows it to
completion, so the testbed can be driven from scripts or CI:

```bash
# server plus one worker on another cluster
scripts/programmatic/launch-worker.py --server-host clusterA --site clusterB

# workers on two sites, submitted as SLURM batch jobs
scripts/programmatic/launch-worker.py --server-host clusterA --site clusterB --site clusterC \
    --scheduler --partition debug --walltime 00:30:00

# verify a pw CLI release passes websocket upgrades through pw forward:
# exit code 0 means workers connected through the tunnel
scripts/programmatic/launch-worker.py --server-host clusterA --site clusterB \
    --tunnel-method pw-forward
```

The script resolves cluster names (or `pw://owner/name` URIs) to full
resource objects via `pw cluster ls -o json`, submits with
`pw workflows run`, and streams the run log — including the dispatch
step's live queue states and worker-connect confirmations. Exit code 0
means the run completed. `--no-deploy-server` runs workers-only against
a live server, `--no-wait` finishes the run at submission instead of
blocking until workers connect (keep the default blocking behavior for
tunnel verification runs), `--dry-run` validates inputs without executing,
`--print-inputs` shows the generated JSON, and `--no-watch` returns
immediately after submission. Everything printed is also written to
`launch-worker.log` in the current directory (`--log` changes the path,
`--log ''` disables it). On PBS sites `--partition` maps to the queue
and `--pbs-directives` replaces the generated `#PBS` lines entirely.
Requires an authenticated pw CLI.

## Adapting to your application

Replace two heredocs and nothing else:

- the `server.py` heredoc in `start_server` with a command that starts
  your service on `127.0.0.1:<port>` (expose an HTTP health path and,
  if your workers hold a long-lived connection, a WebSocket path)
- the `worker.py` heredoc in `dispatch_workers` with your worker client,
  and adjust the two `WebSocket Connected` markers the supervisor greps
  for to match a line your client prints on success

The endpoint session, tunnels, dispatch loop, scheduler submission,
queue watch, and supervised restart are the pattern and stay as-is.

```
browser --> endpoint session --> server (:8090)
                                   /health /workers /register /ws

worker sites (0..N):  worker --> server via SSH tunnel
                      (direct on the server host; scheduled workers reach
                       a tunnel on the submit node over the cluster fabric)
```

The server tracks workers: registration is an HTTP POST, and live status
comes from a WebSocket each worker holds open. `/workers` (visible
through the endpoint session) shows `connected: true/false` per worker.
This mirrors the register-then-connect shape of real agent systems, so
tunnel or scheduler problems reproduce here with the same symptoms.

## Inputs

- **Server host** - cluster for the server and its endpoint session.
- **Services** - `Deploy server` (off = workers-only run against a live
  server) and `Restart server if already running`.
- **Settings** - workdir, session subdomain, server port, and
  **Tunnel method** (`auto` | `ssh` | `pw-forward`):
  - `auto`: plain ssh via the platform proxy command when `~/.ssh/pwcli`
    exists, otherwise `pw forward`.
  - `ssh`: always plain ssh.
  - `pw-forward`: always `pw forward`. Use this to verify whether a pw
    CLI release passes WebSocket upgrades: the dispatch probes the
    upgrade path before trusting a tunnel and the run fails if upgrades
    do not traverse, so a green run with this setting is a positive
    verification.
- **Worker sites** - a list; one worker per entry. Each entry has an
  optional name (blank = `worker-<hostname>`), a working directory, and
  a **Submit via scheduler?** toggle with per-scheduler directive
  groups shown based on the resource's scheduler type: partition/account
  pickers, walltime and extra args for SLURM; an account string (`-A`)
  and a free-form directives editor for PBS, whose `#PBS` lines are
  embedded verbatim in the batch script (default: debug queue, short
  walltime).

## How workers connect

- On the **server host**: directly via localhost, no tunnel.
- On a **remote site (login node)**: the dispatch opens a persistent SSH
  tunnel from the site to the server host, probes both HTTP and the
  WebSocket upgrade path through it (recreating the tunnel if the
  upgrade probe times out), then starts the worker. The worker is
  restarted up to 3 times until its `WebSocket Connected` line appears.
- On a **scheduled resource**: the tunnel terminates on the submit/login
  node, bound to its cluster-internal IP, and the batch job connects to
  `http://<submit-node-ip>:<port>` over the cluster fabric. Compute
  nodes need no pw CLI and no outbound network access. The job runs the
  worker in the foreground (the allocation lives as long as the worker)
  and restarts it up to 5 times until it connects. The dispatch step
  polls the queue and the job log, streaming state transitions
  (`PENDING` -> `RUNNING` -> connected) into the run log, and fails the
  run if the worker never connects.

Re-runs are idempotent: a running server or worker is detected and left
alone, and a queued or running worker batch job is not resubmitted
(canceled jobs in transient states such as SLURM `CG` do not block).

## Verifying a pw CLI tunnel fix

1. Set **Tunnel method** to `pw-forward`.
2. Add a remote worker site (optionally with the scheduler enabled).
3. Run. The dispatch fails with "tunnel fails the websocket probe" or
   "server unreachable (or WS blocked)" if upgrades do not pass; a run
   that ends with `worker connected` confirms the tunnel carries
   WebSocket traffic end to end.

## Files on the hosts

- Server host workdir: `server.py`, `server.log`, `endpoint.log`.
- Worker site workdir: `worker.py`, `worker.log` (login-node workers),
  `worker-job.sh` and `worker-job.log` (scheduled workers),
  `tunnel.log`.
