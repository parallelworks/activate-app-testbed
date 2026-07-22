#!/usr/bin/env python3
"""Launch testbed workers through the generic workflow using the pw CLI.

Builds the workflow inputs, submits the run with `pw workflows run`, and
follows its status and logs until the dispatch finishes. The server is
deployed if not already running (idempotent); pass --no-deploy-server
for a workers-only run against a live server.

Examples:
  # server plus one worker on another cluster
  ./launch-worker.py --server-host clusterA --site clusterB

  # workers on two sites, one as a SLURM batch job
  ./launch-worker.py --server-host clusterA --site clusterB \
      --site clusterC --scheduler --partition debug --walltime 00:30:00

  # verify a pw CLI release passes websocket upgrades through pw forward
  ./launch-worker.py --server-host clusterA --site clusterB \
      --tunnel-method pw-forward

Targets the published "app-testbed" workflow by default; publish local
changes with `pw workflows update --yaml workflow.yaml app-testbed`, or
pass --workflow ./workflow.yaml to run the local file directly.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))


class Tee:
    """Duplicate a stream into a log file."""

    def __init__(self, stream, fh):
        self.stream, self.fh = stream, fh

    def write(self, data):
        self.stream.write(data)
        self.fh.write(data)
        self.fh.flush()

    def flush(self):
        self.stream.flush()
        self.fh.flush()


def pw(args, ctx, check=True, timeout=120):
    cmd = ["pw"] + (["--context", ctx] if ctx else []) + args
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and p.returncode != 0:
        sys.exit("pw %s failed:\n%s" % (" ".join(args), (p.stderr or p.stdout).strip()))
    return p.stdout


def whoami(ctx):
    out = pw(["auth", "whoami"], ctx).strip().splitlines()
    return out[0].strip() if out else ""


def resolve_resource(ident, ctx, me, cache={}):
    """Map a cluster name or pw:// URI to a computeResource input object."""
    if "items" not in cache:
        out = pw(["cluster", "ls", "-o", "json"], ctx)
        try:
            items = json.loads(out)
        except json.JSONDecodeError:
            sys.exit("could not parse `pw cluster ls -o json` output")
        if isinstance(items, dict):
            items = items.get("clusters") or items.get("items") or []
        cache["items"] = items
    items = cache["items"]
    want_ns, want_name = None, ident
    m = re.match(r"pw://([^/]+)/(.+)$", ident)
    if m:
        want_ns, want_name = m.group(1), m.group(2)
    for c in items:
        name = c.get("name") or ""
        ns = c.get("namespace") or (c.get("uri") or "").replace("pw://", "").split("/")[0] or None
        if name == want_name and (want_ns is None or ns in (None, want_ns)):
            ns = ns or want_ns or me
            ctype = c.get("type") or c.get("provider") or ""
            sched = c.get("schedulerType") or ("slurm" if "slurm" in ctype else "pbs" if "pbs" in ctype else "")
            return {
                "$type": "computeResource",
                "id": c.get("id", ""),
                "ip": c.get("ip") or c.get("ipAddress") or "",
                "name": name,
                "namespace": ns,
                "provider": ctype,
                "schedulerType": sched,
                "type": ctype,
                "uri": c.get("uri") or "pw://%s/%s" % (ns, name),
                "user": c.get("user") or ns,
            }
    names = sorted({c.get("name", "?") for c in items})
    sys.exit("cluster %r not found; available: %s" % (ident, ", ".join(names)))


def pbs_directives(a):
    """Verbatim #PBS lines for the workflow's directives editor input."""
    if a.pbs_directives:
        return a.pbs_directives
    lines = ["#PBS -q %s" % (a.partition or "debug"),
             "#PBS -l select=1:ncpus=1",
             "#PBS -l walltime=%s" % a.walltime]
    if a.extra:
        lines.append(a.extra if a.extra.lstrip().startswith("#PBS") else "#PBS " + a.extra)
    return "\n".join(lines) + "\n"


def build_inputs(a, server_res, site_resources):
    workers = []
    for res in site_resources:
        workers.append({
            "resource": res,
            "worker_name": a.worker_name if len(site_resources) == 1 else "",
            "workdir": a.worker_workdir,
            "scheduler": a.scheduler,
            "slurm": {"partition": a.partition, "account": a.account,
                      "time": a.walltime, "extra": a.extra},
            "pbs": {"account": a.account,
                    "scheduler_directives": pbs_directives(a)},
        })
    return {
        "server": {"resource": server_res},
        "services": {"deploy_server": a.deploy_server, "restart_server": a.restart},
        "app": {"workdir": a.workdir, "subdomain": a.subdomain,
                "server_port": a.port, "tunnel_method": a.tunnel_method,
                "wait_for_workers": a.wait_workers},
        "workers": workers,
    }


def submit(a, inputs):
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(inputs, f)
        path = f.name
    args = ["workflows", "run", "-o", "json", "-i", path]
    if a.dry_run:
        args.append("--dry-run")
    args.append(a.workflow)
    out = pw(args, a.context, timeout=180)
    os.unlink(path)
    try:
        run = json.loads(out)
        run = run.get("run", run)
        return run.get("slug")
    except json.JSONDecodeError:
        m = re.search(r"Slug:\s*(\S+)", out)
        return m.group(1) if m else None


def run_status(slug, ctx):
    out = pw(["workflows", "runs", "view", slug, "-o", "json"], ctx, check=False)
    try:
        return json.loads(out).get("status", "")
    except json.JSONDecodeError:
        return ""


SECTION = re.compile(r"^=== job: (.+?) > step: (.+?) \(.*\) ===$")


def log_sections(logs):
    """Split `pw workflows runs logs` output into (job > step, content)."""
    order, sections, key = [], {}, None
    for line in logs.splitlines():
        m = SECTION.match(line)
        if m:
            key = "%s > %s" % (m.group(1), m.group(2))
            if key not in sections:
                order.append(key)
                sections[key] = []
            continue
        if key:
            sections[key].append(line)
    return [(k, "\n".join(sections[k]).strip("\n")) for k in order]


def watch(a, slug):
    print("watching run %s (poll %ds, timeout %d min)" % (slug, a.poll, a.timeout_min))
    seen = {}

    def emit(logs):
        for key, content in log_sections(logs):
            # a step that has not started yet shows a fetch-error
            # placeholder that is later replaced with the real log;
            # hold those back instead of streaming them
            if not content or "(error fetching logs:" in content:
                continue
            prev = seen.get(key)
            if content == prev:
                continue
            if prev and content.startswith(prev):
                delta = content[len(prev):].lstrip("\n")
                if delta:
                    print(delta)
            else:
                print("\n--- %s ---" % key)
                if content:
                    print(content)
            seen[key] = content

    deadline = time.time() + a.timeout_min * 60
    status = "running"
    while time.time() < deadline:
        emit(pw(["workflows", "runs", "logs", slug], a.context, check=False, timeout=180))
        status = run_status(slug, a.context)
        if status and status not in ("running", "pending", "queued"):
            break
        time.sleep(a.poll)
    emit(pw(["workflows", "runs", "logs", slug], a.context, check=False, timeout=180))
    if status not in ("completed", "running", "pending", "queued", ""):
        err = pw(["workflows", "runs", "errors", slug], a.context, check=False)
        if err.strip():
            print("\n===== run errors =====")
            print(err.strip())
    print("\nrun %s finished: %s" % (slug, status or "unknown"))
    return 0 if status == "completed" else 1


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--server-host", required=True, help="cluster running (or to run) the server")
    p.add_argument("--site", action="append", required=True,
                   help="cluster to run a worker on (repeatable; name or pw:// URI)")
    p.add_argument("--worker-name", default="",
                   help="blank = worker-<hostname>; ignored with multiple sites")
    p.add_argument("--worker-workdir", default="~/app-testbed-worker")
    p.add_argument("--scheduler", action="store_true", help="submit workers as batch jobs")
    p.add_argument("--partition", default="", help="SLURM partition / PBS queue")
    p.add_argument("--account", default="")
    p.add_argument("--walltime", default="01:00:00")
    p.add_argument("--extra", default="", help="extra sbatch args / extra #PBS directive")
    p.add_argument("--pbs-directives", default="",
                   help="verbatim #PBS lines for PBS sites "
                        "(default: built from --partition/--walltime/--account)")
    p.add_argument("--no-deploy-server", dest="deploy_server", action="store_false", default=True)
    p.add_argument("--restart", action="store_true", help="restart the server if already running")
    p.add_argument("--no-wait", dest="wait_workers", action="store_false", default=True,
                   help="do not block the run until workers connect")
    p.add_argument("--workdir", default="~/app-testbed")
    p.add_argument("--subdomain", default="apptest")
    p.add_argument("--port", type=int, default=8090)
    p.add_argument("--tunnel-method", choices=["auto", "ssh", "pw-forward"], default="auto",
                   help="pw-forward verifies whether a pw CLI release passes websocket upgrades")
    p.add_argument("--workflow", default="app-testbed",
                   help="published workflow name or local yaml path")
    p.add_argument("--context", default=os.environ.get("PW_CONTEXT", ""),
                   help="pw CLI context (default: current)")
    p.add_argument("--no-watch", dest="watch", action="store_false", default=True)
    p.add_argument("--poll", type=int, default=15, help="watch poll interval seconds")
    p.add_argument("--timeout-min", type=int, default=45)
    p.add_argument("--dry-run", action="store_true", help="validate without executing")
    p.add_argument("--print-inputs", action="store_true", help="print inputs JSON and exit")
    p.add_argument("--log", default="launch-worker.log",
                   help="also write all output to this file ('' disables)")
    a = p.parse_args()

    if a.log:
        fh = open(a.log, "w")
        sys.stdout = Tee(sys.stdout, fh)
        sys.stderr = Tee(sys.stderr, fh)
        print("logging to %s" % os.path.abspath(a.log))

    me = whoami(a.context)
    server_res = resolve_resource(a.server_host, a.context, me)
    site_resources = [resolve_resource(s, a.context, me) for s in a.site]
    inputs = build_inputs(a, server_res, site_resources)
    if a.print_inputs:
        print(json.dumps(inputs, indent=2))
        return 0

    slug = submit(a, inputs)
    if not slug:
        sys.exit("could not determine run slug from pw output")
    if a.dry_run:
        print("dry run validated: %s" % slug)
        return 0
    print("launched run: %s (%d worker site(s), scheduler=%s, tunnel=%s)"
          % (slug, len(site_resources), a.scheduler, a.tunnel_method))
    if not a.watch:
        print("follow with: pw %sworkflows runs logs %s"
              % (("--context %s " % a.context) if a.context else "", slug))
        return 0
    return watch(a, slug)


if __name__ == "__main__":
    sys.exit(main())
