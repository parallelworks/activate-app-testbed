#!/usr/bin/env python3
"""Launch testbed workers through the generic workflow using the pw CLI.

Submits a run (server deployed if not already running; --no-deploy-server
for workers-only) and follows it until it finishes. Targets the
marketplace workflow (add it with `pw marketplace add-to-account
app-testbed`); pass --workflow ./workflow.yaml for the local file.

Examples:
  ./launch-worker.py --server-host clusterA --site clusterB
  ./launch-worker.py --server-host clusterA --site clusterB --site clusterC \\
      --scheduler --partition debug --walltime 00:30:00
  ./launch-worker.py --server-host clusterA --site clusterB --tunnel-method pw-forward
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time

SECTION = re.compile(r"^=== job: (.+?) > step: (.+?) \(.*\) ===$")


class Tee:
    def __init__(self, stream, fh):
        self.stream, self.fh = stream, fh

    def write(self, d):
        self.stream.write(d)
        self.fh.write(d)
        self.fh.flush()

    def flush(self):
        self.stream.flush()


def pw(args, ctx, check=True, timeout=180):
    cmd = ["pw"] + (["--context", ctx] if ctx else []) + args
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and p.returncode != 0:
        sys.exit("pw %s failed:\n%s" % (" ".join(args), (p.stderr or p.stdout).strip()))
    return p.stdout


def resolve(ident, ctx, me, cache={}):
    """Cluster name or pw://owner/name URI -> computeResource object."""
    if "items" not in cache:
        try:
            cache["items"] = json.loads(pw(["cluster", "ls", "-o", "json"], ctx))
        except json.JSONDecodeError:
            sys.exit("could not parse `pw cluster ls -o json` output")
    items = cache["items"] if isinstance(cache["items"], list) else []
    m = re.match(r"pw://([^/]+)/(.+)$", ident)
    want_ns, want = (m.group(1), m.group(2)) if m else (None, ident)
    for c in items:
        ns = c.get("namespace") or (c.get("uri") or "").replace("pw://", "").split("/")[0] or None
        if c.get("name") == want and (want_ns is None or ns in (None, want_ns)):
            ns = ns or want_ns or me
            t = c.get("type") or c.get("provider") or ""
            sched = c.get("schedulerType") or ("slurm" if "slurm" in t else "pbs" if "pbs" in t else "")
            return {"$type": "computeResource", "id": c.get("id", ""),
                    "ip": c.get("ip") or c.get("ipAddress") or "", "name": want,
                    "namespace": ns, "provider": t, "schedulerType": sched, "type": t,
                    "uri": c.get("uri") or "pw://%s/%s" % (ns, want), "user": c.get("user") or ns}
    names = ", ".join(sorted({c.get("name", "?") for c in items}))
    sys.exit("cluster %r not found; available: %s" % (ident, names))


def pbs_directives(a):
    if a.pbs_directives:
        return a.pbs_directives
    lines = ["#PBS -q %s" % (a.partition or "debug"),
             "#PBS -l select=1:ncpus=1",
             "#PBS -l walltime=%s" % a.walltime]
    if a.extra:
        lines.append(a.extra if a.extra.lstrip().startswith("#PBS") else "#PBS " + a.extra)
    return "\n".join(lines) + "\n"


def build_inputs(a, server_res, site_resources):
    workers = [{"resource": res,
                "worker_name": a.worker_name if len(site_resources) == 1 else "",
                "workdir": a.worker_workdir, "scheduler": a.scheduler,
                "slurm": {"partition": a.partition, "account": a.account,
                          "time": a.walltime, "extra": a.extra},
                "pbs": {"account": a.account,
                        "scheduler_directives": pbs_directives(a)}}
               for res in site_resources]
    return {
        "server": {"resource": server_res},
        "services": {"deploy_server": a.deploy_server, "restart_server": a.restart},
        "app": {"workdir": a.workdir, "subdomain": a.subdomain, "server_port": a.port,
                "tunnel_method": a.tunnel_method, "wait_for_workers": a.wait_workers},
        "workers": workers,
    }


def submit(a, inputs):
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(inputs, f)
    args = ["workflows", "run", "-o", "json", "-i", f.name] + \
        (["--dry-run"] if a.dry_run else []) + [a.workflow]
    out = pw(args, a.context)
    os.unlink(f.name)
    try:
        run = json.loads(out)
        return run.get("run", run).get("slug")
    except json.JSONDecodeError:
        return None


def log_sections(logs):
    """Split `pw workflows runs logs` output into (job > step, content)."""
    order, sections, key = [], {}, None
    for line in logs.splitlines():
        m = SECTION.match(line)
        if m:
            key = "%s > %s" % m.groups()
            if key not in sections:
                order.append(key)
                sections[key] = []
        elif key:
            sections[key].append(line)
    return [(k, "\n".join(sections[k]).strip("\n")) for k in order]


def watch(a, slug):
    print("watching run %s (poll %ds, timeout %d min)" % (slug, a.poll, a.timeout_min))
    seen = {}

    def emit():
        for key, content in log_sections(pw(["workflows", "runs", "logs", slug], a.context, check=False)):
            prev = seen.get(key)
            # unstarted steps show a fetch-error placeholder that is
            # replaced with the real log later; hold those back
            if not content or "(error fetching logs:" in content or content == prev:
                continue
            if prev and content.startswith(prev):
                delta = content[len(prev):].lstrip("\n")
                if delta:
                    print(delta)
            else:
                print("\n--- %s ---\n%s" % (key, content))
            seen[key] = content

    def status():
        try:
            return json.loads(pw(["workflows", "runs", "view", slug, "-o", "json"],
                                 a.context, check=False)).get("status", "")
        except json.JSONDecodeError:
            return ""

    st, deadline = "running", time.time() + a.timeout_min * 60
    while time.time() < deadline:
        emit()
        st = status()
        if st and st not in ("running", "pending", "queued"):
            break
        time.sleep(a.poll)
    emit()
    if st not in ("completed", "running", "pending", "queued", ""):
        err = pw(["workflows", "runs", "errors", slug], a.context, check=False).strip()
        if err:
            print("\n===== run errors =====\n" + err)
    print("\nrun %s finished: %s" % (slug, st or "unknown"))
    return 0 if st == "completed" else 1


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    arg = p.add_argument
    arg("--server-host", required=True, help="cluster running (or to run) the server")
    arg("--site", action="append", required=True,
        help="cluster to run a worker on (repeatable; name or pw:// URI)")
    arg("--worker-name", default="", help="blank = worker-<pw user>-<hostname>; ignored with multiple sites")
    arg("--worker-workdir", default="~/app-testbed-worker")
    arg("--scheduler", action="store_true", help="submit workers as batch jobs")
    arg("--partition", default="", help="SLURM partition / PBS queue")
    arg("--account", default="")
    arg("--walltime", default="01:00:00")
    arg("--extra", default="", help="extra sbatch args / extra #PBS directive")
    arg("--pbs-directives", default="", help="verbatim #PBS lines (replaces the generated ones)")
    arg("--no-deploy-server", dest="deploy_server", action="store_false", default=True)
    arg("--restart", action="store_true", help="restart the server if already running")
    arg("--no-wait", dest="wait_workers", action="store_false", default=True,
        help="do not block the run until workers connect")
    arg("--workdir", default="~/app-testbed")
    arg("--subdomain", default="apptest")
    arg("--port", type=int, default=8090)
    arg("--tunnel-method", choices=["auto", "ssh", "pw-forward"], default="auto",
        help="pw-forward verifies whether a pw CLI release passes websocket upgrades")
    arg("--workflow", default="marketplace.app-testbed.v1.0",
        help="workflow name or local yaml path")
    arg("--context", default=os.environ.get("PW_CONTEXT", ""), help="pw CLI context")
    arg("--no-watch", dest="watch", action="store_false", default=True)
    arg("--poll", type=int, default=15)
    arg("--timeout-min", type=int, default=45)
    arg("--dry-run", action="store_true", help="validate without executing")
    arg("--print-inputs", action="store_true", help="print inputs JSON and exit")
    arg("--log", default="launch-worker.log", help="also write output to this file ('' disables)")
    a = p.parse_args()

    if a.log:
        fh = open(a.log, "w")
        sys.stdout, sys.stderr = Tee(sys.stdout, fh), Tee(sys.stderr, fh)
        print("logging to %s" % os.path.abspath(a.log))

    me = pw(["auth", "whoami"], a.context).strip().splitlines()[0].strip()
    server_res = resolve(a.server_host, a.context, me)
    inputs = build_inputs(a, server_res, [resolve(s, a.context, me) for s in a.site])
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
          % (slug, len(a.site), a.scheduler, a.tunnel_method))
    if not a.watch:
        print("follow with: pw workflows runs logs %s" % slug)
        return 0
    return watch(a, slug)


if __name__ == "__main__":
    sys.exit(main())
