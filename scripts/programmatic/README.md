# Programmatic launch

`launch-worker.py` resolves cluster names or `pw://owner/name` URIs to
full resource objects, builds the workflow inputs, submits the
marketplace workflow (`marketplace.app-testbed.v1.0` — add it to your
account once with `pw marketplace add-to-account app-testbed`), and
follows the run until the workers connect:

```bash
# server plus one worker on another cluster
./launch-worker.py --server-host clusterA --site clusterB

# workers on two sites, submitted as batch jobs
./launch-worker.py --server-host clusterA --site clusterB --site clusterC \
    --scheduler --partition debug --walltime 00:30:00

# verify a pw CLI release passes websocket upgrades through pw forward
./launch-worker.py --server-host clusterA --site clusterB --tunnel-method pw-forward
```

Runs under the caller's current pw context (`PW_CONTEXT` or `--context`
to override) and writes its output to a log file in the current
directory. `--no-wait` finishes the run at submission instead of
blocking until workers connect. See `./launch-worker.py --help` for all
options; `../manual/` is the pw-CLI-only path with a static inputs file.
