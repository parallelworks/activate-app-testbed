# Manual launch

Launch a testbed run with the pw CLI alone (add the workflow to your
account once with `pw marketplace add-to-account app-testbed`):

```bash
pw workflows run marketplace.app-testbed.v1.0 -i worker-inputs.json
```

Fill in the `resource` blocks in `worker-inputs.json` for your clusters
first. `run-worker.sh` wraps the same call and follows the run to
completion:

```bash
./run-worker.sh
```

To generate the inputs file from live clusters:

```bash
../programmatic/launch-worker.py --server-host <cluster> --site <cluster> --print-inputs > worker-inputs.json
```
