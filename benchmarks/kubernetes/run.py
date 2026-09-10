#!/usr/bin/env python3
"""Run one disposable workload and collect anonymous cgroup measurements."""

import argparse
import concurrent.futures
import json
import shlex
import time
import uuid

import cluster


def inject_fault(namespace, kind, pods, dataset):
    role = "sink"
    if kind == "mongo-stepdown":
        role = "mongodb"
    elif kind == "search-terminate":
        role = "opensearch"
    candidates = [pod for pod in pods if pod["role"] == role]
    if not candidates:
        raise RuntimeError("No owned Pod for the requested fault")
    pod = candidates[0]["name"]
    if kind == "search-terminate":
        endpoint = f"http://localhost:9200/_cat/shards/{dataset}?format=json&h=prirep,state,node"
        shards = json.loads(cluster.kubectl(["-n", namespace, "exec", pod, "--", "curl", "--fail", "--silent", endpoint]))
        primaries = {shard["node"] for shard in shards if shard["prirep"] == "p" and shard["state"] == "STARTED"}
        selected = [entry["name"] for entry in candidates if entry["name"] in primaries]
        if not selected:
            raise RuntimeError("Could not locate the test index primary on an owned database Pod")
        pod = selected[0]
    if kind == "sink-rollout":
        command = ["-n", namespace, "rollout", "restart", "deployment/sink"]
    elif kind == "sink-crash":
        # Go handles SIGABRT by exiting without the normal server drain.
        command = ["-n", namespace, "exec", pod, "-c", role, "--", "sh", "-c", "kill -ABRT 1"]
    elif kind == "mongo-stepdown":
        seeds = ",".join(p["name"] + ".mongodb:27017" for p in candidates)
        command = ["-n", namespace, "exec", pod, "--", "mongosh", "--quiet", "--host", "rs0/" + seeds, "--eval", "rs.stepDown(15, true)"]
    else:
        command = ["-n", namespace, "delete", "pod", pod, "--wait=false"]
    return cluster.kubectl(command)


def sample(namespace, pod):
    script = "cat /sys/fs/cgroup/cpu.stat; echo memory_current; cat /sys/fs/cgroup/memory.current; echo memory_events; cat /sys/fs/cgroup/memory.events; echo uptime; cut -d ' ' -f 1 /proc/uptime; echo main_rss_kib; awk '/^VmRSS:/ {print $2}' /proc/1/status; echo io_stat; cat /sys/fs/cgroup/io.stat"
    started = time.monotonic()
    raw = cluster.kubectl(["-n", namespace, "exec", pod["name"], "-c", pod["container"], "--", "sh", "-c", script])
    lines = raw.splitlines()
    marker = lines.index("memory_current")
    cpu = dict(line.split() for line in lines[:marker])
    uptime_marker = lines.index("uptime")
    events = dict(line.split() for line in lines[marker + 3:uptime_marker])
    io = {"rbytes": 0, "wbytes": 0, "rios": 0, "wios": 0}
    for line in lines[lines.index("io_stat") + 1:]:
        for item in line.split()[1:]:
            key, value = item.split("=", 1)
            if key in io:
                io[key] += int(value)
    value = {"role": pod["role"], "ordinal": pod["ordinal"], "timestamp": float(lines[uptime_marker + 1]),
             "collection_seconds": time.monotonic() - started, "cpu": {k: int(v) for k, v in cpu.items()},
             "memory_bytes": int(lines[marker + 1]), "main_rss_bytes": int(lines[uptime_marker + 3]) * 1024,
             "memory_events": {k: int(v) for k, v in events.items()}, "io": io}
    return value


def fault_observed(kind, command_succeeded, metrics):
    if kind == "mongo-stepdown":
        return command_succeeded
    role = "opensearch" if kind == "search-terminate" else "sink"
    affected = [metric for metric in metrics if metric["role"] == role]
    if not affected:
        return False
    if kind == "sink-crash":
        # A successful signal can close exec before the API reports success.
        return any(metric["pod_replaced"] or (metric["final_restarts"] or 0) > metric["initial_restarts"] for metric in affected)
    replaced = [metric["pod_replaced"] for metric in affected]
    if kind == "sink-rollout":
        return command_succeeded and all(replaced)
    return command_succeeded and any(replaced)


def sample_search_stats(namespace, pod, dataset):
    endpoint = f"http://localhost:9200/{dataset}/_stats/flush,translog,indexing?filter_path=_all"
    raw = cluster.kubectl(["-n", namespace, "exec", pod, "--", "curl", "--fail", "--silent", "--max-time", "5", endpoint])
    value = {"unix_ns": time.time_ns(), "stats": json.loads(raw)}
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--sample-seconds", type=float, default=5)
    parser.add_argument("--keep-data", action="store_true")
    parser.add_argument("--search-stats", action="store_true", help="Sample index flush/translog statistics during a search workload")
    parser.add_argument("--max-seconds", type=int, default=1800)
    parser.add_argument("--fault", choices=["sink-crash", "sink-terminate", "sink-rollout", "mongo-stepdown", "search-terminate"])
    parser.add_argument("--fault-after-seconds", type=float, default=20)
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    opts = parser.parse_args()
    state = cluster.owned_state(opts)
    namespace = state["namespace"]
    entries = json.loads(cluster.kubectl(["get", "pods", "-n", namespace, "-o", "json"]))["items"]
    config_map = json.loads(cluster.kubectl(["get", "configmap", "sink-config", "-n", namespace, "-o", "json"]))
    config = json.loads(config_map["data"]["sink.json"])
    pods, role_counts = [], {}
    for item in sorted(entries, key=lambda p: p["metadata"]["name"]):
        pod_labels = item["metadata"].get("labels") or {}
        role = pod_labels.get("app")
        if role not in ["sink", "load", "mongodb", "opensearch"] or item["metadata"].get("deletionTimestamp"):
            continue
        ready = any(c["type"] == "Ready" and c["status"] == "True" for c in item["status"].get("conditions", []))
        if item["status"]["phase"] != "Running" or not ready:
            raise RuntimeError("Wait for every test Pod to become ready before measuring")
        ordinal = role_counts.get(role, 0)
        role_counts[role] = ordinal + 1
        pod = {"name": item["metadata"]["name"], "uid": item["metadata"]["uid"], "role": role, "container": role, "ordinal": ordinal,
               "resources": item["spec"]["containers"][0]["resources"], "node": item["spec"]["nodeName"],
               "restarts": sum(c["restartCount"] for c in item["status"].get("containerStatuses", []))}
        pods.append(pod)
    expected_members = state.get("database_replicas", 1)
    if any(role_counts.get(role, 0) != expected_members for role in ["mongodb", "opensearch"]):
        raise RuntimeError("Wait for the complete configured database topology before measuring")
    sink_nodes = {p["node"] for p in pods if p["role"] == "sink"}
    if any(p["node"] in sink_nodes for p in pods if p["role"] == "load"):
        raise RuntimeError("Load and Sink must run on separate nodes")
    arguments = opts.arguments[1:] if opts.arguments[:1] == ["--"] else opts.arguments
    protected = {"address", "search-endpoint", "dataset"}
    if any(argument.lstrip("-").split("=", 1)[0] in protected for argument in arguments):
        raise RuntimeError("The Kubernetes runner owns its namespace-local endpoints and unique dataset")
    dataset = "perf-" + uuid.uuid4().hex[:20]
    load_pods = [p for p in pods if p["role"] == "load"]
    if len(load_pods) != 1 or not sink_nodes:
        raise RuntimeError("Require one ready load generator and at least one Sink")
    load_pod = load_pods[0]["name"]
    directory = "/tmp/" + dataset
    command = ["/opt/bin/sink-perf", "-dataset", dataset, *arguments]
    inner = f"{shlex.join(command)} > {directory}/result.json 2> {directory}/stderr; echo $? > {directory}/status"
    launch = f"mkdir {directory} && nohup sh -c {shlex.quote(inner)} </dev/null >/dev/null 2>&1 &"
    destination = cluster.private_artifact_path(opts.output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    pending = {"dataset": dataset, "load_pod": load_pod, "remote_directory": directory, "label": opts.label}
    destination.with_suffix(".pending.json").write_text(json.dumps(pending, indent=2) + "\n")
    cluster.kubectl(["-n", namespace, "exec", load_pod, "-c", "load", "--", "sh", "-c", launch])
    samples, sampling_errors = [], 0
    search_samples, search_sampling_errors = [], 0
    measuring_started, fault = None, None
    messages, returncode = [], None
    deadline = time.monotonic() + opts.max_seconds
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(pods)) as pool:
        while time.monotonic() < deadline:
            progress_script = f"if test -f {directory}/status; then cat {directory}/status; else echo running; fi; cat {directory}/stderr 2>/dev/null || true"
            try:
                progress = cluster.kubectl(["-n", namespace, "exec", load_pod, "-c", "load", "--", "sh", "-c", progress_script]).splitlines()
            except RuntimeError:
                sampling_errors += 1
                time.sleep(opts.sample_seconds)
                continue
            messages = progress[1:]
            if progress[0] != "running":
                returncode = int(progress[0])
                break
            measuring = any("measured workload begins" in line for line in messages)
            finished = any("measured workload finished" in line for line in messages)
            if measuring and not finished:
                if measuring_started is None:
                    measuring_started = time.monotonic()
                elapsed = time.monotonic() - measuring_started
                if opts.fault and fault is None and elapsed >= opts.fault_after_seconds:
                    fault = {"kind": opts.fault, "observed_after_seconds": elapsed, "started_unix_ns": time.time_ns(), "command_succeeded": False}
                    fault_started = time.monotonic()
                    try:
                        inject_fault(namespace, opts.fault, pods, dataset)
                        fault["command_succeeded"] = True
                    except RuntimeError as err:
                        # A killed process can close exec before it replies.
                        fault["command_error"] = str(err)
                    fault["command_seconds"] = time.monotonic() - fault_started
                futures = [pool.submit(sample, namespace, pod) for pod in pods]
                search_future = None
                if opts.search_stats:
                    search_pod = next(pod["name"] for pod in pods if pod["role"] == "opensearch")
                    search_future = pool.submit(sample_search_stats, namespace, search_pod, dataset)
                for future in futures:
                    try:
                        samples.append(future.result())
                    except RuntimeError:
                        sampling_errors += 1
                if search_future is not None:
                    try:
                        search_samples.append(search_future.result())
                    except (RuntimeError, json.JSONDecodeError):
                        search_sampling_errors += 1
            time.sleep(opts.sample_seconds)
    if returncode is None:
        raise RuntimeError("Remote run did not finish; inspect the local pending file before starting another run")
    output = cluster.kubectl(["-n", namespace, "exec", load_pod, "-c", "load", "--", "cat", directory + "/result.json"])
    final_pods = json.loads(cluster.kubectl(["get", "pods", "-n", namespace, "-o", "json"]))["items"]
    final_status = {p["metadata"]["name"]: sum(c["restartCount"] for c in p["status"].get("containerStatuses", [])) for p in final_pods}
    final_uids = {p["metadata"]["name"]: p["metadata"]["uid"] for p in final_pods}
    try:
        result = json.loads(output)
    except json.JSONDecodeError:
        result = {"verified": False, "harness_error": "Load generator did not return JSON"}
    result["healthy"] = bool(result.get("verified") and not result.get("errors") and not result.get("scheduled_not_issued"))
    metrics = []
    for pod in pods:
        matching = [s for s in samples if s["role"] == pod["role"] and s["ordinal"] == pod["ordinal"]]
        value = {"role": pod["role"], "ordinal": pod["ordinal"], "resources": pod["resources"], "initial_restarts": pod["restarts"],
                 "final_restarts": final_status.get(pod["name"]), "pod_replaced": final_uids.get(pod["name"]) != pod["uid"], "samples": len(matching)}
        if len(matching) >= 2 and not value["pod_replaced"] and value["initial_restarts"] == value["final_restarts"]:
            first, last = matching[0], matching[-1]
            elapsed = last["timestamp"] - first["timestamp"]
            delta = {k: last["cpu"][k] - first["cpu"][k] for k in last["cpu"]}
            value.update(cpu_cores=delta["usage_usec"] / 1e6 / elapsed,
                         throttled_period_percent=100 * delta.get("nr_throttled", 0) / max(1, delta.get("nr_periods", 0)),
                         throttled_seconds=delta.get("throttled_usec", 0) / 1e6,
                         peak_sampled_memory_bytes=max(s["memory_bytes"] for s in matching),
                         peak_sampled_main_rss_bytes=max(s["main_rss_bytes"] for s in matching),
                         oom_kills=last["memory_events"].get("oom_kill", 0) - first["memory_events"].get("oom_kill", 0))
            value["io_per_second"] = {key: (last["io"][key] - first["io"][key]) / elapsed for key in first["io"]}
        metrics.append(value)
    result["infrastructure_stable"] = all(
        not m["pod_replaced"] and m["initial_restarts"] == m["final_restarts"] and not m.get("oom_kills") for m in metrics)
    result["healthy"] = result["healthy"] and result["infrastructure_stable"]
    result["fault"] = fault
    if fault:
        fault["started_after_seconds"] = (fault["started_unix_ns"] - result.get("started_unix_ns", fault["started_unix_ns"])) / 1e9
        fault["confirmed"] = fault_observed(opts.fault, fault["command_succeeded"], metrics)
        # Recovery experiments are separate from undisturbed capacity samples.
        result["healthy"] = False
    if opts.fault and (fault is None or not fault["confirmed"]):
        result["healthy"] = False
        result["harness_error"] = "The requested fault was not confirmed"
    if opts.search_stats and not search_samples:
        result["healthy"] = False
        result["harness_error"] = "The requested search statistics could not be collected"
    server_pods = [p for p in entries if (p["metadata"].get("labels") or {}).get("app") == "sink"]
    server_environment = [{"variables": {e["name"]: e.get("value") for e in p["spec"]["containers"][0].get("env", []) if e["name"] in ["GOMEMLIMIT", "GOMAXPROCS", "GOGC"]},
                           "prestop_seconds": p["spec"]["containers"][0].get("lifecycle", {}).get("preStop", {}).get("sleep", {}).get("seconds", 0),
                           "termination_grace_seconds": p["spec"].get("terminationGracePeriodSeconds"),
                           "binary_sha256": (p["metadata"].get("annotations") or {}).get("binary-hash")} for p in server_pods]
    result.update(label=opts.label, returncode=returncode, resource_metrics=metrics, samples=samples, sampling_errors=sampling_errors,
                  search_stats_sampling_errors=search_sampling_errors, search_stats_samples=len(search_samples),
                  server_service_config=config["service"], server_environment=server_environment,
                  load_binary_sha256=state["binaries"]["sink-perf"], node_instance_type=state["node_type"],
                  sink_colocated_with_backend=any(p["node"] in sink_nodes for p in pods if p["role"] in ["mongodb", "opensearch"]))
    destination.write_text(json.dumps(result, indent=2) + "\n")
    if opts.search_stats:
        destination.with_suffix(".flush-stats.json").write_text(json.dumps(search_samples, indent=2) + "\n")
    destination.with_suffix(".stderr").write_text("\n".join(messages) + "\n")
    if result.get("verified") and not opts.keep_data:
        if result["settings"]["store"] == "mongo":
            script = f"if (!db.getSiblingDB('{dataset}').dropDatabase().ok) quit(1)"
            seeds = ",".join(p["name"] + ".mongodb:27017" for p in pods if p["role"] == "mongodb")
            cluster.kubectl(["-n", namespace, "exec", "mongodb-0", "--", "mongosh", "--quiet", "--host", "rs0/" + seeds, "--eval", script])
        else:
            cluster.kubectl(["-n", namespace, "exec", "opensearch-0", "--", "curl", "--fail", "--silent", "-X", "DELETE", f"http://localhost:9200/{dataset}"])
        result["dataset_deleted"] = True
    destination.write_text(json.dumps(result, indent=2) + "\n")
    # Error text can contain endpoint names: keep stderr only in local raw evidence.
    destination.with_suffix(".stderr").write_text("\n".join(messages) + "\n")
    summary = {key: result.get(key) for key in ["label", "rpcs_per_second", "operations_per_second", "p95_ms", "p99_ms", "verified", "healthy", "errors", "scheduled_not_issued", "returncode"]}
    print(json.dumps(summary), flush=True)
    if returncode or not result.get("verified") or result.get("harness_error"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
