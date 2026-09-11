#!/usr/bin/env python3
"""Export an allowlist of anonymous measurements; never copy raw cluster state."""

import argparse
import csv
import json
import pathlib


def row_for(result):
    settings = result["settings"]
    metrics = result.get("resource_metrics", [])
    servers = [m for m in metrics if m["role"] == "sink"]
    clients = [m for m in metrics if m["role"] == "load"]
    databases = [m for m in metrics if m["role"] == ("mongodb" if settings["store"] == "mongo" else "opensearch")]
    environments = result.get("server_environment", [])
    variables = environments[0]["variables"] if environments else {}
    service = result.get("server_service_config") or {}
    batching = service.get("batching") or {}
    errors = result.get("errors") or {}
    fault = result.get("fault") or {}
    timeline = result.get("timeline", [])
    failures = [second["second"] for second in timeline if second["failed_rpcs"]]
    final_window = max(0, result.get("elapsed_seconds", 0) - 30)
    complete_seconds = int(result.get("elapsed_seconds", 0))
    window_seconds = min(30, complete_seconds)
    final_complete_window = [second for second in timeline if complete_seconds - window_seconds <= second["second"] < complete_seconds]
    stable = all(not m.get("pod_replaced") and m.get("initial_restarts") == m.get("final_restarts") and not m.get("oom_kills") for m in metrics)
    healthy = bool(result.get("verified") and result.get("healthy", True) and not errors and not result.get("scheduled_not_issued")
                   and not result.get("returncode") and not result.get("harness_error") and stable and not fault)
    row = {"case": result["label"], "backend": settings["store"], "workload": settings["workload"],
           "sink_replicas": len(servers), "cpu_limit_per_sink": servers[0]["resources"]["limits"]["cpu"] if servers else "",
           "memory_limit_per_sink": servers[0]["resources"]["limits"]["memory"] if servers else "",
           "sink_shares_node_with_backend": result.get("sink_colocated_with_backend", ""),
           "node_family": result.get("node_instance_type", ""), "gomemlimit": variables.get("GOMEMLIMIT", ""),
           "gomaxprocs": variables.get("GOMAXPROCS", "auto"), "gogc": variables.get("GOGC", "100"),
           "prestop_seconds": environments[0].get("prestop_seconds", "") if environments else "",
           "termination_grace_seconds": environments[0].get("termination_grace_seconds", "") if environments else "",
           "execution_mib": service.get("max_in_flight_bytes", 0) // (1 << 20), "read_mib": service.get("max_read_bytes", 0) // (1 << 20),
           "batch_wait_ms": batching.get("max_wait_milliseconds"), "batch_operations": batching.get("max_operations", 1000),
           "batch_mib": batching.get("max_bytes", 16 << 20) / (1 << 20),
           "batching_enabled": batching.get("enabled", True), "concurrency": settings["concurrency"],
           "keys": settings["keys"], "hot_keys": settings["hot_keys"], "padding_bytes": settings["padding_bytes"],
           "random_padding": settings.get("random_padding", False),
           "full_incoming_document": settings.get("full_incoming_document", False),
           "dns_min_interval_seconds": settings.get("dns_min_resolution_interval_ns", 30_000_000_000) / 1e9,
           "extra_fields": settings.get("extra_string_fields", 0), "operations_per_rpc": settings["operations_per_rpc"],
           "visible": settings["wait_until_visible"], "return_document": settings["return_document"],
           "backend_members": len(databases), "search_shards": settings.get("search_shards", 1),
           "search_replicas": settings.get("search_replicas", 0), "client_connections": settings.get("connections", 4),
           "search_flush_mib": settings.get("search_flush_mib", 0),
           "search_active_shards": settings.get("search_active_shards", "1"),
           "offered_rpcs_per_second": settings["offered_rpcs_per_second"], "elapsed_seconds": result.get("elapsed_seconds"),
           "rpc_timeout_seconds": settings.get("rpc_timeout_ns", 5_000_000_000) / 1e9,
           "error_backoff_seconds": settings.get("error_backoff_ns", 10_000_000) / 1e9,
           "successful_rpcs_per_second": result.get("rpcs_per_second"), "successful_operations_per_second": result.get("operations_per_second"),
           "p50_ms": result.get("p50_ms"), "p95_ms": result.get("p95_ms"), "p99_ms": result.get("p99_ms"), "max_ms": result.get("max_ms"),
           "execution_p99_ms": result.get("execution_p99_ms", ""),
           "errors": sum(errors.values()), "scheduled_not_issued": result.get("scheduled_not_issued", 0),
           "failed_rpcs": result["rpcs"] - result["successful_rpcs"] if "successful_rpcs" in result else "",
           "verified": result.get("verified", False), "cold_mapping": settings.get("cold_mapping", True),
           "healthy": healthy, "excluded": bool(result.get("excluded_reason")),
           "fault": fault.get("kind", ""),
           "fault_confirmed": fault.get("confirmed", ""), "fault_after_seconds": fault.get("started_after_seconds", ""),
           "first_error_second": min(failures) if failures else "", "last_error_second": max(failures) if failures else "",
           "last_30s_failed_rpcs": sum(second["failed_rpcs"] for second in timeline if second["second"] >= final_window),
           "last_30s_successful_rpcs_per_second": sum(second["successful_rpcs"] for second in final_complete_window) / window_seconds if timeline and window_seconds else "",
           "reconciled_unacknowledged": result.get("reconciled_unacknowledged", 0),
           "warm_connections": settings.get("warm_connections", False),
           "sink_cpu_cores": sum(m.get("cpu_cores", 0) for m in servers),
           "load_cpu_cores": sum(m.get("cpu_cores", 0) for m in clients),
           "backend_cpu_cores": sum(m.get("cpu_cores", 0) for m in databases),
           "backend_read_mib_per_second": sum(m.get("io_per_second", {}).get("rbytes", 0) for m in databases) / (1 << 20) if any("io_per_second" in m for m in databases) else "",
           "backend_write_mib_per_second": sum(m.get("io_per_second", {}).get("wbytes", 0) for m in databases) / (1 << 20) if any("io_per_second" in m for m in databases) else "",
           "sink_max_throttled_period_percent": max((m.get("throttled_period_percent", 0) for m in servers), default=0),
           "sink_peak_sampled_cgroup_mib": max((m.get("peak_sampled_memory_bytes", 0) for m in servers), default=0) / (1 << 20),
           "sink_peak_sampled_rss_mib": max((m.get("peak_sampled_main_rss_bytes", 0) for m in servers), default=0) / (1 << 20),
           "pod_replaced": any(m.get("pod_replaced", False) for m in metrics),
           "oom_kills": sum(m.get("oom_kills", 0) for m in metrics), "sampling_errors": result.get("sampling_errors", 0),
           "search_stats_samples": result.get("search_stats_samples", 0), "search_stats_sampling_errors": result.get("search_stats_sampling_errors", 0),
           "server_binary_sha256": environments[0].get("binary_sha256", "") if environments else "",
           "load_binary_sha256": result.get("load_binary_sha256", "")}
    return row


def flush_rows_for(result, samples):
    rows = []
    for sample in samples:
        elapsed = (sample["unix_ns"] - result["started_unix_ns"]) / 1e9
        if not 0 <= elapsed <= result["elapsed_seconds"]:
            continue
        stats = sample.get("stats", {}).get("_all", {})
        primary = stats.get("primaries", {})
        total = stats.get("total", {})
        if "flush" not in primary or "translog" not in primary:
            continue
        row = {"case": result["label"], "observed_after_seconds": elapsed,
               "search_shards": result["settings"].get("search_shards", 1),
               "primary_flushes": primary["flush"]["total"],
               "primary_flush_time_ms": primary["flush"]["total_time_in_millis"],
               "all_copy_flushes": total.get("flush", {}).get("total", ""),
               "all_copy_flush_time_ms": total.get("flush", {}).get("total_time_in_millis", ""),
               "primary_uncommitted_translog_mib": primary["translog"]["uncommitted_size_in_bytes"] / (1 << 20),
               "primary_indexed_operations": primary.get("indexing", {}).get("index_total", "")}
        rows.append(row)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--flush-output", help="Optional anonymous index flush observations")
    opts = parser.parse_args()
    rows, flush_rows = [], []
    for filename in sorted(pathlib.Path(opts.input_dir).glob("*.json")):
        result = json.loads(filename.read_text())
        if isinstance(result, dict) and result.get("settings") and "resource_metrics" in result:
            rows.append(row_for(result))
            flush_file = filename.with_suffix(".flush-stats.json")
            if opts.flush_output and flush_file.exists():
                flush_rows.extend(flush_rows_for(result, json.loads(flush_file.read_text())))
    if not rows:
        raise SystemExit("No measured results found")
    destination = pathlib.Path(opts.output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    print(f"Exported {len(rows)} anonymous measurements")
    if opts.flush_output and flush_rows:
        destination = pathlib.Path(opts.flush_output)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("w", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=list(flush_rows[0]), lineterminator="\n")
            writer.writeheader()
            writer.writerows(flush_rows)
        print(f"Exported {len(flush_rows)} anonymous flush observations")


if __name__ == "__main__":
    main()
