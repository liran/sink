import argparse
import json
import pathlib
import tempfile
import unittest
from unittest import mock

import cluster
import export
import run


class OwnershipTests(unittest.TestCase):
    def test_api_calls_remain_bound_to_the_validated_context(self):
        response = argparse.Namespace(returncode=0, stdout="", stderr="")
        with mock.patch.object(cluster, "BOUND_CONTEXT", "validated-context"), mock.patch.object(cluster.subprocess, "run", return_value=response) as execute:
            cluster.kubectl(["get", "pods"])
            self.assertIn("--context", execute.call_args.args[0])
            self.assertIn("validated-context", execute.call_args.args[0])

    def test_raw_evidence_cannot_be_written_into_the_repository(self):
        destination = pathlib.Path(cluster.__file__).resolve().parent / "raw.json"
        with self.assertRaisesRegex(RuntimeError, "outside the repository"):
            cluster.private_artifact_path(destination)

    def test_changed_context_never_deletes_anything(self):
        state = {"namespace": "sink-perf-owned", "uid": "original", "context_hash": "expected"}
        with tempfile.TemporaryDirectory() as directory:
            filename = pathlib.Path(directory) / "state.json"
            filename.write_text(json.dumps(state))
            opts = argparse.Namespace(state=str(filename))
            with mock.patch.object(cluster, "context_hash", return_value="different"), mock.patch.object(cluster, "kubectl") as api:
                with self.assertRaisesRegex(RuntimeError, "context changed"):
                    cluster.cleanup(opts)
                api.assert_not_called()

    def test_recreated_namespace_never_gets_deleted(self):
        state = {"namespace": "sink-perf-owned", "uid": "original", "context_hash": "expected"}
        replacement = {"metadata": {"uid": "replacement", "labels": {"app.kubernetes.io/part-of": cluster.LABEL}}}
        with tempfile.TemporaryDirectory() as directory:
            filename = pathlib.Path(directory) / "state.json"
            filename.write_text(json.dumps(state))
            opts = argparse.Namespace(state=str(filename))
            with mock.patch.object(cluster, "context_hash", return_value="expected"), mock.patch.object(cluster, "kubectl", return_value=json.dumps(replacement)) as api:
                with self.assertRaisesRegex(RuntimeError, "ownership changed"):
                    cluster.cleanup(opts)
                self.assertTrue(all(call.args[0][0] == "get" for call in api.call_args_list))

    def test_missing_namespace_without_volume_record_is_not_verified(self):
        state = {"namespace": "sink-perf-owned", "uid": "original", "context_hash": "expected"}
        with tempfile.TemporaryDirectory() as directory:
            filename = pathlib.Path(directory) / "state.json"
            filename.write_text(json.dumps(state))
            opts = argparse.Namespace(state=str(filename))
            with mock.patch.object(cluster, "context_hash", return_value="expected"), mock.patch.object(cluster, "kubectl", return_value=""):
                with self.assertRaisesRegex(RuntimeError, "manual verification"):
                    cluster.cleanup(opts)
            self.assertNotIn("cleanup_verified", json.loads(filename.read_text()))


class ExportTests(unittest.TestCase):
    def test_flush_export_omits_absolute_time_and_cluster_details(self):
        result = {"label": "example", "started_unix_ns": 10_000_000_000, "elapsed_seconds": 60,
                  "settings": {"search_shards": 3, "dataset": "do-not-export"}}
        stats = {"flush": {"total": 2, "total_time_in_millis": 500},
                 "translog": {"uncommitted_size_in_bytes": 1 << 20}, "private": "do-not-export"}
        samples = [{"unix_ns": 15_000_000_000, "stats": {"_all": {"primaries": stats, "total": stats}},
                    "node": "do-not-export"}]
        rows = export.flush_rows_for(result, samples)
        self.assertEqual(rows[0]["observed_after_seconds"], 5)
        self.assertEqual(rows[0]["primary_uncommitted_translog_mib"], 1)
        self.assertNotIn("do-not-export", json.dumps(rows))
        self.assertNotIn("unix_ns", json.dumps(rows))

    def test_export_omits_connection_and_cluster_details(self):
        settings = {"store": "mongo", "workload": "merge", "concurrency": 1, "keys": 1, "hot_keys": 0,
                    "padding_bytes": 1024, "operations_per_rpc": 1, "wait_until_visible": False,
                    "return_document": False, "offered_rpcs_per_second": 0,
                    "address": "do-not-export", "search_endpoint": "do-not-export", "dataset": "do-not-export"}
        result = {"settings": settings, "label": "example", "namespace": "do-not-export", "node": "do-not-export",
                  "context": "do-not-export", "stderr": "do-not-export", "server_service_config": {"extra_private_field": "do-not-export"}}
        exported = export.row_for(result)
        self.assertNotIn("do-not-export", json.dumps(exported))
        self.assertEqual(exported["case"], "example")


class FaultTargetTests(unittest.TestCase):
    def test_rollout_acceptance_does_not_prove_replacement(self):
        metrics = [{"role": "sink", "pod_replaced": False}, {"role": "sink", "pod_replaced": True}]
        self.assertFalse(run.fault_observed("sink-rollout", True, metrics))
        metrics[0]["pod_replaced"] = True
        self.assertTrue(run.fault_observed("sink-rollout", True, metrics))

    def test_delete_acceptance_requires_replacement_of_the_correct_role(self):
        metrics = [{"role": "sink", "pod_replaced": True}, {"role": "opensearch", "pod_replaced": False}]
        self.assertFalse(run.fault_observed("search-terminate", True, metrics))
        metrics[1]["pod_replaced"] = True
        self.assertTrue(run.fault_observed("search-terminate", True, metrics))

    def test_crash_can_be_confirmed_after_exec_disconnects(self):
        metrics = [{"role": "sink", "pod_replaced": False, "initial_restarts": 0, "final_restarts": 1}]
        self.assertTrue(run.fault_observed("sink-crash", False, metrics))
        metrics[0]["final_restarts"] = 0
        self.assertFalse(run.fault_observed("sink-crash", True, metrics))

    def test_search_failure_targets_an_owned_primary_holder(self):
        pods = [{"role": "opensearch", "name": "opensearch-0"}, {"role": "opensearch", "name": "opensearch-1"}]
        shards = [{"prirep": "p", "state": "STARTED", "node": "opensearch-1"}]
        with mock.patch.object(cluster, "kubectl", side_effect=[json.dumps(shards), "deleted"]) as api:
            run.inject_fault("sink-perf-owned", "search-terminate", pods, "perf-example")
            self.assertEqual(api.call_args.args[0], ["-n", "sink-perf-owned", "delete", "pod", "opensearch-1", "--wait=false"])

    def test_search_failure_cannot_delete_an_unowned_primary(self):
        pods = [{"role": "opensearch", "name": "opensearch-0"}]
        shards = [{"prirep": "p", "state": "STARTED", "node": "unowned"}]
        with mock.patch.object(cluster, "kubectl", return_value=json.dumps(shards)) as api:
            with self.assertRaisesRegex(RuntimeError, "owned database Pod"):
                run.inject_fault("sink-perf-owned", "search-terminate", pods, "perf-example")
            self.assertEqual(api.call_count, 1)


if __name__ == "__main__":
    unittest.main()
