import argparse
import json
import pathlib
import tempfile
import unittest
from unittest import mock

import cluster
import export


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


if __name__ == "__main__":
    unittest.main()
