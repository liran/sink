#!/usr/bin/env python3
"""Disposable, namespace-scoped infrastructure for Sink capacity experiments.

Use the caller's kubectl context. State contains ownership IDs, never kubeconfig
contents. Keep state and raw evidence outside the repository.
"""

import argparse
import hashlib
import json
import pathlib
import re
import subprocess
import sys
import time
import uuid


LABEL = "sink-benchmark"
BOUND_CONTEXT = ""


def private_artifact_path(filename):
    destination = pathlib.Path(filename).resolve()
    repository = pathlib.Path(__file__).resolve().parents[2]
    if destination.is_relative_to(repository):
        raise RuntimeError("Keep raw Kubernetes state and evidence outside the repository")
    return destination


def kubectl(arguments, payload=None):
    if arguments[0] in ["apply", "create"]:
        arguments = [*arguments, "--validate=false"]
    if arguments[0] == "apply":
        # Every applied object belongs to the UID-checked disposable namespace.
        # Server-side apply avoids downloading the cluster-wide OpenAPI schema.
        arguments = [*arguments, "--server-side", "--field-manager=sink-benchmark", "--force-conflicts"]
    prefix = ["kubectl", "--request-timeout=30s"]
    if BOUND_CONTEXT and arguments[0] != "config":
        prefix.extend(["--context", BOUND_CONTEXT])
    command = [*prefix, *arguments]
    result = subprocess.run(command, input=payload, text=True, capture_output=True, check=False)
    if result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result.stdout


def context_hash():
    global BOUND_CONTEXT
    BOUND_CONTEXT = kubectl(["config", "current-context"]).strip()
    return hashlib.sha256(BOUND_CONTEXT.encode()).hexdigest()


def resources(cpu, memory):
    quantities = {"cpu": str(cpu), "memory": memory}
    result = {"requests": quantities.copy(), "limits": quantities.copy()}
    return result


def labels(role):
    result = {"app.kubernetes.io/part-of": LABEL, "app": role}
    return result


def metadata(name, namespace):
    result = {"name": name, "namespace": namespace}
    return result


def template_metadata(role, annotations=None):
    values = {"karpenter.sh/do-not-disrupt": "true", "cluster-autoscaler.kubernetes.io/safe-to-evict": "false"}
    extra = annotations or {}
    values.update(extra)
    result = {"labels": labels(role), "annotations": values}
    return result


def service(namespace, role, port, headless=False):
    spec = {"selector": labels(role), "ports": [{"port": port, "targetPort": port}]}
    if headless:
        spec.update(clusterIP="None", publishNotReadyAddresses=role in ["mongodb", "opensearch"])
    result = {"apiVersion": "v1", "kind": "Service", "metadata": metadata(role, namespace), "spec": spec}
    return result


def disruption_budget(namespace):
    result = {"apiVersion": "policy/v1", "kind": "PodDisruptionBudget", "metadata": metadata("protect-active-benchmark", namespace),
              "spec": {"minAvailable": "100%", "selector": {"matchLabels": {"app.kubernetes.io/part-of": LABEL}}}}
    return result


def protect_namespace(opts):
    state = owned_state(opts)
    manifest = disruption_budget(state["namespace"])
    kubectl(["apply", "-f", "-"], json.dumps(manifest))
    print("Active benchmark protected from voluntary Pod eviction", flush=True)


def pod_spec(containers, node_type, arch="arm64", node_selector=None):
    spec = {"automountServiceAccountToken": False, "containers": containers,
            "terminationGracePeriodSeconds": 40, "nodeSelector": {"kubernetes.io/os": "linux", "kubernetes.io/arch": arch}}
    if node_type:
        spec["nodeSelector"]["node.kubernetes.io/instance-type"] = node_type
    extra = node_selector or {}
    for key, value in extra.items():
        if key in spec["nodeSelector"] and spec["nodeSelector"][key] != value:
            raise RuntimeError("Additional node labels cannot override architecture or instance type")
        spec["nodeSelector"][key] = value
    spec["affinity"] = {"podAntiAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": [{
        "weight": 100, "podAffinityTerm": {"labelSelector": {"matchLabels": {"app.kubernetes.io/part-of": LABEL}},
        "topologyKey": "kubernetes.io/hostname"}}]}}
    return spec


def database(opts, role):
    namespace = opts.namespace
    mongo = role == "mongodb"
    port = 27017 if mongo else 9200
    container = {"name": role, "image": "mongo:8.0" if mongo else "opensearchproject/opensearch:3.8.0",
                 "resources": resources(4, "8Gi"), "ports": [{"containerPort": port}],
                 "volumeMounts": [{"name": "data", "mountPath": "/data/db" if mongo else "/usr/share/opensearch/data"}]}
    if mongo:
        container["args"] = ["mongod", "--replSet", "rs0", "--bind_ip_all", "--wiredTigerCacheSizeGB", "3"]
        container["readinessProbe"] = {"tcpSocket": {"port": port}, "periodSeconds": 5}
    else:
        values = {"cluster.name": "sink-benchmark", "node.name": "$(POD_NAME)",
                  "discovery.seed_hosts": "opensearch-0.opensearch,opensearch-1.opensearch,opensearch-2.opensearch",
                  "cluster.initial_cluster_manager_nodes": "opensearch-0", "node.store.allow_mmap": "false",
                  "DISABLE_INSTALL_DEMO_CONFIG": "true", "DISABLE_SECURITY_PLUGIN": "true",
                  "OPENSEARCH_JAVA_OPTS": "-Xms4g -Xmx4g"}
        container["env"] = [{"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}}]
        container["env"].extend({"name": key, "value": value} for key, value in values.items())
        container["readinessProbe"] = {"httpGet": {"port": port, "path": "/"}, "periodSeconds": 5}
    spec = pod_spec([container], opts.node_type, opts.arch, opts.node_selector)
    spec["securityContext"] = {"fsGroup": 999 if mongo else 1000}
    claim = {"metadata": {"name": "data"}, "spec": {"accessModes": ["ReadWriteOnce"], "storageClassName": opts.storage_class,
             "resources": {"requests": {"storage": "20Gi"}}}}
    stateful = {"apiVersion": "apps/v1", "kind": "StatefulSet", "metadata": metadata(role, namespace),
                "spec": {"serviceName": role, "replicas": 1, "selector": {"matchLabels": labels(role)},
                         "template": {"metadata": template_metadata(role), "spec": spec}, "volumeClaimTemplates": [claim]}}
    return [service(namespace, role, port, headless=True), stateful]


def initialize(opts):
    state_path = private_artifact_path(opts.state)
    if state_path.exists():
        raise RuntimeError("State already exists; reuse it or clean up the owned namespace first")
    initial_context_hash = context_hash()
    opts.node_selector = parse_node_labels(opts.node_label)
    storage_class = json.loads(kubectl(["get", "storageclass", opts.storage_class, "-o", "json"]))
    if storage_class.get("reclaimPolicy") != "Delete":
        raise RuntimeError("Choose a disposable StorageClass with reclaimPolicy Delete")
    namespace = {"apiVersion": "v1", "kind": "Namespace", "metadata": {"generateName": "sink-perf-",
                 "labels": {"app.kubernetes.io/part-of": LABEL}, "annotations": {"sink-benchmark/run-id": str(uuid.uuid4())}}}
    created = json.loads(kubectl(["create", "-f", "-", "-o", "json"], json.dumps(namespace)))
    opts.namespace = created["metadata"]["name"]
    state = {"namespace": opts.namespace, "uid": created["metadata"]["uid"], "context_hash": initial_context_hash,
             "node_type": opts.node_type, "arch": opts.arch, "storage_class": opts.storage_class, "node_selector": opts.node_selector}
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, indent=2) + "\n")
    objects = [{"apiVersion": "v1", "kind": "ResourceQuota", "metadata": metadata("benchmark-budget", opts.namespace),
                "spec": {"hard": {"requests.cpu": "48", "limits.cpu": "48", "requests.memory": "96Gi", "limits.memory": "96Gi",
                                  "requests.storage": "160Gi", "persistentvolumeclaims": "8", "pods": "24"}}},
               {"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy", "metadata": metadata("isolated-benchmark", opts.namespace),
                "spec": {"podSelector": {}, "policyTypes": ["Ingress", "Egress"], "ingress": [{"from": [{"podSelector": {}}]}],
                         "egress": [{"to": [{"podSelector": {}}]}, {"ports": [{"protocol": "UDP", "port": 53}, {"protocol": "TCP", "port": 53}]}]}}]
    artifact_container = {"name": "artifacts", "image": "busybox:1.37.0", "command": ["httpd", "-f", "-p", "8081", "-h", "/artifacts"],
                          "resources": resources("100m", "128Mi"), "volumeMounts": [{"name": "artifacts", "mountPath": "/artifacts"}]}
    artifact_spec = pod_spec([artifact_container], opts.node_type, opts.arch, opts.node_selector)
    artifact_spec["volumes"] = [{"name": "artifacts", "emptyDir": {"sizeLimit": "512Mi"}}]
    artifact_controller = {"apiVersion": "apps/v1", "kind": "ReplicaSet", "metadata": metadata("artifacts", opts.namespace),
                           "spec": {"replicas": 1, "selector": {"matchLabels": labels("artifacts")},
                                    "template": {"metadata": template_metadata("artifacts"), "spec": artifact_spec}}}
    objects.extend([service(opts.namespace, "artifacts", 8081), artifact_controller, disruption_budget(opts.namespace)])
    for role in ["mongodb", "opensearch"]:
        objects.extend(database(opts, role))
    manifest = {"apiVersion": "v1", "kind": "List", "items": objects}
    kubectl(["apply", "-f", "-"], json.dumps(manifest))
    result = {"namespace": opts.namespace, "state": str(state_path), "status": "created"}
    print(json.dumps(result))


def owned_state(opts):
    state = json.loads(private_artifact_path(opts.state).read_text())
    if state["context_hash"] != context_hash():
        raise RuntimeError("kubectl context changed; refusing to operate on a different cluster")
    namespace = json.loads(kubectl(["get", "namespace", state["namespace"], "-o", "json"]))
    namespace_labels = namespace["metadata"].get("labels") or {}
    if namespace["metadata"]["uid"] != state["uid"] or namespace_labels.get("app.kubernetes.io/part-of") != LABEL:
        raise RuntimeError("Namespace ownership changed; refusing to operate")
    return state


def parse_node_labels(values):
    selected = {}
    for value in values:
        key, separator, content = value.partition("=")
        if not key or not separator or not content:
            raise RuntimeError("Use --node-label key=value")
        selected[key] = content
    return selected


def set_placement(opts):
    state = owned_state(opts)
    state["node_selector"] = parse_node_labels(opts.node_label)
    private_artifact_path(opts.state).write_text(json.dumps(state, indent=2) + "\n")
    print("Node placement saved for subsequent deployments", flush=True)


def upload(opts):
    state = owned_state(opts)
    name = pathlib.Path(opts.binary).name
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", name):
        raise RuntimeError("Artifact name must use simple letters, digits, dots, underscores and hyphens")
    digest = hashlib.sha256(pathlib.Path(opts.binary).read_bytes()).hexdigest()
    script = f"cat > /artifacts/{name}.tmp && echo '{digest}  /artifacts/{name}.tmp' | sha256sum -c - >/dev/null && mv /artifacts/{name}.tmp /artifacts/{name}"
    command = ["kubectl", "--context", BOUND_CONTEXT, "-n", state["namespace"], "exec", "-i", "replicaset/artifacts", "--", "sh", "-c", script]
    with open(opts.binary, "rb") as source:
        subprocess.run(command, stdin=source, check=True)
    binaries = state.get("binaries") or {}
    binaries[name] = digest
    state["binaries"] = binaries
    pathlib.Path(opts.state).write_text(json.dumps(state, indent=2) + "\n")
    result = {"uploaded": name}
    print(json.dumps(result))


def deploy_server(opts):
    if opts.prestop_seconds < 0 or opts.grace_seconds < opts.prestop_seconds + 30:
        raise RuntimeError("Termination grace must include pre-stop and the 30 second server drain")
    state = owned_state(opts)
    namespace = state["namespace"]
    binaries = state.get("binaries") or {}
    if opts.binary_name not in binaries or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", opts.binary_name):
        raise RuntimeError("Upload the named binary first")
    seeds = ",".join(f"mongodb-{i}.mongodb:27017" for i in range(state.get("database_replicas", 1)))
    config = {"mode": "server", "grpc": {"address": ":8080"}, "prometheus": {"address": ":9090"},
              "storages": [{"name": "mongo", "driver": "mongodb", "mongodb": {"uri": f"mongodb://{seeds}/?replicaSet=rs0", "max_concurrent_writes": opts.mongo_concurrency}},
                           {"name": "search", "driver": "opensearch", "search": {"endpoints": ["http://opensearch:9200"]}}],
              "service": {"request_timeout_seconds": 10, "max_in_flight_requests": 128, "max_store_requests": 32,
                          "max_in_flight_bytes": opts.execution_mib << 20, "max_read_bytes": opts.read_mib << 20,
                          "max_operations": 1000, "max_merge_attempts": 3,
                          "batching": {"enabled": opts.batching == "true", "max_wait_milliseconds": opts.wait_ms,
                                       "max_operations": opts.batch_operations, "max_bytes": opts.batch_mib << 20, "max_queued_bytes": 64 << 20},
                          "lua": {"timeout_milliseconds": 100, "max_result_bytes": 16 << 20}}, "shutdown_timeout_seconds": 30}
    config_text = json.dumps(config)
    config_map = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": metadata("sink-config", namespace), "data": {"sink.json": config_text}}
    container = {"name": "sink", "image": "busybox:1.37.0", "command": ["/bin/sink", "--config", "/config/sink.json"],
                 "resources": resources(opts.cpu, opts.memory), "ports": [{"containerPort": 8080}, {"containerPort": 9090}],
                 "env": [{"name": "GOMEMLIMIT", "value": opts.go_memory}, {"name": "GOGC", "value": str(opts.gogc)}],
                 "volumeMounts": [{"name": "binary", "mountPath": "/bin/sink", "subPath": "sink"}, {"name": "config", "mountPath": "/config", "readOnly": True}],
                 "readinessProbe": {"httpGet": {"path": "/readyz", "port": 9090}, "periodSeconds": 2},
                 "securityContext": {"runAsUser": 65532, "runAsGroup": 65532, "allowPrivilegeEscalation": False,
                                     "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}}}
    if opts.gomaxprocs:
        environment = {"name": "GOMAXPROCS", "value": str(opts.gomaxprocs)}
        container["env"].append(environment)
    if opts.prestop_seconds:
        container["lifecycle"] = {"preStop": {"sleep": {"seconds": opts.prestop_seconds}}}
    spec = pod_spec([container], state["node_type"], state.get("arch", "arm64"), state.get("node_selector"))
    spec["terminationGracePeriodSeconds"] = opts.grace_seconds
    spec["volumes"] = [{"name": "binary", "emptyDir": {}}, {"name": "config", "configMap": {"name": "sink-config"}}]
    spec["initContainers"] = [{"name": "binary", "image": "busybox:1.37.0", "command": ["sh", "-c", f"wget -qO /out/sink http://artifacts:8081/{opts.binary_name} && chmod 755 /out/sink"],
                               "resources": resources("100m", "128Mi"), "volumeMounts": [{"name": "binary", "mountPath": "/out"}]}]
    spec["affinity"]["podAntiAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"] = [{
        "labelSelector": {"matchExpressions": [{"key": "app", "operator": "In", "values": ["sink", "load"]}]},
        "topologyKey": "kubernetes.io/hostname"}]
    annotations = {"config-hash": hashlib.sha256(config_text.encode()).hexdigest(), "binary-hash": state["binaries"][opts.binary_name]}
    strategy = {"type": "Recreate"}
    if opts.rolling == "true":
        strategy = {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": 1, "maxUnavailable": 0}}
    deployment = {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": metadata("sink", namespace), "spec": {
        "replicas": opts.replicas, "minReadySeconds": opts.min_ready_seconds, "selector": {"matchLabels": labels("sink")}, "strategy": strategy,
        "template": {"metadata": template_metadata("sink", annotations), "spec": spec}}}
    objects = [config_map, deployment, service(namespace, "sink", 8080, headless=True)]
    manifest = {"apiVersion": "v1", "kind": "List", "items": objects}
    kubectl(["apply", "-f", "-"], json.dumps(manifest))
    result = {"deployed": "sink", "cpu": opts.cpu, "memory": opts.memory, "replicas": opts.replicas, "wait_ms": opts.wait_ms}
    print(json.dumps(result))


def deploy_load(opts):
    state = owned_state(opts)
    binaries = state.get("binaries") or {}
    if "sink-perf" not in binaries:
        raise RuntimeError("Upload sink-perf first")
    container = {"name": "load", "image": "busybox:1.37.0", "command": ["sleep", "infinity"],
                 "resources": resources(opts.cpu, opts.memory), "env": [{"name": "GOMEMLIMIT", "value": "3GiB"}],
                 "volumeMounts": [{"name": "binary", "mountPath": "/opt/bin"}]}
    spec = pod_spec([container], state["node_type"], state.get("arch", "arm64"), state.get("node_selector"))
    spec["volumes"] = [{"name": "binary", "emptyDir": {}}]
    spec["initContainers"] = [{"name": "binary", "image": "busybox:1.37.0", "command": ["sh", "-c", "wget -qO /out/sink-perf http://artifacts:8081/sink-perf && chmod 755 /out/sink-perf"],
                               "resources": resources("100m", "128Mi"), "volumeMounts": [{"name": "binary", "mountPath": "/out"}]}]
    spec["affinity"]["podAntiAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"] = [{
        "labelSelector": {"matchLabels": labels("sink")}, "topologyKey": "kubernetes.io/hostname"}]
    annotations = {"binary-hash": state["binaries"]["sink-perf"]}
    deployment = {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": metadata("load", state["namespace"]), "spec": {
        "replicas": 1, "selector": {"matchLabels": labels("load")}, "strategy": {"type": "Recreate"},
        "template": {"metadata": template_metadata("load", annotations), "spec": spec}}}
    kubectl(["apply", "-f", "-"], json.dumps(deployment))
    print("Load generator deployed")


def wait_ready(namespace, role, replicas):
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        pods = json.loads(kubectl(["get", "pods", "-n", namespace, "-l", f"app={role}", "-o", "json"]))["items"]
        ready = [p for p in pods if not p["metadata"].get("deletionTimestamp") and any(
            c["type"] == "Ready" and c["status"] == "True" for c in p["status"].get("conditions", []))]
        if len(ready) >= replicas:
            return
        time.sleep(5)
    raise RuntimeError(f"Timed out waiting for {replicas} ready {role} pods")


def prepare_legacy_mongo(opts):
    state = owned_state(opts)
    namespace, role = state["namespace"], "mongodb-legacy"
    container = {"name": role, "image": "mongo:7.0", "args": ["mongod", "--replSet", "legacy", "--bind_ip_all", "--wiredTigerCacheSizeGB", "1"],
                 "resources": resources(2, "2Gi"), "volumeMounts": [{"name": "data", "mountPath": "/data/db"}],
                 "readinessProbe": {"tcpSocket": {"port": 27017}, "periodSeconds": 5}}
    spec = pod_spec([container], state["node_type"], state.get("arch", "arm64"), state.get("node_selector"))
    spec["volumes"] = [{"name": "data", "emptyDir": {"sizeLimit": "2Gi"}}]
    spec["securityContext"] = {"fsGroup": 999}
    deployment = {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": metadata(role, namespace), "spec": {
        "replicas": 1, "selector": {"matchLabels": labels(role)}, "strategy": {"type": "Recreate"},
        "template": {"metadata": template_metadata(role), "spec": spec}}}
    manifest = {"apiVersion": "v1", "kind": "List", "items": [service(namespace, role, 27017), deployment]}
    kubectl(["apply", "-f", "-"], json.dumps(manifest))
    wait_ready(namespace, role, 1)
    configuration = {"_id": "legacy", "members": [{"_id": 0, "host": "mongodb-legacy:27017"}]}
    script = f"try {{ rs.status() }} catch(e) {{ if (e.code !== 94) throw e; if (!rs.initiate({json.dumps(configuration)}).ok) quit(1); }}"
    kubectl(["-n", namespace, "exec", "deployment/" + role, "--", "mongosh", "--quiet", "--eval", script])
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            kubectl(["-n", namespace, "exec", "deployment/" + role, "--", "mongosh", "--quiet", "--eval", "if (!db.hello().isWritablePrimary) quit(1)"])
            print("MongoDB 7 compatibility replica set ready", flush=True)
            return
        except RuntimeError:
            time.sleep(5)
    raise RuntimeError("Legacy MongoDB did not become primary")


def prepare_databases(opts):
    state = owned_state(opts)
    namespace = state["namespace"]
    if opts.replicas < state.get("database_replicas", 1):
        raise RuntimeError("Create a fresh namespace to return to a smaller database topology")
    for role in ["mongodb", "opensearch"]:
        template = {"metadata": template_metadata(role)}
        if state.get("node_selector"):
            template["spec"] = {"nodeSelector": state["node_selector"]}
        patch = {"spec": {"template": template}}
        kubectl(["-n", namespace, "patch", "statefulset/" + role, "--type=merge", "-p", json.dumps(patch)])
    kubectl(["-n", namespace, "scale", "statefulset/mongodb", "statefulset/opensearch", f"--replicas={opts.replicas}"])
    for role in ["mongodb", "opensearch"]:
        wait_ready(namespace, role, opts.replicas)
        command = ["kubectl", "--context", BOUND_CONTEXT, "-n", namespace, "rollout", "status", "statefulset/" + role, "--timeout=600s"]
        subprocess.run(command, check=True)
    members = [{"_id": i, "host": f"mongodb-{i}.mongodb:27017"} for i in range(opts.replicas)]
    configuration = {"_id": "rs0", "members": members}
    script = "try { rs.status() } catch(e) { if (e.code !== 94) throw e; "
    script += f"if (!rs.initiate({json.dumps(configuration)}).ok) quit(1); }}"
    kubectl(["-n", namespace, "exec", "mongodb-0", "--", "mongosh", "--quiet", "--eval", script])
    deadline = time.monotonic() + 600
    for member in members[1:]:
        script = f"const member={json.dumps(member)}; if (!rs.conf().members.some(m=>m._id===member._id)) {{ if (!rs.add(member).ok) quit(1); }}"
        while True:
            try:
                kubectl(["-n", namespace, "exec", "mongodb-0", "--", "mongosh", "--quiet", "--eval", script])
                break
            except RuntimeError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(5)
    script = f"const s=rs.status(); if (s.members.length !== {opts.replicas} || s.members.some(m=>![1,2].includes(m.state))) quit(1)"
    while True:
        try:
            kubectl(["-n", namespace, "exec", "mongodb-0", "--", "mongosh", "--quiet", "--eval", script])
            break
        except RuntimeError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(5)
    endpoint = (f"http://localhost:9200/_cluster/health?wait_for_nodes={opts.replicas}&wait_for_status=green"
                "&wait_for_no_relocating_shards=true&wait_for_no_initializing_shards=true&timeout=60s")
    while True:
        health = json.loads(kubectl(["-n", namespace, "exec", "opensearch-0", "--", "curl", "--fail", "--silent", endpoint]))
        if (not health.get("timed_out") and health.get("number_of_nodes") == opts.replicas and health.get("status") == "green"
                and not health.get("relocating_shards") and not health.get("initializing_shards")):
            break
        if time.monotonic() >= deadline:
            raise RuntimeError("OpenSearch did not settle into the requested healthy topology")
        time.sleep(5)
    state["database_replicas"] = opts.replicas
    pathlib.Path(opts.state).write_text(json.dumps(state, indent=2) + "\n")
    print(f"Both databases ready with {opts.replicas} member(s)", flush=True)


def cleanup(opts):
    state_path = private_artifact_path(opts.state)
    state = json.loads(state_path.read_text())
    if state["context_hash"] != context_hash():
        raise RuntimeError("kubectl context changed; refusing cleanup")
    existing = kubectl(["get", "namespace", state["namespace"], "--ignore-not-found", "-o", "json"])
    if existing:
        state = owned_state(opts)
        volumes = json.loads(kubectl(["get", "pvc", "-n", state["namespace"], "-o", "json"]))["items"]
        volume_names = [v["spec"].get("volumeName") for v in volumes if v["spec"].get("volumeName")]
        state["volumes_pending_cleanup"] = sorted(set(state.get("volumes_pending_cleanup", []) + volume_names))
        state["cleanup_requested"] = True
        state_path.write_text(json.dumps(state, indent=2) + "\n")
        kubectl(["delete", "namespace", state["namespace"], "--wait=false"])
    elif not state.get("cleanup_requested"):
        raise RuntimeError("Namespace disappeared before cleanup recorded its volumes; manual verification required")
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        namespace_exists = bool(kubectl(["get", "namespace", state["namespace"], "--ignore-not-found", "-o", "name"]))
        pending = []
        for name in state["volumes_pending_cleanup"]:
            if kubectl(["get", "pv", name, "--ignore-not-found", "-o", "name"]):
                pending.append(name)
        if not namespace_exists and not pending:
            state["cleanup_verified"] = True
            state_path.write_text(json.dumps(state, indent=2) + "\n")
            print("Cleanup verified: namespace, PVCs and dynamically provisioned PVs are gone", flush=True)
            return
        print(f"Waiting for cleanup: namespace_remaining={namespace_exists}, volumes_remaining={len(pending)}", flush=True)
        time.sleep(10)
    raise RuntimeError("Cleanup still pending; retain the state file and rerun cleanup")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True, help="Local state file outside the repository")
    commands = parser.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init")
    init.add_argument("--node-type", default="")
    init.add_argument("--arch", choices=["arm64", "amd64"], default="arm64")
    init.add_argument("--storage-class", default="gp3")
    init.add_argument("--node-label", action="append", default=[])
    init.set_defaults(execute=initialize)
    push = commands.add_parser("upload")
    push.add_argument("binary")
    push.set_defaults(execute=upload)
    server = commands.add_parser("server")
    server.add_argument("--binary-name", default="sink-candidate")
    server.add_argument("--cpu", default="2")
    server.add_argument("--memory", default="2Gi")
    server.add_argument("--go-memory", default="1536MiB")
    server.add_argument("--gomaxprocs", type=int, default=0)
    server.add_argument("--gogc", type=int, default=100)
    server.add_argument("--execution-mib", type=int, default=512)
    server.add_argument("--read-mib", type=int, default=32)
    server.add_argument("--wait-ms", type=int, default=2)
    server.add_argument("--batching", choices=["true", "false"], default="true")
    server.add_argument("--batch-operations", type=int, default=1000)
    server.add_argument("--batch-mib", type=int, default=16)
    server.add_argument("--mongo-concurrency", type=int, default=64)
    server.add_argument("--replicas", type=int, default=1)
    server.add_argument("--rolling", choices=["true", "false"], default="false")
    server.add_argument("--prestop-seconds", type=int, default=0)
    server.add_argument("--grace-seconds", type=int, default=60)
    server.add_argument("--min-ready-seconds", type=int, default=0)
    server.set_defaults(execute=deploy_server)
    load = commands.add_parser("load")
    load.add_argument("--cpu", default="4")
    load.add_argument("--memory", default="4Gi")
    load.set_defaults(execute=deploy_load)
    databases = commands.add_parser("databases")
    databases.add_argument("--replicas", type=int, choices=[1, 3], default=1)
    databases.set_defaults(execute=prepare_databases)
    legacy = commands.add_parser("legacy-mongo")
    legacy.set_defaults(execute=prepare_legacy_mongo)
    protect = commands.add_parser("protect")
    protect.set_defaults(execute=protect_namespace)
    placement = commands.add_parser("placement")
    placement.add_argument("--node-label", action="append", default=[])
    placement.set_defaults(execute=set_placement)
    remove = commands.add_parser("cleanup")
    remove.set_defaults(execute=cleanup)
    opts = parser.parse_args()
    try:
        opts.execute(opts)
    except (RuntimeError, subprocess.CalledProcessError) as err:
        sys.exit(str(err))


if __name__ == "__main__":
    main()
