"""Bring up and prove CDC in a fresh isolated Docker Compose project."""

import argparse
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
RAW = "stokora.operational.sales_events"
DLQ = "stokora.quarantine"
INTERNAL = ("stokora.connect.config", "stokora.connect.offset", "stokora.connect.status")
PROJECT = "stokora-core-smoke"
ENV = dict(os.environ)
SECRETS = ROOT / "config/secrets"


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def command(args, *, sql=None, allow_failure=False, timeout=180):
    result = subprocess.run(args, input=sql, capture_output=True, text=True, encoding="utf-8",
                            errors="replace", cwd=ROOT, env=ENV, timeout=timeout)
    if result.returncode and not allow_failure:
        # Commands only use fixed paths, but vendor errors may contain credentials.
        raise RuntimeError(f"{args[0]} command exited {result.returncode}: {args[-1] if sql is None else 'SQL'}")
    return result


class Core:
    def __init__(self, project):
        self.project = project
        self.prefix = ["docker", "compose", "-p", project, "--env-file", "config/.env.example", "-f", "infra/compose/compose.yaml",
                       "-f", "infra/compose/core.yml", "--profile", "core"]

    def compose(self, *args, **options):
        return command(self.prefix + list(args), **options)

    def inside(self, service, *args, **options):
        return self.compose("exec", "-T", service, *args, **options)

    def postgres(self, sql, **options):
        return self.inside("postgres", "psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres",
                           "-d", "stokora", sql=sql, **options)

    def kafka(self, tool, *args, observer=False, consumer=False, **options):
        config = "--consumer.config" if consumer else "--command-config"
        credentials = "/run/secrets/broker_observer" if observer else "/run/secrets/broker_admin"
        return self.inside("kafka", "env", "KAFKA_HEAP_OPTS=-Xmx192m", f"/opt/kafka/bin/kafka-{tool}.sh",
                           "--bootstrap-server", "kafka:9092", config, credentials, *args, **options)

    def connect(self, route, *args, **options):
        return self.inside("connect", "curl", "-fsS", "--max-time", "5", *args,
                           f"http://localhost:8083/{route}", **options)

    def require_ready(self, *, timeout=90):
        started = time.monotonic()
        deadline = started + timeout
        previous = None
        while (remaining := deadline - time.monotonic()) > 0:
            try:
                response = self.connect("connectors/sales/status", allow_failure=True, timeout=min(6, remaining))
                states = ("UNAVAILABLE",)
                if not response.returncode:
                    status = json.loads(response.stdout)
                    states = (status["connector"]["state"], *(task["state"] for task in status["tasks"]))
                require("FAILED" not in states, "Connector task failed; inspect private Connect status")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                response = self.postgres("""SELECT s.active, s.wal_status, coalesce(r.state,'none')
                    FROM pg_replication_slots s LEFT JOIN pg_stat_replication r ON r.pid=s.active_pid
                    WHERE s.slot_name='stokora_sales' AND s.plugin='pgoutput' AND s.database=current_database()""",
                    allow_failure=True, timeout=min(6, remaining))
                slot = response.stdout.strip() or ("missing" if not response.returncode else "unavailable")
            except subprocess.TimeoutExpired:
                states, slot = ("TIMEOUT",), "unavailable"
            streaming = states == ("RUNNING", "RUNNING") and slot in (
                "t|reserved|streaming", "t|extended|streaming",
            )
            broker = "unchecked"
            remaining = deadline - time.monotonic()
            if streaming and remaining > 0:
                try:
                    response = self.kafka("get-offsets", "--topic", RAW, observer=True,
                                          allow_failure=True, timeout=min(6, remaining))
                    broker = ("ready" if response.returncode == 0 and
                              re.fullmatch(re.escape(RAW) + r":0:[0-9]+", response.stdout.strip())
                              else "unavailable")
                except subprocess.TimeoutExpired:
                    broker = "timeout"
            current = states, slot, broker
            if current != previous:
                print(f"CDC readiness seconds={time.monotonic()-started:.3f} "
                      f"states={states} slot={slot} kafka={broker}", flush=True)
                previous = current
            if broker == "ready":
                if time.monotonic() <= deadline:
                    return
                break
            time.sleep(max(0, min(0.5, deadline - time.monotonic())))
        raise RuntimeError(f"CDC not streaming within {timeout}s; last state={previous}")

    def read(self, count):
        result = self.kafka("console-consumer", "--topic", RAW, "--from-beginning", "--timeout-ms", "18000",
                            "--max-messages", str(count), "--property", "print.key=true",
                            "--property", "print.partition=true", "--property", "print.offset=true",
                            observer=True, consumer=True)
        records = []
        for line in result.stdout.splitlines():
            partition, offset, key, value = line.split("\t", 3)
            records.append((int(partition.split(":")[1]), int(offset.split(":")[1]), json.loads(key),
                            None if value == "null" else json.loads(value)))
        require(len(records) == count, f"Expected {count} Kafka records, found {len(records)}")
        return records

    def lag(self):
        query = """SELECT active, wal_status,
           pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint,
           pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint
           FROM pg_replication_slots WHERE slot_name='stokora_sales';"""
        output = self.postgres(query).stdout.strip()
        fields = output.split("|")
        require(len(fields) == 4 and all(field.isdigit() for field in fields[2:]), "No measurable WAL slot")
        print(f"SLOT active={fields[0]} status={fields[1]} retained_bytes={fields[2]} unconfirmed_bytes={fields[3]}", flush=True)
        return fields[0]

    def end_offset(self):
        result = self.kafka("get-offsets", "--topic", RAW).stdout.strip()
        topic, partition, offset = result.split(":")
        require(topic == RAW and partition == "0", "Unexpected Kafka watermark")
        return int(offset)


def provision(project):
    directory = SECRETS / project
    directory.mkdir(parents=True, mode=0o700)
    admin, connect, observer, cdc, postgres = (secrets.token_hex(32) for _ in range(5))
    jaas = "org.apache.kafka.common.security.plain.PlainLoginModule required"
    properties = "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\n"
    for name, content in {
        "postgres_password": postgres,
        "cdc_password": cdc,
        "broker_jaas": f'KafkaServer {{ {jaas} username="stokora_admin" password="{admin}" '
                       f'user_stokora_admin="{admin}" user_stokora_connect="{connect}" '
                       f'user_stokora_observer="{observer}"; }};\n',
        "connect_jaas": f'KafkaClient {{ {jaas} username="stokora_connect" password="{connect}"; }};\n',
        "broker_admin": properties + f'sasl.jaas.config={jaas} username="stokora_admin" password="{admin}";\n',
        "broker_observer": properties + f'sasl.jaas.config={jaas} username="stokora_observer" password="{observer}";\n',
        "connect_database": f"password={cdc}\n",
    }.items():
        fd = os.open(directory / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)

def configure(project):
    directory = SECRETS / project
    ENV.update(CORE_SECRET_DIR=directory.as_posix(),
               POSTGRES_PASSWORD_FILE=(directory / "postgres_password").as_posix())


def topics(core):
    for name in (*INTERNAL, RAW, DLQ):
        options = ("--config", "cleanup.policy=compact") if name in INTERNAL else ()
        core.kafka("topics", "--create", "--if-not-exists", "--topic", name,
                   "--partitions", "1", "--replication-factor", "1", *options)
    for name in INTERNAL:
        core.kafka("acls", "--add", "--allow-principal", "User:stokora_connect",
                   "--operation", "Read", "--operation", "Write", "--operation", "DescribeConfigs", "--topic", name)
    core.kafka("acls", "--add", "--allow-principal", "User:stokora_connect", "--operation", "Write", "--topic", RAW)
    core.kafka("acls", "--add", "--allow-principal", "User:stokora_connect", "--operation", "Read", "--group", "stokora-connect")
    core.kafka("acls", "--add", "--allow-principal", "User:stokora_observer", "--operation", "Read", "--topic", RAW)
    core.kafka("acls", "--add", "--allow-principal", "User:stokora_observer", "--operation", "Read",
               "--group", "console-consumer-", "--resource-pattern-type", "prefixed")
    denied = core.kafka("console-consumer", "--topic", DLQ, "--timeout-ms", "5000",
                        "--max-messages", "1", observer=True, consumer=True, allow_failure=True)
    require("TopicAuthorizationException" in denied.stdout + denied.stderr, "Kafka ACL refusal missing")
    print("PASS Kafka SASL observer cannot consume quarantine topic", flush=True)


def insert(event):
    return f"""INSERT INTO operational.sales_events
        VALUES ('SIM_A','SIM_SITE','{event}','SIM_PRODUCT','2024-01-01',2,'MAD',250,
                'stokora_simulator',true,'step03-smoke');"""


def assert_records(records):
    require([partition for partition, *_ in records] == [0] * 6, "Expected one partition")
    offsets = [offset for _, offset, *_ in records]
    require(offsets == sorted(set(offsets)), "Kafka offsets not strictly increasing")
    expected = [("snapshot", "r"), ("live", "c"), ("live", "u"), ("live", "d"),
                ("live", "tombstone"), ("resume", "c")]
    actual = [(key["event_id"], record["op"] if record else "tombstone")
              for _, _, key, record in records]
    require(actual == expected, f"Unexpected synthetic CDC ordering: {actual}")
    require(all(key["tenant_id"] == "SIM_A" and key["site_id"] == "SIM_SITE"
                for _, _, key, _ in records), "Wrong CDC key scope")
    for partition, offset, key, record in records:
        if record:
            source = record["source"]
            require(isinstance(source["lsn"], int) and isinstance(record["ts_ms"], int),
                    "Missing CDC LSN or timestamp")
            require(record["op"] != "r" or source["snapshot"] in ("true", "last", True),
                    "Snapshot metadata missing")
            for state in (record["before"], record["after"]):
                if state:
                    require(state["is_simulated"] is True, "Synthetic provenance missing")
            print(f"CDC partition={partition} offset={offset} key={key['event_id']} op={record['op']} "
                  f"lsn={source['lsn']} ts_ms={record['ts_ms']}", flush=True)
        else:
            print(f"CDC partition={partition} offset={offset} key={key['event_id']} tombstone=true", flush=True)
    update, delete = records[2][3], records[3][3]
    require(update["before"]["quantity"] == 2 and update["after"]["quantity"] == 5
            and delete["before"]["quantity"] == 5 and delete["after"] is None, "CDC before/after mismatch")
    require(sum(record["op"] == "c" for *_, record in records if record) == 2,
            "Snapshot must be separate from new operational writes")
    print("PASS snapshot, c/u/d, tombstone, metadata, and lagged insert", flush=True)


def smoke(core):
    start = time.monotonic()
    core.compose("up", "-d", "--wait", "--wait-timeout", "150", "postgres", "kafka")
    print(f"READY postgres+kafka seconds={time.monotonic() - start:.2f}", flush=True)
    topics(core)
    core.postgres("""INSERT INTO operational.tenants VALUES ('SIM_A'),('SIM_B');
        INSERT INTO operational.stores VALUES ('SIM_A','SIM_SITE'),('SIM_B','SIM_SITE');
        INSERT INTO operational.products VALUES ('SIM_A','SIM_PRODUCT'),('SIM_B','SIM_PRODUCT');"""
                  + insert("snapshot"))
    denied = core.postgres("SET ROLE stokora_noaccess; SELECT * FROM operational.sales_events;",
                           allow_failure=True)
    require(denied.returncode != 0 and "permission denied" in denied.stderr, "PostgreSQL ACL refusal missing")
    role = core.postgres("""SELECT rolsuper,rolreplication,rolcreatedb,rolcreaterole,
        has_table_privilege('stokora_cdc','operational.sales_events','SELECT'),
        has_table_privilege('stokora_cdc','operational.sales_events','INSERT'),
        has_table_privilege('stokora_cdc','operational.audit','SELECT')
        FROM pg_roles WHERE rolname='stokora_cdc';""").stdout.strip()
    require(role == "f|t|f|f|t|f|f", "Replication privileges or column scope unexpected")
    print(f"PASS PostgreSQL noaccess refused; role(super,repl,db,role,sales_read,sales_write,audit_read)={role}", flush=True)
    core.compose("up", "-d", "--wait", "--wait-timeout", "150", "connect")
    core.connect("connectors/sales/config", "-X", "PUT", "-H", "Content-Type: application/json",
                 "--data-binary", "@/opt/stokora/sales.json")
    core.require_ready()
    snapshot = core.read(1)[0][3]
    require(snapshot["op"] == "r" and snapshot["after"]["event_id"] == "snapshot", "Snapshot not seen")
    core.postgres(insert("live"))
    duplicate = core.postgres(insert("live"), allow_failure=True)
    require(duplicate.returncode != 0 and "duplicate key" in duplicate.stderr, "Duplicate insert not rejected")
    duplicate_count = core.postgres("SELECT count(*) FROM operational.sales_events WHERE event_id='live';").stdout.strip()
    require(duplicate_count == "1", "Idempotent key lost uniqueness")
    print("PASS duplicate sale key refused; exactly one row remains", flush=True)
    core.postgres("UPDATE operational.sales_events SET quantity=5 WHERE event_id='live';")
    core.postgres("DELETE FROM operational.sales_events WHERE event_id='live';")
    require(core.lag() == "t", "Expected active replication slot")
    core.compose("stop", "connect")
    core.postgres(insert("resume"))
    require(core.lag() == "f", "Expected inactive slot while Connect is stopped")
    core.compose("start", "connect")
    core.require_ready()
    records = core.read(6)
    assert_records(records)
    require(core.lag() == "t", "Replication slot did not become active")
    require(core.end_offset() == 6, "Unexpected duplicate or extra Kafka record")

    container_ids = core.compose("ps", "-q").stdout.split()
    require(len(container_ids) == 3, "Expected three core containers")
    inspection = json.loads(command(["docker", "inspect", *container_ids]).stdout)
    require(all(not item["HostConfig"]["PortBindings"] for item in inspection), "Sensitive host port is published")
    network = json.loads(command(["docker", "network", "inspect", core.project + "_data"]).stdout)
    require(network[0]["Internal"], "Core network is not internal")
    print("PASS three services without host port bindings on internal network", flush=True)
    print(command(["docker", "stats", "--no-stream", "--format", "{{.Name}} {{.MemUsage}} {{.CPUPerc}}",
                   *container_ids]).stdout.strip(), flush=True)

    core.compose("stop")
    core.compose("up", "-d", "--wait", "--wait-timeout", "150")
    core.require_ready()
    rows = core.postgres("SELECT string_agg(event_id, ',' ORDER BY event_id) FROM operational.sales_events;").stdout.strip()
    require(rows == "resume,snapshot", "PostgreSQL rows did not persist")
    require(core.read(6) == records, "Kafka payload or offsets changed after restart")
    require(core.end_offset() == 6, "Kafka watermark changed after full restart")
    print("PASS full core stop/start retained rows, connector registration, and Kafka records", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default=PROJECT, help="Compose project name for this smoke test")
    parser.add_argument("--health", action="store_true", help="read service and slot health of an existing smoke project")
    args = parser.parse_args()
    project = args.project
    if not re.fullmatch(r"stokora-core-smoke(?:-[a-z][a-z0-9-]{0,31})?", project):
        parser.error("use stokora-core-smoke or a descriptive stokora-core-smoke suffix")
    if args.health:
        configure(project)
        core = Core(project)
        print(core.compose("ps", "-a").stdout.strip())
        core.require_ready()
        require(core.lag() == "t", "Replication slot is inactive")
        return
    if (SECRETS / project).exists() or command(["docker", "volume", "ls", "-q", "--filter",
                                               f"label=com.docker.compose.project={project}"]).stdout.strip():
        parser.error("smoke project already exists; use --health for the existing fixture; see reports/infrastructure/DOCKER_LIFECYCLE_GUIDE.md for repeatable startup")
    print(f"PROJECT {project} (synthetic, isolated volumes; retained after run)", flush=True)
    provision(project)
    configure(project)
    core = Core(project)
    try:
        smoke(core)
    finally:
        core.compose("stop", allow_failure=True)


if __name__ == "__main__":
    main()
