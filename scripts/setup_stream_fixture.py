"""Prepare the private STEP 05 synthetic broker without touching STEP 03 volumes."""

import json

from scripts.smoke_cdc import (
    Core, RAW, SECRETS, command, configure, insert, provision, require, topics,
)

PROJECT = "stokora-stream-fixture"


def main():
    directory = SECRETS / PROJECT
    if directory.exists():
        raise RuntimeError("STEP 05 fixture already exists; preserve and inspect it")
    provision(PROJECT)
    configure(PROJECT)
    core = Core(PROJECT)
    try:
        core.compose("up", "-d", "--wait", "--wait-timeout", "150", "postgres", "kafka")
        topics(core)
        core.kafka("acls", "--add", "--allow-principal", "User:stokora_observer", "--operation", "Read",
                   "--group", "stokora-bronze-", "--resource-pattern-type", "prefixed")
        observer = (directory / "broker_observer").read_text(encoding="utf-8")
        jaas = next(line.partition("=")[2] for line in observer.splitlines() if line.startswith("sasl.jaas.config="))
        (directory / "stream_jaas").write_text(f"KafkaClient {{ {jaas} }};\n", encoding="utf-8")
        core.postgres("""INSERT INTO operational.tenants VALUES ('SIM_A'),('SIM_B');
            INSERT INTO operational.stores VALUES ('SIM_A','SIM_SITE'),('SIM_B','SIM_SITE');
            INSERT INTO operational.products VALUES ('SIM_A','SIM_PRODUCT'),('SIM_B','SIM_PRODUCT');"""
                      + insert("snapshot"))
        core.compose("up", "-d", "--wait", "--wait-timeout", "150", "connect")
        core.connect("connectors/sales/config", "-X", "PUT", "-H", "Content-Type: application/json",
                     "--data-binary", "@/opt/stokora/sales.json")
        core.require_ready()
        require(core.read(1)[0][3]["op"] == "r", "initial snapshot missing")

        core.postgres(insert("live") + insert("resume"))
        core.postgres("UPDATE operational.sales_events SET quantity=5 WHERE event_id='live';")
        core.postgres("DELETE FROM operational.sales_events WHERE event_id='live';")
        records = core.read(6)
        require(sorted(value["op"] for *_, value in records if value) == ["c", "c", "d", "r", "u"],
                "missing genuine CDC actions")
        key, envelope = next((key, value) for _, _, key, value in records
                             if value and key["event_id"] == "live" and value["op"] == "c")
        prefix = json.dumps(key) + "\t"
        rejected = dict(envelope, after=dict(envelope["after"], quantity=-1))
        payloads = [json.dumps(envelope), "{malformed", json.dumps(rejected), json.dumps(dict(envelope, op="t"))]
        core.inside("kafka", "env", "KAFKA_HEAP_OPTS=-Xmx192m", "/opt/kafka/bin/kafka-console-producer.sh",
                    "--bootstrap-server", "kafka:9092", "--producer.config", "/run/secrets/broker_admin",
                    "--topic", RAW, "--property", "parse.key=true",
                    sql="".join(f"{prefix}{payload}\n" for payload in payloads))
        require(core.end_offset() == 10, "fixture Kafka watermark mismatch")
        sql = core.postgres("SET ROLE stokora_noaccess; SELECT * FROM operational.sales_events;", allow_failure=True)
        require(sql.returncode != 0 and "permission denied" in sql.stderr, "PostgreSQL authorized unexpected read")
        ids = core.compose("ps", "-q").stdout.split()
        inspected = json.loads(command(["docker", "inspect", *ids]).stdout)
        require(all(not i["HostConfig"]["PortBindings"] for i in inspected), "core published host ports")
        network = json.loads(command(["docker", "network", "inspect", PROJECT + "_data"]).stdout)
        require(network[0]["Internal"], "fixture network must be internal")
        print("PASS fixture Kafka end=10; PostgreSQL unauthorized read denied; private network and no host ports")
    except BaseException:
        core.compose("stop", allow_failure=True)
        raise


if __name__ == "__main__":
    main()
