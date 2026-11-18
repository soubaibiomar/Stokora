"""Single-host synthetic runtime. Compose service startup and synthetic job dispatcher."""
import argparse
import grp
import fcntl
import json
import os
from pathlib import Path
import pwd
import secrets
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.request

ROOT = Path("/var/lib/postgresql/data")
APP = Path("/app")
RAW = "stokora.operational.sales_events"
INTERNAL = ["stokora.connect.config", "stokora.connect.offset", "stokora.connect.status"]
ENV = dict(os.environ, HOME="/tmp", PGHOST="/run/postgresql", KAFKA_HEAP_OPTS="-Xms64m -Xmx256m")
children = []
stopping = False


def account(user):
    entry = pwd.getpwnam(user)
    return dict(user=entry.pw_uid, group=entry.pw_gid, extra_groups=[])


def run(*args, user="root", stdin=None, quiet=False, env=None, timeout=120):
    if user == "forecast":
        args=("python","/app/infra/docker/offline.py",*args)
    return subprocess.run(list(map(str, args)), input=stdin, text=True, check=True, cwd="/tmp",
                          stdout=subprocess.PIPE if quiet else None, env=env or ENV,
                          timeout=timeout, **account(user))


def own(path, user, mode):
    uid = account(user)
    os.chown(path, uid["user"], uid["group"])
    path.chmod(mode)


def write(path, content, user):
    path.write_text(content)
    own(path, user, 0o600)


def sql(statement, database="stokora"):
    return run("psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-d", database, user="postgres",
               stdin=statement, quiet=True).stdout.strip()


def provision():
    marker = ROOT / "trust/format-1"
    if not marker.exists():
        if any(any((ROOT / name).iterdir()) for name in ("trust", "pg", "broker", "connector", "reader-auth")):
            raise ValueError("Unrecognized or partially initialized state; preserve for inspection")
        own(ROOT,"root",0o711)
        for name, user in (("pg", "postgres"), ("broker", "broker"),
                           ("connector", "connector"), ("reader-auth", "reader"), ("trust", "root")):
            own(ROOT / name, user, 0o755 if name == "trust" else 0o700)
        with tempfile.TemporaryDirectory() as scratch:
            ca = Path(scratch) / "ca.key"
            public = ROOT / "trust/ca.pem"
            run("openssl","req","-x509","-newkey","rsa:3072","-nodes","-days","90","-subj","/CN=stokora-demo-ca",
                "-addext","basicConstraints=critical,CA:TRUE","-keyout",ca,"-out",public)
            public.chmod(0o444)
            for directory, name, user in (("pg","postgres","postgres"),("broker","kafka","broker"),
                                         ("connector","localhost","connector"),("connector","operator","root")):
                folder=ROOT/directory
                stem = "operator" if name == "operator" else "tls"
                key, cert = folder / (stem + ".key"), folder / (stem + ".crt")
                csr=Path(scratch)/"leaf.csr"
                ext=Path(scratch)/"usage"
                ext.write_text("extendedKeyUsage=clientAuth\n" if name=="operator" else
                               f"extendedKeyUsage=serverAuth\nsubjectAltName=DNS:{name}\n")
                run("openssl","req","-new","-newkey","rsa:2048","-nodes","-subj","/CN="+name,
                    "-keyout",key,"-out",csr)
                run("openssl","x509","-req","-in",csr,"-CA",public,"-CAkey",ca,"-CAserial",Path(scratch)/"serial",
                    "-CAcreateserial","-days","89","-extfile",ext,"-out",cert)
                own(key,user,0o400)
                own(cert,user,0o444)
        broker,connector,reader,cdc=[secrets.token_hex(32) for _ in range(4)]
        login="org.apache.kafka.common.security.plain.PlainLoginModule required"
        properties=lambda name, password: (f"security.protocol=SASL_SSL\nsasl.mechanism=PLAIN\nssl.truststore.type=PEM\n"
            f"ssl.truststore.location={ROOT}/trust/ca.pem\nsasl.jaas.config={login} username=\"{name}\" password=\"{password}\";\n")
        config=f"""process.roles=broker,controller
node.id=1
controller.quorum.voters=1@kafka:9093
controller.listener.names=CONTROLLER
listeners=BROKER://0.0.0.0:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=BROKER://kafka:9092
inter.broker.listener.name=BROKER
listener.security.protocol.map=BROKER:SASL_SSL,CONTROLLER:SASL_SSL
sasl.enabled.mechanisms=PLAIN
sasl.mechanism.controller.protocol=PLAIN
sasl.mechanism.inter.broker.protocol=PLAIN
ssl.keystore.type=PEM
ssl.keystore.location={ROOT}/broker/server.pem
ssl.truststore.type=PEM
ssl.truststore.location={ROOT}/trust/ca.pem
ssl.endpoint.identification.algorithm=https
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=User:stokora_admin
allow.everyone.if.no.acl.found=false
auto.create.topics.enable=false
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.dirs=/var/lib/stokora-kafka/log
"""
        jaas=f'{login} username="stokora_admin" password="{broker}" user_stokora_admin="{broker}" user_stokora_connect="{connector}" user_stokora_observer="{reader}";'
        for listener in ("broker","controller"):
            config+=f"listener.name.{listener}.plain.sasl.jaas.config={jaas}\n"
        write(ROOT/"broker/server.properties",config,"broker")
        for folder,user in (("broker","broker"),("connector","connector")):
            write(ROOT/folder/"server.pem",(ROOT/folder/"tls.key").read_text()+(ROOT/folder/"tls.crt").read_text(),user)
        write(ROOT/"broker/kafka.properties",properties("stokora_admin",broker),"broker")
        write(ROOT/"reader-auth/reader.jaas",f'KafkaClient {{ {login} username="stokora_observer" password="{reader}"; }};\n',"reader")
        write(ROOT/"reader-auth/client.properties",properties("stokora_observer",reader),"reader")
        write(ROOT/"connector/database","password="+cdc+"\n","connector")
        write(ROOT/"pg/cdc_password",cdc,"root")
        worker=(APP/"infra/kafka/connectors/worker.properties").read_text()
        worker=worker.replace("http://0.0.0.0:8083","https://127.0.0.1:8083").replace("rest.advertised.host.name=connect","rest.advertised.host.name=localhost")
        worker=worker.replace("/run/secrets/connect_database",str(ROOT/"connector/database"))
        worker="\n".join(line for line in worker.splitlines() if "security.protocol=" not in line and "sasl.mechanism=" not in line)+"\n"
        for prefix in ("","producer.","consumer.","admin."):
            worker+="".join(prefix+line+"\n" for line in properties("stokora_connect",connector).splitlines())
        worker+=f"listeners.https.ssl.client.auth=required\nlisteners.https.ssl.keystore.type=PEM\nlisteners.https.ssl.keystore.location={ROOT}/connector/server.pem\nlisteners.https.ssl.truststore.type=PEM\nlisteners.https.ssl.truststore.location={ROOT}/trust/ca.pem\n"
        write(ROOT/"connector/worker.properties",worker,"connector")
        # Jetty's HTTPS listener uses JVM KeyStore formats; Kafka clients separately use PEM.
        folder=ROOT/"connector"
        if not (folder/"rest.p12").exists():
            write(folder/"rest.pass",secrets.token_hex(32),"connector")
            run("openssl","pkcs12","-export","-inkey",folder/"tls.key","-in",folder/"tls.crt",
                "-out",folder/"rest.p12","-passout","file:"+str(folder/"rest.pass"),user="connector")
        if not (folder/"trust.p12").exists():
            run("keytool","-importcert","-noprompt","-alias","ca","-file",ROOT/"trust/ca.pem",
                "-storetype","PKCS12","-keystore",folder/"trust.p12","-storepass:file",folder/"rest.pass",user="connector")
        password=(folder/"rest.pass").read_text()
        lines=[x for x in (folder/"worker.properties").read_text().splitlines() if not x.startswith("listeners.https.")]
        lines.extend(("listeners.https.ssl.client.auth=required","listeners.https.ssl.keystore.type=PKCS12",
                      "listeners.https.ssl.keystore.location="+str(folder/"rest.p12"),
                      "listeners.https.ssl.keystore.password="+password,"listeners.https.ssl.key.password="+password,
                      "listeners.https.ssl.truststore.type=PKCS12","listeners.https.ssl.truststore.location="+str(folder/"trust.p12"),
                      "listeners.https.ssl.truststore.password="+password))
        write(folder/"worker.properties","\n".join(lines)+"\n","connector")
        run("initdb","-D",ROOT/"pg/db","--auth-local=peer","--auth-host=scram-sha-256","--locale=en_US.utf8",user="postgres")
        write(ROOT/"pg/db/pg_hba.conf","local all all peer\nhostnossl all all all reject\nhostssl all stokora_cdc all scram-sha-256\nhostssl replication stokora_cdc all scram-sha-256\n","postgres")
        marker.write_text("compose-1\n")
    if marker.read_text() != "compose-1\n":
        raise ValueError("Unsupported state format")


def launch(service, command, user, env=None):
    proc=subprocess.Popen(list(map(str, command)), cwd="/tmp", env=env or ENV,
                          start_new_session=True, **account(user))
    children.append((service,proc))
    print("starting",service,proc.pid,flush=True)


def alive():
    if stopping:
        raise InterruptedError
    for name,p in children:
        if p.poll() is not None:
            raise RuntimeError(f"{name} exited {p.returncode}; inspect docker compose logs")


def until(probe,seconds=120):
    end=time.monotonic()+seconds
    while time.monotonic()<end:
        alive()
        try:
            if probe():return
        except (subprocess.SubprocessError,OSError):pass
        time.sleep(1)
    raise TimeoutError(probe.__name__)


def kafka(tool,*args):
    return run("kafka-"+tool+".sh","--bootstrap-server","kafka:9092","--command-config",ROOT/"broker/kafka.properties",*args,quiet=True,timeout=25)


def rest(route,config=None):
    ctx=ssl.create_default_context(cafile=str(ROOT/"trust/ca.pem"))
    ctx.load_cert_chain(ROOT/"connector/operator.crt",ROOT/"connector/operator.key")
    req=urllib.request.Request("https://localhost:8083/"+route,data=json.dumps(config).encode() if config else None,
        method="PUT" if config else "GET",headers={"Content-Type":"application/json"})
    with urllib.request.urlopen(req,context=ctx,timeout=45 if config else 4) as response:
        return json.load(response)


def ready():
    r=rest("connectors/sales/status")
    return r["connector"]["state"]=="RUNNING" and len(r["tasks"])==1 and r["tasks"][0]["state"]=="RUNNING"


def boot_postgres():
    provision()
    socketdir=Path("/run/postgresql")
    socketdir.mkdir(exist_ok=True)
    own(socketdir,"postgres",0o700)
    launch("postgres",["postgres","-D",ROOT/"pg/db","-c","listen_addresses=*","-c","ssl=on",
        "-c",f"ssl_cert_file={ROOT}/pg/tls.crt","-c",f"ssl_key_file={ROOT}/pg/tls.key","-c","wal_level=logical"],"postgres")
    until(lambda:sql("select 1","postgres")=="1")
    if sql("select count(*) from pg_database where datname='stokora'","postgres")=="0":
        sql("CREATE DATABASE stokora","postgres")
    if sql("SELECT to_regnamespace('operational') IS NULL")=="t":
        sql((APP/"infra/db/migrations/001_operational.sql").read_text())
    if sql("select count(*) from pg_publication where pubname='stokora_sales'")=="0":
        migration=(APP/"infra/db/migrations/002_cdc.sh").read_text().split("<<'SQL'\n")[1].split("\nSQL")[0]
        run("psql","-Xq","-v","ON_ERROR_STOP=1","-d","stokora",user="postgres",stdin=migration,
            env=dict(ENV,STOKORA_CDC_PASSWORD=(ROOT/"pg/cdc_password").read_text()))

def boot_kafka():
    data = Path("/var/lib/stokora-kafka")
    own(data, "broker", 0o700)
    if not (data / "log/meta.properties").exists():
        if any(data.iterdir()):
            raise ValueError("Unrecognized Kafka state")
        kid = run("kafka-storage.sh", "random-uuid", user="broker", quiet=True).stdout.strip()
        run("kafka-storage.sh", "format", "-t", kid, "-c", ROOT/"broker/server.properties", user="broker")
    launch("kafka",["kafka-server-start.sh", ROOT/"broker/server.properties", "--override", "listeners=BROKER://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093"],"broker",dict(ENV,KAFKA_HEAP_OPTS="-Xms128m -Xmx512m",LOG_DIR="/tmp/kafka-logs"))
    until(lambda:kafka("topics","--list").returncode==0)
    if not (Path("/var/lib/stokora-kafka/topics-ready")).exists():
        for topic in [RAW,"stokora.quarantine",*INTERNAL]:
            kafka("topics","--create","--if-not-exists","--topic",topic,"--partitions","1","--replication-factor","1",
                  *(["--config","cleanup.policy=compact"] if topic in INTERNAL else []))
        for topic in INTERNAL:
            kafka("acls","--add","--allow-principal","User:stokora_connect","--operation","Read","--operation","Write","--operation","DescribeConfigs","--topic",topic)
        for who,op,resource,value in (("connect","Write","topic",RAW),("observer","Read","topic",RAW),("connect","Read","group","stokora-connect"),("observer","Read","group","stokora-bronze-")):
            kafka("acls","--add","--allow-principal","User:stokora_"+who,"--operation",op,"--"+resource,value,*(["--resource-pattern-type","prefixed"] if value.endswith("-") else []))
        (Path("/var/lib/stokora-kafka/topics-ready")).touch()

def boot_connect():
    launch("connect",["connect-distributed.sh",ROOT/"connector/worker.properties"],"connector",dict(ENV,LOG_DIR="/tmp/connect-logs"))
    until(lambda:isinstance(rest("connectors"),list))
    config=json.loads((APP/"infra/kafka/connectors/sales.json").read_text().replace("/run/secrets/connect_database",str(ROOT/"connector/database")))
    config.update({"database.sslmode":"verify-full","database.sslrootcert":str(ROOT/"trust/ca.pem")})
    if "sales" not in rest("connectors"):
        rest("connectors/sales/config",config)
    until(ready)

def boot_spark():
    for name, user, mode in (("reader", "reader", 0o2750), ("jobs", "jobs", 0o755),
                             ("batch", "jobs", 0o700), ("forecast", "forecast", 0o700)):
        path = ROOT / name
        own(path, user, mode)
    os.chown(ROOT/"reader", pwd.getpwnam("reader").pw_uid, grp.getgrnam("jobs").gr_gid)
    (ROOT/"reader").chmod(0o2750)


BOOT = {"postgres": boot_postgres, "kafka": boot_kafka, "connect": boot_connect, "spark": boot_spark}


def service(role):
    def stop(*_):
        global stopping
        stopping=True
    signal.signal(signal.SIGTERM,stop)
    try:
        BOOT[role]()
        Path("/run/stokora-ready").touch()
        while True:
            alive()
            time.sleep(1)
    except InterruptedError:pass
    finally:
        Path("/run/stokora-ready").unlink(missing_ok=True)
        for name,p in reversed(children):
            if p.poll() is None:
                print("stopping",name,flush=True)
                os.killpg(p.pid,signal.SIGTERM)
                try:p.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    os.killpg(p.pid,signal.SIGKILL)
                    p.wait()

def generate():
    from pipelines.generators.transactions import generate as scenario
    data=scenario(42)
    # Bound fixture values are sent via COPY, outside SQL syntax.
    import csv
    import io
    buf=io.StringIO()
    writer=csv.writer(buf,lineterminator="\n")
    for e in data["events"]:
        if e["type"]=="sale_created":
            p=e["payload"]
            writer.writerow([e["tenant_id"],e["site_id"],e["event_id"],p["product_id"],e["origin_timestamp"][:10],p["quantity"],p["currency"],p["unit_price_minor"],e["source"],"true",e["synthetic_scenario_id"]])
    sql("CREATE TABLE IF NOT EXISTS public.stokora_demo_run(seed integer PRIMARY KEY)")
    if sql("SELECT count(*) FROM public.stokora_demo_run WHERE seed=42")=="1":
        print("Seed 42 already applied")
        return
    # Keep generator tenant IDs intact. Exercise the separately authorized CDC fixture too.
    writer.writerow(["SIM_A","SIM_SITE","demo-authorized","SIM_PRODUCT","2024-01-01",2,"MAD",250,"stokora_simulator","true","step03-smoke"])
    result=sql("""BEGIN;
CREATE TEMP TABLE fixture (LIKE operational.sales_events);
COPY fixture FROM STDIN WITH CSV;
"""+buf.getvalue()+"""\\.
INSERT INTO operational.tenants SELECT DISTINCT tenant_id FROM fixture ON CONFLICT DO NOTHING;
INSERT INTO operational.stores SELECT DISTINCT tenant_id,site_id FROM fixture ON CONFLICT DO NOTHING;
INSERT INTO operational.products SELECT DISTINCT tenant_id,product_id FROM fixture ON CONFLICT DO NOTHING;
INSERT INTO operational.sales_events SELECT * FROM fixture;
UPDATE operational.sales_events SET quantity=quantity+1 WHERE event_id='demo-authorized';
DELETE FROM operational.sales_events WHERE event_id=(SELECT max(event_id) FROM fixture);
INSERT INTO public.stokora_demo_run VALUES(42);
COMMIT;""")
    print(data["manifest"],result)


def cli():
    parser=argparse.ArgumentParser(prog="stokora")
    parser.add_argument("action",choices=["status","test","generate","pipeline","forecast","recover"])
    action=parser.parse_args().action
    role = os.environ["STOKORA_SERVICE"]
    allowed = {"postgres": {"generate"}, "kafka": set(), "connect": set(),
               "spark": {"test", "pipeline", "forecast", "recover"}}
    if not Path("/run/stokora-ready").is_file():
        raise SystemExit(f"{role} is not ready")
    if action == "status":
        if role == "postgres" and sql("select 1") != "1":
            raise SystemExit("Database probe failed")
        if role == "connect" and not ready():
            raise SystemExit("Connector or task is not running")
        if role == "kafka":
            kafka("topics", "--list")
        print(f"{role} ready")
        return
    if action not in allowed[role]:
        parser.error(f"{action} is unavailable in {role}")
    if action == "generate":
        with (ROOT/"pg/generate.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            generate()
        return
    with (ROOT/"jobs/jobs.lock").open("a") as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        with tempfile.TemporaryDirectory(prefix="stokora-job-") as tmp:
            own(Path(tmp),"forecast" if action in ("forecast","test") else "jobs",0o700)
            env=dict(ENV,HOME=tmp,TMPDIR=tmp,STOKORA_KAFKA_CA=str(ROOT/"trust/ca.pem"),
                     JAVA_TOOL_OPTIONS=f"-Djava.io.tmpdir={tmp}")
            if action=="test":
                run("python","-m","unittest","discover","-s","/app/tests/data","-v",user="forecast",env=env)
            elif action=="pipeline":
                until(lambda:run("kafka-get-offsets.sh", "--bootstrap-server", "kafka:9092",
                                 "--command-config", ROOT/"reader-auth/client.properties", "--topic", RAW,
                                 user="reader", quiet=True, timeout=25).stdout.strip().endswith(":8"), seconds=45)
                with tempfile.TemporaryDirectory(prefix="stokora-stream-") as stream_tmp:
                    own(Path(stream_tmp),"reader",0o700)
                    stream_env=dict(env,HOME=stream_tmp,TMPDIR=stream_tmp,STOKORA_STREAM_ROOT="/stream",JAVA_TOOL_OPTIONS=f"-Djava.io.tmpdir={stream_tmp} -Djava.security.auth.login.config={ROOT}/reader-auth/reader.jaas")
                    run("python","-m","pipelines.streaming.consume_cdc","--once",user="reader",env=stream_env,timeout=600)
                run("python","/app/infra/docker/demo_gold.py",user="jobs",env=env,timeout=600)
            elif action == "recover":
                run("python", "/app/infra/docker/recovery_test.py", user="reader", env=ENV, timeout=600)
            else:
                run("python","-m","pipelines.ml.run_baselines",user="forecast",env=env,timeout=600)


if __name__=="__main__":
    if len(sys.argv) == 2 and sys.argv[1] in BOOT:
        service(sys.argv[1])
    else:
        cli()
