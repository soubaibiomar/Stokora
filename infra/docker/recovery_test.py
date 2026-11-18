"""Qualify commit-before-ack recovery on a private copy in this container."""
from pathlib import Path
import hashlib
import os
import subprocess
import tempfile

def main():
    with tempfile.TemporaryDirectory(prefix="stokora-recovery-") as path:
        root=Path(path)
        env=dict(os.environ,STOKORA_STREAM_ROOT=path,TMPDIR=path,HOME=path,
                 STOKORA_KAFKA_CA="/var/lib/postgresql/data/trust/ca.pem",
                 JAVA_TOOL_OPTIONS=f"-Djava.io.tmpdir={path} -Djava.security.auth.login.config=/var/lib/postgresql/data/reader-auth/reader.jaas")
        ledger=root/"cdc_bronze/_delta_log"
        def commits():
            return {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in ledger.glob("*.json")}
        fail=subprocess.run(["python","/app/tests/integration/streaming/crash_after_delta.py"],env=env,timeout=180)
        assert fail.returncode==73,fail.returncode
        saved=commits()
        assert len(saved)==2 and not (root/"checkpoint-main/commits/0").exists()
        for extra in ([],["--replay"]):
            subprocess.run(["python","-m","pipelines.streaming.consume_cdc","--once",*extra],env=env,check=True,timeout=180)
            assert commits()==saved
        print("PASS crash exit 73, restart and fresh replay preserved both commit hashes",saved)
if __name__=="__main__":
    main()
