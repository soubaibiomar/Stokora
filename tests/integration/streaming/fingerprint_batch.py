"""Read-only integrity fingerprint of retained STEP 04 fixture and legacy targets."""
import hashlib
from pathlib import Path

root = Path("/batch")
targets = [root / "m5-fixture/m5_bronze", *root.glob("acceptance-empty-source-*/table/m5_bronze"),
           *root.glob("step04-evidence-*/legacy/m5_bronze")]
assert (targets[0] / "_delta_log").is_dir()
for target in sorted(targets):
    for path in sorted(target.rglob("*")):
        if path.is_file():
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            print(f"{digest}  {path.relative_to(root)}")
