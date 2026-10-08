"""Exercise the process wrapper protocol without a platform lease service."""

import json
import os
from pathlib import Path
import signal
import subprocess
import sys

assert sys.argv[1:3] == ["run", "--root"]
assert sys.argv[4] == "--"
root = Path(sys.argv[3])
with (root / "invocations.jsonl").open("a") as output:
    output.write(json.dumps({"args": sys.argv[5:], "isolated": sys.flags.isolated}) + "\n")
if (root / "blocked").exists():
    sys.exit(73)

child = subprocess.Popen(sys.argv[5:], close_fds=False)
for number in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
    signal.signal(number, lambda number, frame: child.send_signal(number))
status = child.wait()
if status < 0:
    signal.signal(-status, signal.SIG_DFL)
    os.kill(os.getpid(), -status)
sys.exit(status)
