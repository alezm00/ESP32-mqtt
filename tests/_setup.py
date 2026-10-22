"""Import this first in every test module: puts the library, its dependency and the fakes on sys.path."""
import os
import sys

HERE = __file__.rsplit("/", 1)[0] if "/" in __file__ else "."
ROOT = HERE + "/.."
WIFI = ROOT + "/deps/ESP32-wifi"

try:
    os.stat(WIFI + "/wifimanager")
except OSError:
    raise SystemExit("deps/ESP32-wifi is empty: run `git submodule update --init` first")

for p in (ROOT, WIFI, WIFI + "/tests/fakes", HERE + "/fakes", HERE + "/vendor", HERE):
    if p not in sys.path:
        sys.path.insert(0, p)
if WIFI + "/tests" not in sys.path:
    sys.path.append(WIFI + "/tests")  # last: only for its _util helpers (tmpdir, rmtree, async_test)
