"""Import this first in every test module: puts the library, its dependency and the fakes on sys.path.

The environment variable UMQTT picks the vendored umqtt.simple: 1.8.1 (default), 1.4.0 or 1.3.4.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
WIFI = os.path.join(ROOT, "deps", "ESP32-wifi")
UMQTT = os.environ.get("UMQTT", "1.8.1")

if not os.path.isdir(os.path.join(WIFI, "wifimanager")):
    sys.exit("deps/ESP32-wifi is empty: run `git submodule update --init` first")

for p in (ROOT, WIFI, os.path.join(WIFI, "tests", "fakes"), os.path.join(HERE, "fakes"),
          os.path.join(HERE, "vendor", "umqtt-" + UMQTT)):
    if p not in sys.path:
        sys.path.insert(0, p)
