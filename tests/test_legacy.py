"""Runs the whole suite again against the older umqtt.simple versions frozen in older firmware:

    1.3.4  MicroPython <= 1.22   (usocket, ssl=True + ssl_params, no connect timeout, no unsubscribe)
    1.4.0  MicroPython 1.23-1.24 (ssl=SSLContext only, no ssl_params, no connect timeout, no unsubscribe)
    1.6.0  MicroPython 1.25-1.26 (both TLS forms, connect timeout, no unsubscribe)
"""
import os
import subprocess
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


@unittest.skipIf(os.environ.get("UMQTT"), "already running against a chosen umqtt.simple")
class LegacyUmqttTests(unittest.TestCase):
    def check(self, version):
        env = dict(os.environ, UMQTT=version)
        r = subprocess.run([sys.executable, "-m", "unittest", "test_mqtt", "test_async"], cwd=HERE, env=env,
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        self.assertEqual(r.returncode, 0, r.stdout[-3000:])

    def test_umqtt_1_3_4(self):
        self.check("1.3.4")

    def test_umqtt_1_4_0(self):
        self.check("1.4.0")

    def test_umqtt_1_6_0(self):
        self.check("1.6.0")


if __name__ == "__main__":
    unittest.main()
