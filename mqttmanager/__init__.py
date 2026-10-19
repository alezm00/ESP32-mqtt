"""mqttmanager - MQTT for MicroPython on ESP32, on top of umqtt.simple and wifimanager.

    from wifimanager import WiFiManager
    from mqttmanager import MQTTManager
    wm = WiFiManager(networks=[("MyHome", "my-password")])
    mq = MQTTManager("192.168.1.10", wifi=wm)
    mq.connect()
"""
from .manager import (MQTTManager, __version__, topic_matches,
                      IDLE, NO_WIFI, CONNECTING, CONNECTED, BACKOFF)

__all__ = ["MQTTManager", "topic_matches", "IDLE", "NO_WIFI", "CONNECTING", "CONNECTED", "BACKOFF"]
