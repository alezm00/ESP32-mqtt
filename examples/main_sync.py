"""Plain blocking script, no asyncio in your code.

connect() blocks until WiFi and the broker are up; check() in the main loop reads incoming messages,
sends pings, detects a lost connection and reconnects (it also runs wm.check()).
"""
import time

from wifimanager import WiFiManager
from mqttmanager import MQTTManager

wm = WiFiManager(networks=[("MyHome", "my-home-password")])
mq = MQTTManager("broker.local", wifi=wm, on_message=lambda topic, payload: print(topic, payload))
mq.subscribe("home/esp32/#")

mq.connect()                          # or mq.connect(timeout=60) -> False if still offline

last = time.ticks_ms()
while True:
    mq.check()                        # cheap: does nothing unless something is due. Call it often.
    if time.ticks_diff(time.ticks_ms(), last) >= 10000:
        last = time.ticks_ms()
        mq.publish("home/esp32/uptime", time.ticks_ms() // 1000)   # sent now, or queued while offline
    time.sleep_ms(50)
