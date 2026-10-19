"""asyncio application: WiFi and MQTT are supervised in the background, your tasks just keep running.

Copy `wifimanager/` and `mqttmanager/` to /lib/ on the board, this file to /main.py.
"""
import asyncio

from wifimanager import WiFiManager
from mqttmanager import MQTTManager

wm = WiFiManager(networks=[("MyHome", "my-home-password")])

mq = MQTTManager(
    "192.168.1.10",                     # broker host name or IP
    wifi=wm,
    user="device", password="device-password",   # None if the broker allows anonymous clients
    status_topic="home/esp32/status",   # retained "online" now, "offline" (will) when the device vanishes
    on_connect=lambda: print("MQTT ONLINE"),
    on_disconnect=lambda reason: print("MQTT OFFLINE:", reason),
)


@mq.on("home/esp32/led")
def led(topic, payload):
    print("LED ->", payload.decode())


async def worker():
    n = 0
    while True:
        n += 1
        mid = mq.publish("home/esp32/counter", n, qos=1)   # queued in RAM while offline
        if await mq.wait_delivered(mid, timeout=10):
            print("delivered", n)
        else:
            print("still queued:", mq.pending(), "message(s)")
        await asyncio.sleep(10)


async def main():
    await mq.start()                    # starts the WiFiManager too, then everything runs in background
    await worker()


asyncio.run(main())
