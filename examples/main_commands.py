"""Commands with replies: every handler's return value is published on <command topic>/reply.

Try it from a PC:
    mosquitto_sub -h broker.local -v -t 'devices/kitchen/#'
    mosquitto_pub -h broker.local -t devices/kitchen/cmd/led -m toggle
"""
import asyncio

from machine import Pin
from wifimanager import WiFiManager
from mqttmanager import MQTTManager

BASE = "devices/kitchen"
led = Pin(2, Pin.OUT)

wm = WiFiManager(networks=[("MyHome", "my-home-password")])
mq = MQTTManager("broker.local", wifi=wm, status_topic=BASE + "/status")


@mq.on(BASE + "/cmd/led", qos=1, reply=True)
def cmd_led(topic, payload):
    cmd = payload.decode().strip().lower()
    if cmd == "toggle":
        led.value(not led.value())
    elif cmd in ("on", "off"):
        led.value(cmd == "on")
    else:
        raise ValueError("expected on, off or toggle")   # replied as {"error": "ValueError: ..."}
    return {"led": "on" if led.value() else "off"}


@mq.on(BASE + "/cmd/info", reply=True)
def cmd_info(topic, payload):
    return {"ip": wm.ip, "rssi": wm.rssi(), "queued": mq.pending()}


@mq.on(BASE + "/cmd/slow", reply=BASE + "/events")     # async handlers work too; fixed reply topic
async def cmd_slow(topic, payload):
    await asyncio.sleep(2)
    return "slow job done"


async def main():
    await mq.start()
    while True:
        await asyncio.sleep(60)


asyncio.run(main())
