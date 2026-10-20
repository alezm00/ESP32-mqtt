"""End-to-end device: real sockets (MicroPython unix port), fake radio, a real mosquitto. Started by run.sh."""
import sys
import asyncio
import network
from wifimanager import WiFiManager
from mqttmanager import MQTTManager

PORT = int(sys.argv[1])
network.WORLD.add_ap("Home", "homepass12", delay_ms=50)
wm = WiFiManager(None, config_file="/tmp/e2e_wifi.json", config_mode="never")
wm.log.console = False
wm.add_network("Home", "homepass12")


def ev(*a):
    print("EVENT", *a)


mq = MQTTManager("127.0.0.1", port=PORT, wifi=wm, client_id="e2e-dev", keepalive=4, response_timeout=2,
                 connect_timeout=3, backoff_max=4, status_topic="e2e/status", queue_size=50,
                 on_connect=lambda: ev("up"), on_disconnect=lambda r: ev("down", r))


@mq.on("e2e/cmd/#", qos=1, reply=True)
def cmd(topic, payload):
    ev("cmd", topic, payload.decode())
    return {"echo": payload.decode()}


async def main():
    await mq.start()
    n = 0
    while True:
        await asyncio.sleep(5)
        n += 1
        mq.publish("e2e/tick", n, qos=1)

asyncio.run(main())
