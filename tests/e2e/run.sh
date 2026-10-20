#!/bin/bash
# End-to-end check against a real broker (needs mosquitto + mosquitto-clients and the MicroPython unix port).
#
#     tests/e2e/run.sh [path/to/micropython]
#
# The device (tests/e2e/device.py) runs on MicroPython with real TCP sockets. This script crashes, freezes
# and restarts mosquitto around it and prints what a real subscriber received, phase by phase.
set -u
MP=${1:-micropython}
P=18830
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../.." && pwd)
WORK=$(mktemp -d)
cd "$WORK"
printf 'listener %s 127.0.0.1\nallow_anonymous true\n' $P > mosq.conf
broker() { mosquitto -c mosq.conf >> broker.log 2>&1 & BPID=$!; sleep 0.5; }
broker
mosquitto_sub -p $P -v -t 'e2e/#' > sub1.log & S1=$!
(cd "$REPO" && MICROPYPATH=.frozen:tests/vendor:deps/ESP32-wifi/tests/fakes:deps/ESP32-wifi:. exec "$MP" "$HERE/device.py" $P) > dev.log 2>&1 & DPID=$!
sleep 3
mosquitto_pub -p $P -q 1 -t e2e/cmd/led -m on
sleep 7
echo "--- 1. connect, command -> reply, QoS 1 tick:"; cat sub1.log
kill -9 $BPID; wait $BPID 2>/dev/null; sleep 8          # broker crash: loss detected, ticks queued
broker
mosquitto_sub -p $P -v -t 'e2e/#' > sub2.log & S2=$!
sleep 10
echo "--- 2. after the broker restart (queued ticks delivered):"; cat sub2.log
kill -STOP $BPID; sleep 12; kill -CONT $BPID            # stalled broker: keepalive / PUBACK timeout
sleep 12
echo "--- 3. after the stall:"; tail -4 sub2.log
kill -9 $DPID; wait $DPID 2>/dev/null; sleep 7          # device dies: the broker publishes its will
echo "--- 4. device killed (last will):"; tail -2 sub2.log
kill $S1 $S2 $BPID 2>/dev/null
echo "--- device events:"; grep -E "EVENT|WARN|ERROR" dev.log
rm -rf "$WORK"
