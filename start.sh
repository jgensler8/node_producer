#!/bin/bash
node index.js \
  --id raspberrypi \
  --tty-buf /dev/ttyAMA0 \
  --kafka-host 192.168.1.97 \
  --kafka-port 9092
