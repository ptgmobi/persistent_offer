#!/usr/bin/env bash

killall persistent

sleep 2

nohup bin/persistent > persistent.log 2>&1 &
