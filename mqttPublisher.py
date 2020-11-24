#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 21 17:06:10 2020

@author: mps
"""

import yaml
import json
import paho.mqtt.client as mqtt
import subprocess
import logging
import time
import os
import re
import sys

logger = logging.getLogger("root")
logging.basicConfig(level=logging.DEBUG, format="%(message)s")

dir_path = os.path.dirname(os.path.realpath(__file__))
logger.debug(dir_path)
connected = False


def on_connect(client, userdata, flags, rc):
    print("Connected with flags [%s] rtn code [%d]"% (flags, rc) )
    client.loop_start()

def on_disconnect(client, userdata, rc):
    print("disconnected with rtn code [%d]"% (rc) )
    client.reconnect()

with open(dir_path + "/mqtt_publisher_config.json", "r") as config_file:
    config = yaml.safe_load(config_file)
logger.debug(config)

client = mqtt.Client()
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.username_pw_set(
    config["mqtt_vhost"] + ":" + config["mqtt_user"], config["mqtt_pw"]
)
client.connect(host=config["mqtt_host"], port=config["mqtt_port"], keepalive=30)


last_start = 0
while True:
    if time.time() - last_start > config["interval"] and client.is_connected():
        last_start = time.time()
        for sensor in config["sensors"]:
            command = [
                f'{config["python_command"]}',
                "-u",
                "./LYWSD03MMC.py",
                "-d",
                f'{sensor["mac"]}',
                "-c",
                "1",
                "-b",
                "-urc",
                "3",
            ]
            logger.debug(f"Command: {' '.join(command)}")
            try:
                p = subprocess.Popen(
                    command,
                    cwd=dir_path,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                )
                output, errors = p.communicate(timeout=300)

                logger.debug(f"Errors:\n{errors}")
                logger.debug(f"Output:\n{output}")

                dict_from_output = {}
                for line in output.split("\n"):
                    line_yaml = yaml.safe_load(line)
                    if type(line_yaml) == dict:
                        for i in line_yaml:
                            if type(line_yaml[i]) == str:
                                line_yaml[i] = re.sub("[^\d\.]", "", line_yaml[i])
                            line_yaml[i] = float(line_yaml[i])
                        dict_from_output.update(line_yaml)
                logger.debug(dict_from_output)
                if len(dict_from_output) > 0:
                    topic = config["topic_prefix"] + sensor["mac"].replace(":", "-")
                    payload_dict = {"MAC": sensor["mac"], "tag": sensor["tag"]}
                    payload_dict.update(dict_from_output)
                    client.publish(topic, json.dumps(payload_dict))
                else:
                    logger.info("Did not get any data from program call")
            except subprocess.TimeoutExpired as e:
                logger.error(e)
    time.sleep(.2)
    client.loop()
    if not client.is_connected():
        client.reconnect()
        time.sleep(10)
        if not client.is_connected():
            raise ConnectionError("MQTT reconnect failed")
            sys.exit()
