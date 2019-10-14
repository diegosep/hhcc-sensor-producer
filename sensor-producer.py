#!/usr/bin/env python3

import ssl
import time
import sys
import re
import json
import os.path
import argparse
from time import time, sleep, localtime, strftime
from collections import OrderedDict
from colorama import init as colorama_init
from colorama import Fore, Back, Style
from configparser import ConfigParser
from unidecode import unidecode
from miflora.miflora_poller import MiFloraPoller, MI_BATTERY, MI_CONDUCTIVITY, MI_LIGHT, MI_MOISTURE, MI_TEMPERATURE
from btlewrap import available_backends, BluepyBackend, GatttoolBackend, PygattBackend, BluetoothBackendException
import sdnotify
from google.cloud import pubsub_v1

project_name = 'Xiaomi Mi Flora Plant Sensor MQTT Client/Daemon'
project_url = 'https://github.com/ThomDietrich/miflora-mqtt-daemon'

parameters = OrderedDict([
    (MI_LIGHT, dict(name="LightIntensity", name_pretty='Sunlight Intensity', typeformat='%d', unit='lux', device_class="illuminance")),
    (MI_TEMPERATURE, dict(name="AirTemperature", name_pretty='Air Temperature', typeformat='%.1f', unit='°C', device_class="temperature")),
    (MI_MOISTURE, dict(name="SoilMoisture", name_pretty='Soil Moisture', typeformat='%d', unit='%', device_class="humidity")),
    (MI_CONDUCTIVITY, dict(name="SoilConductivity", name_pretty='Soil Conductivity/Fertility', typeformat='%d', unit='µS/cm')),
    (MI_BATTERY, dict(name="Battery", name_pretty='Sensor Battery Level', typeformat='%d', unit='%', device_class="battery"))
])

publisher = pubsub_v1.PublisherClient()
topic_name = 'projects/{project_id}/topics/{topic}'.format(
    project_id=os.getenv('GOOGLE_CLOUD_PROJECT'),
    topic='my-topic',  # Set this to something appropriate.
)

if False:
    # will be caught by python 2.7 to be illegal syntax
    print('Sorry, this script requires a python3 runtime environemt.', file=sys.stderr)

# Argparse
parser = argparse.ArgumentParser(description=project_name, epilog='For further details see: ' + project_url)
parser.add_argument('--gen-openhab', help='generate openHAB items based on configured sensors', action='store_true')
parser.add_argument('--config_dir', help='set directory where config.ini is located', default=sys.path[0])
parse_args = parser.parse_args()

# Intro
colorama_init()
print(Fore.GREEN + Style.BRIGHT)
print(project_name)
print('Source:', project_url)
print(Style.RESET_ALL)

# Systemd Service Notifications - https://github.com/bb4242/sdnotify
sd_notifier = sdnotify.SystemdNotifier()

# Logging function
def print_line(text, error = False, warning=False, sd_notify=False, console=True):
    timestamp = strftime('%Y-%m-%d %H:%M:%S', localtime())
    if console:
        if error:
            print(Fore.RED + Style.BRIGHT + '[{}] '.format(timestamp) + Style.RESET_ALL + '{}'.format(text) + Style.RESET_ALL, file=sys.stderr)
        elif warning:
            print(Fore.YELLOW + '[{}] '.format(timestamp) + Style.RESET_ALL + '{}'.format(text) + Style.RESET_ALL)
        else:
            print(Fore.GREEN + '[{}] '.format(timestamp) + Style.RESET_ALL + '{}'.format(text) + Style.RESET_ALL)
    timestamp_sd = strftime('%b %d %H:%M:%S', localtime())
    if sd_notify:
        sd_notifier.notify('STATUS={} - {}.'.format(timestamp_sd, unidecode(text)))

# Identifier cleanup
def clean_identifier(name):
    clean = name.strip()
    for this, that in [[' ', '-'], ['ä', 'ae'], ['Ä', 'Ae'], ['ö', 'oe'], ['Ö', 'Oe'], ['ü', 'ue'], ['Ü', 'Ue'], ['ß', 'ss']]:
        clean = clean.replace(this, that)
    clean = unidecode(clean)
    return clean

def flores_to_openhab_items(flores, reporting_mode):
    print_line('Generating openHAB items. Copy to your configuration and modify as needed...')
    items = list()
    items.append('// miflora.items - Generated by miflora-mqtt-daemon.')
    items.append('// Adapt to your needs! Things you probably want to modify:')
    items.append('//     Room group names, icons,')
    items.append('//     "gAll", "broker", "UnknownRoom"')
    items.append('')
    items.append('// Mi Flora specific groups')
    items.append('Group gMiFlora "All Mi Flora sensors and elements" (gAll)')
    for param, param_properties in parameters.items():
        items.append('Group g{} "Mi Flora {} elements" (gAll, gMiFlora)'.format(param_properties['name'], param_properties['name_pretty']))
    if reporting_mode == 'mqtt-json':
        for [flora_name, flora] in flores.items():
            location = flora['location_clean'] if flora['location_clean'] else 'UnknownRoom'
            items.append('\n// Mi Flora "{}" ({})'.format(flora['name_pretty'], flora['mac']))
            items.append('Group g{}{} "Mi Flora Sensor {}" (gMiFlora, g{})'.format(location, flora_name, flora['name_pretty'], location))
            for [param, param_properties] in parameters.items():
                basic = 'Number {}_{}_{}'.format(location, flora_name, param_properties['name'])
                label = '"{} {} {} [{} {}]"'.format(location, flora['name_pretty'], param_properties['name_pretty'], param_properties['typeformat'], param_properties['unit'].replace('%', '%%'))
                details = '<text> (g{}{}, g{})'.format(location, flora_name, param_properties['name'])
                channel = '{{mqtt="<[broker:{}/{}:state:JSONPATH($.{})]"}}'.format(base_topic, flora_name, param)
                items.append(' '.join([basic, label, details, channel]))
        items.append('')
        print('\n'.join(items))
    #elif reporting_mode == 'mqtt-homie':
    else:
        raise IOError('Given reporting_mode not supported for the export to openHAB items')


# Load configuration file
config_dir = parse_args.config_dir

config = ConfigParser(delimiters=('=', ))
config.optionxform = str
config.read([os.path.join(config_dir, 'config.ini.dist'), os.path.join(config_dir, 'config.ini')])

used_adapter = 'hci0'

sleep_period = 300
miflora_cache_timeout = sleep_period - 1

print_line('Configuration accepted', console=False, sd_notify=True)

sd_notifier.notify('READY=1')

# Initialize Mi Flora sensors
flores = OrderedDict()
for [name, mac] in config['Sensors'].items():
    if not re.match("C4:7C:8D:[0-9A-F]{2}:[0-9A-F]{2}:[0-9A-F]{2}", mac):
        print_line('The MAC address "{}" seems to be in the wrong format. Please check your configuration'.format(mac), error=True, sd_notify=True)
        sys.exit(1)

    if '@' in name:
        name_pretty, location_pretty = name.split('@')
    else:
        name_pretty, location_pretty = name, ''
    name_clean = clean_identifier(name_pretty)
    location_clean = clean_identifier(location_pretty)

    flora = dict()
    print('Adding sensor to device list and testing connection ...')
    print('Name:          "{}"'.format(name_pretty))
    #print_line('Attempting initial connection to Mi Flora sensor "{}" ({})'.format(name_pretty, mac), console=False, sd_notify=True)

    flora_poller = MiFloraPoller(mac=mac, backend=GatttoolBackend, cache_timeout=miflora_cache_timeout, retries=3, adapter=used_adapter)
    flora['poller'] = flora_poller
    flora['name_pretty'] = name_pretty
    flora['mac'] = flora_poller._mac
    flora['refresh'] = sleep_period
    flora['location_clean'] = location_clean
    flora['location_pretty'] = location_pretty
    flora['stats'] = {"count": 0, "success": 0, "failure": 0}
    try:
        flora_poller.fill_cache()
        flora_poller.parameter_value(MI_LIGHT)
        flora['firmware'] = flora_poller.firmware_version()
    except (IOError, BluetoothBackendException):
        print_line('Initial connection to Mi Flora sensor "{}" ({}) failed.'.format(name_pretty, mac), error=True, sd_notify=True)
    else:
        print('Internal name: "{}"'.format(name_clean))
        print('Device name:   "{}"'.format(flora_poller.name()))
        print('MAC address:   {}'.format(flora_poller._mac))
        print('Firmware:      {}'.format(flora_poller.firmware_version()))
        print_line('Initial connection to Mi Flora sensor "{}" ({}) successful'.format(name_pretty, mac), sd_notify=True)
    print()
    flores[name_clean] = flora

# openHAB items generation
if parse_args.gen_openhab:
    flores_to_openhab_items(flores, reporting_mode)
    sys.exit(0)

print_line('Initialization complete, starting MQTT publish loop', console=False, sd_notify=True)

# Sensor data retrieval and publication
while True:
    time.sleep(10)
    for [flora_name, flora] in flores.items():
        data = dict()
        attempts = 2
        flora['poller']._cache = None
        flora['poller']._last_read = None
        flora['stats']['count'] = flora['stats']['count'] + 1
        print_line('Retrieving data from sensor "{}" ...'.format(flora['name_pretty']))
        while attempts != 0 and not flora['poller']._cache:
            try:
                flora['poller'].fill_cache()
                flora['poller'].parameter_value(MI_LIGHT)
            except (IOError, BluetoothBackendException):
                attempts = attempts - 1
                if attempts > 0:
                    print_line('Retrying ...', warning = True)
                flora['poller']._cache = None
                flora['poller']._last_read = None

        if not flora['poller']._cache:
            flora['stats']['failure'] = flora['stats']['failure'] + 1
            print_line('Failed to retrieve data from Mi Flora sensor "{}" ({}), success rate: {:.0%}'.format(
                flora['name_pretty'], flora['mac'], flora['stats']['success']/flora['stats']['count']
                ), error = True, sd_notify = True)
            print()
            continue
        else:
            flora['stats']['success'] = flora['stats']['success'] + 1

        for param,_ in parameters.items():
            data[param] = flora['poller'].parameter_value(param)
        print_line('Result: {}'.format(json.dumps(data)))

        data['timestamp'] = strftime('%Y-%m-%d %H:%M:%S', localtime())
        data['name'] = flora_name
        data['name_pretty'] = flora['name_pretty']
        data['mac'] = flora['mac']
        data['firmware'] = flora['firmware']
        print('Data for "{}": {}'.format(flora_name, json.dumps(data)))
        print()
        message='{light},{temperature},{moisture},{conductivity},{mac},{battery},{timestamp}'.format(
            light=data['light'],
            temperature=data['temperature'],
            moisture=data['moisture'],
            conductivity=data['conductivity'],
            mac=data['mac'],
            battery=data['battery'],
            timestamp=data['timestamp'],
            )
        publisher.publish(topic_name, bytes(message,'utf-8'))