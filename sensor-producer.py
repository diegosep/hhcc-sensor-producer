#!/usr/bin/env python3

import sys
import json
import click
from collections import OrderedDict
from miflora.miflora_poller import MiFloraPoller, MI_BATTERY, MI_CONDUCTIVITY, MI_LIGHT, MI_MOISTURE, MI_TEMPERATURE
from btlewrap import available_backends, BluepyBackend, GatttoolBackend, PygattBackend, BluetoothBackendException
from time import time, sleep, localtime, strftime
from colorama import Fore, Back, Style
from colorama import init as colorama_init

default_mac = "C4:7C:8D:67:46:72"
default_delay = 30
default_device_name = 'Device Name'
default_bootstrap_server = '127.0.0.1:9092'
default_topic = 'chakra_raw_topic'

producer = None
delay = None
topic = None

parameters = OrderedDict([
    (MI_LIGHT, dict(name="LightIntensity", name_pretty='Sunlight Intensity', typeformat='%d', unit='lux', device_class="illuminance")),
    (MI_TEMPERATURE, dict(name="AirTemperature", name_pretty='Air Temperature', typeformat='%.1f', unit='°C', device_class="temperature")),
    (MI_MOISTURE, dict(name="SoilMoisture", name_pretty='Soil Moisture', typeformat='%d', unit='%', device_class="humidity")),
    (MI_CONDUCTIVITY, dict(name="SoilConductivity", name_pretty='Soil Conductivity/Fertility', typeformat='%d', unit='µS/cm')),
    (MI_BATTERY, dict(name="Battery", name_pretty='Sensor Battery Level', typeformat='%d', unit='%', device_class="battery"))
])

colorama_init()
daemon_enabled = True
flores = OrderedDict()

def print_help(ctx, param, value):
    if value is False:
        return
    click.echo(ctx.get_help())
    ctx.exit()

@click.command()
@click.option('--mac', help='mac address of your hhcc mi plant', type=click.STRING, required=True)
@click.option('--delay', default=default_delay, help=f'sleep time between captures (default: {default_delay})', type=int, required=True)
@click.option('--device-name', help=f'device name (default: {default_device_name})', type=click.STRING, required=True)
@click.option('--bootstrap-server', help='bootstrap server', type=click.STRING, required=True)
@click.option('--topic', help='topic name', type=click.STRING, required=True)
@click.option('--help', is_flag=True, expose_value=False, is_eager=False, callback=print_help, help="print help message")
def all_procedure(mac=default_mac, delay=default_delay, device_name=default_device_name, bootstrap_server=default_bootstrap_server, topic=default_topic):

    miflora_cache_timeout = delay - 1
    used_adapter = 'hci0'

    delay = delay
    topic = topic

    flora = dict()
    flora_poller = MiFloraPoller(mac=mac, backend=GatttoolBackend, cache_timeout=miflora_cache_timeout, retries=3, adapter=used_adapter)
    flora['poller'] = flora_poller
    flora['name_pretty'] = device_name
    flora['mac'] = flora_poller._mac
    flora['refresh'] = delay
    flora['stats'] = {"count": 0, "success": 0, "failure": 0}
    try:
        flora_poller.fill_cache()
        flora_poller.parameter_value(MI_LIGHT)
        flora['firmware'] = flora_poller.firmware_version()
    except (IOError, BluetoothBackendException):
        print_line('Initial connection to Mi Flora sensor "{}" ({}) failed.'.format(device_name, mac), error=True)
    else:
        print('Internal name: "{}"'.format(device_name))
        print('Device name:   "{}"'.format(flora_poller.name()))
        print('MAC address:   {}'.format(flora_poller._mac))
        print('Firmware:      {}'.format(flora_poller.firmware_version()))
        print_line('Initial connection to Mi Flora sensor "{}" ({}) successful'.format(device_name, mac))
    print()
    flores[device_name] = flora
    while True:
        for [flora_name, flora] in flores.items():
            data = dict()
            attempts = 2
            data['mac'] = mac
            data['device_name'] = flora['name_pretty']
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
                    ), error = True)
                print()
                continue
            else:
                flora['stats']['success'] = flora['stats']['success'] + 1

            for param,_ in parameters.items():
                data[param] = flora['poller'].parameter_value(param)
                data['datetime'] = strftime('%Y-%m-%d %H:%M:%S', localtime())
            print_line('Result: {}'.format(json.dumps(data)))

            producer.send(topic, json.dumps(data))

        print_line('Status messages published', console=False)

        if daemon_enabled:
            print_line('Sleeping ({} seconds) ...'.format(delay))
            sleep(delay)
            print()
        else:
            print_line('Execution finished in non-daemon-mode')
            break



# Logging function
def print_line(text, error = False, warning=False, console=True):
    timestamp = strftime('%Y-%m-%d %H:%M:%S', localtime())
    if console:
        if error:
            print(Fore.RED + Style.BRIGHT + '[{}] '.format(timestamp) + Style.RESET_ALL + '{}'.format(text) + Style.RESET_ALL, file=sys.stderr)
        elif warning:
            print(Fore.YELLOW + '[{}] '.format(timestamp) + Style.RESET_ALL + '{}'.format(text) + Style.RESET_ALL)
        else:
            print(Fore.GREEN + '[{}] '.format(timestamp) + Style.RESET_ALL + '{}'.format(text) + Style.RESET_ALL)

if __name__ == '__main__':
    all_procedure()