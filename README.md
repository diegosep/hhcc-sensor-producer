# Flora sensor Miflora

Send information to pub/sub from raspberry and HHCC plant sensor

```
$ ./setp-up.sh

$ export GOOGLE_CLOUD_PROJECT=PROJECT_ID

$ export GOOGLE_APPLICATION_CREDENTIALS=[FILE_NAME].json

$ python3 sensor-producer.py
```

Add sensors in `[Sensors]` section in the file config.ini.dist