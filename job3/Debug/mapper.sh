#!/bin/bash
# Uses awk to parse comma-separated-value and keeps Date;WBAN;DryBulbCelsius;Time
# All the fields in the file header
# $1 - WBAN
# $2 - DATE
# $3 - Time
# $13 - DryBulbCelsius - https://en.wikipedia.org/wiki/Dry-bulb_temperature

cat - | awk -F ',' '{print $2 ";" $1  ";" $13 ";" $3}'
	
