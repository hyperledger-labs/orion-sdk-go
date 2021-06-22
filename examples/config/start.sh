#!/bin/bash

# $1 - server executable path
# $2 - config dir

export CONFIG_PATH=/tmp/config-example
./config --configpath $2
./$1 start --configpath $2/config &