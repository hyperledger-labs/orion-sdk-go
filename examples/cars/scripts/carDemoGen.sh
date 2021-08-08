#! /bin/bash

if [ -z "$1" ]
  then
    echo "carDemoGen.sh folder"
    exit 1
fi
printf "http://127.0.0.1:6001" > $1/server.url
mkdir $1/txs

