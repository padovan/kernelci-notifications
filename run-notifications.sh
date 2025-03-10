#!/bin/bash

CODE_DIR='/home/gus/p/ecosystem/kernelci-notifications-deploy'

cd $CODE_DIR
source .venv/bin/activate
./generate-notifications --yes --send --to gus@collabora.com issues
echo "run-notifications: finished execution at $(date)"