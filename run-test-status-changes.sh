#!/bin/bash

CODE_DIR='/home/gus/p/ecosystem/kernelci-notifications-deploy'

cd $CODE_DIR
source .venv/bin/activate
./generate-notifications --send --add-mailing-lists --cc gus@collabora.com --yes summary
echo "run tests status: finished execution at $(date)"