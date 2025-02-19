#!/bin/bash

CODE_DIR='/home/gus/p/ecosystem/kernelci-notifications'

cd $CODE_DIR
source .venv/bin/activate
./generate-notifications --send --to kernelci-results@groups.io --cc gus@collabora.com --yes summary

echo "run tests status: finished execution at $(date)"