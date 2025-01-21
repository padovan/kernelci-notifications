#!/bin/bash

CODE_DIR='/home/gus/p/ecosystem/kernelci-notifications'

cd $CODE_DIR
source .venv/bin/activate
./generate-notifications --send issues_summary
echo "run-notifications: finished execution at $(date)"