#!/bin/bash

CODE_DIR='/home/gus/p/ecosystem/kernelci-notifications'

cd $CODE_DIR
source .venv/bin/activate
./generate-notifications --send --to  gus@collabora.com tests --giturl https://git.kernel.org/pub/scm/linux/kernel/git/next/linux-next.git --branch master
./generate-notifications --send --to  gus@collabora.com --cc laura.nao@collabora.com tests --giturl https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git --branch master

echo "run tests status: finished execution at $(date)"