#!/bin/bash

set -ex

./eng/build.sh
./eng/test-on-azure.sh
cargo package --locked
