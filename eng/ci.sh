#!/usr/bin/bash
#
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

set -uvex -o pipefail

cd $(dirname ${BASH_SOURCE[0]})/../

eng/setup.sh
eng/build.sh
