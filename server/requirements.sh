#!/bin/bash

set -o errexit -o nounset -o pipefail -o xtrace

python3 -m pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu
python3 -m pip install sentence-transformers
