import os
import sys
import time

import matplotlib.pyplot as plt
from qarpo.demoutils import *
from qarpo.demoutils import liveQstat

from utils_image import show_results

print("Imported Python modules successfully.")
!rm -f ./model.pb
!ln -s /data/reference-sample-data/pneumonia-classification-python/model.pb




# Create FP16 IR files
!mo \
--input_model model.pb \
--input_shape=[1,224,224,3] \
--data_type FP16 \
-o models/FP16/ \
--mean_values [123.75,116.28,103.58] \
--scale_values [58.395,57.12,57.375] 

# Create FP32 IR files
!mo \
--input_model model.pb \
--input_shape=[1,224,224,3] \
--data_type FP32 \
-o models/FP32/ \
--mean_values [123.75,116.28,103.58] \
--scale_values [58.395,57.12,57.375] 

# find all resulting IR files
!echo "\nAll IR files that were downloaded or created:"
!find ./models -name "*.xml" -o -name "*.bin"




%%writefile classification_pneumonia_job.sh

# Store input arguments: <output_directory> <device> <fp_precision> <input_file>
OUTPUT_FILE=$1
DEVICE=$2
FP_MODEL=$3
INPUT_FILE="$4"

# The default path for the job is the user's home directory,
#  change directory to where the files are.
echo VENV_PATH=$VENV_PATH
echo OPENVINO_RUNTIME=$OPENVINO_RUNTIME
echo INPUT_FILE=$INPUT_FILE
echo FP_MODEL=$FP_MODEL
echo INPUT_TILE=$INPUT_FILE
echo NUM_REQS=$NUM_REQS

# Follow this order of setting up environment for openVINO 2022.1.0.553
echo "Activating a Python virtual environment from ${VENV_PATH}..."
source ${VENV_PATH}/bin/activate
echo "Activating OpenVINO variables from ${OPENVINO_RUNTIME}..."
source ${OPENVINO_RUNTIME}/setupvars.sh


cd $PBS_O_WORKDIR

# Make sure that the output directory exists.
mkdir -p $OUTPUT_FILE

# Set inference model IR files using specified precision
MODELPATH=models/${FP_MODEL}/model.xml
pip3 install Pillow
# Run the pneumonia detection code
python3 classification_pneumonia.py -m $MODELPATH \
                                    -i "$INPUT_FILE" \
                                    -o $OUTPUT_FILE \
                                    -d $DEVICE