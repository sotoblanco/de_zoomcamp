import json
import sys
from pathlib import Path

from IPython.display import Markdown, display
from openvino.runtime import Core

sys.path.append("../utils")
#from notebook_utils import DeviceNotFoundAlert, NotebookAlert

base_model_dir = Path("model")
omz_cache_dir = Path("cache")
precision = "FP16"
precision_2 = "FP32"

# Check if an iGPU is available on this system to use with Benchmark App.
ie = Core()
gpu_available = "GPU" in ie.available_devices

print(
    f"base_model_dir: {base_model_dir}, omz_cache_dir: {omz_cache_dir}, gpu_availble: {gpu_available}"
)


model_name = "mobilenet-ssd"

# download the model
download_command = (
    f"omz_downloader --name {model_name} --output_dir {base_model_dir} --cache_dir {omz_cache_dir}"
)
display(Markdown(f"Download command: `{download_command}`"))
display(Markdown(f"Downloading {model_name}..."))
! $download_command



model_info_output = %sx omz_info_dumper --name $model_name
model_info = json.loads(model_info_output.get_nlstr())

if len(model_info) > 1:
    NotebookAlert(
        f"There are multiple IR files for the {model_name} model. The first model in the "
        "omz_info_dumper output will be used for benchmarking. Change "
        "`selected_model_info` in the cell below to select a different model from the list.",
        "warning",
    )

model_info


selected_model_info = model_info[0]
MODEL_PATH = (
    base_model_dir
    / Path(selected_model_info["subdirectory"])
    / Path(f"{precision_2}/{selected_model_info['name']}.xml")
)
print(MODEL_PATH, "exists:", MODEL_PATH.exists())


# Run the following cell to use the Model Optimizer to create the FP16 and FP32 model IR files
!mo \
--input_model raw_models/public/mobilenet-ssd/mobilenet-ssd.caffemodel \
--input_shape=[1,3,300,300] \
--data_type FP16 \
--output_dir models/mobilenet-ssd/FP16 \
--mean_values [127.5,127.5,127.5] \
--scale_values [0.007843,0.007843,0.007843]

# Create FP32 IR files
!mo \
--input_model raw_models/public/mobilenet-ssd/mobilenet-ssd.caffemodel \
--input_shape=[1,3,300,300] \
--data_type FP32 \
--output_dir models/mobilenet-ssd/FP32 \
--mean_values [127.5,127.5,127.5] \
--scale_values [0.007843,0.007843,0.007843]

# find all resulting IR files
!echo "\nAll IR files that were created:"
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
MODELPATH=models/${FP_MODEL}/mobilenet-ssd.xml
pip3 install Pillow
# Run the pneumonia detection code
python3 classification_pneumonia.py -m $MODELPATH \
                                    -i "$INPUT_FILE" \
                                    -o $OUTPUT_FILE \
                                    -d $DEVICE