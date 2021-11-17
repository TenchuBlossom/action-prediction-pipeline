#!/bin/bash

#SBATCH --ntasks 20
#SBATCH --nodes 1
#SBATCH --time 25:0:0
#SBATCH --qos castlespowergpu
#SBATCH --mail-type ALL

set -e
module purge; module load bluebear

module load Python/3.9.5-GCCcore-10.3.0-bare

export VENV_DIR="${HOME}/virtual-environments"
export VENV_PATH="${VENV_DIR}/my-virtual-env-${BB_CPU}"

# Create a master venv directory if necessary
mkdir -p "${VENV_DIR}"

# Check if virtual environment exists and create it if not
if [[ ! -d ${VENV_PATH} ]]; then
    python3 -m venv --system-site-packages "${VENV_PATH}"
fi

# Activate the virtual environment
# shellcheck disable=SC2086
source ${VENV_PATH}/bin/activate

# install python dependencies
pip install pyyaml
pip install pandas
pip install scikit-learn
pip install catboost
pip install ray
pip install numpy
pip install matplotlib
pip install alive-progress
pip install tqdm
pip install seaborn
pip install mergedeep

# Execute pipeline
python -m pipelines.end_to_end.CB2Pipeline
