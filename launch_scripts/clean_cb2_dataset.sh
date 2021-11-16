#!/bin/bash

#SBATCH --ntasks 20
#SBATCH --nodes 1
#SBATCH --time 25:0:0
#SBATCH --qos castlespowergpu
#SBATCH --mail-type ALL

set -e
module purge; module load bluebear

pwd
module load Miniconda3/4.9.2
module load Python/3.9.5-GCCcore-10.3.0-bare

conda activate envs/ml-env

python -m pipelines.end_to_end.CB2Pipeline
