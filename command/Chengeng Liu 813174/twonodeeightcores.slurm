#!/bin/bash
# 2 nodes, 2 tasks per node = 4 cores
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --time=0-12:00:00
#SBATCH --partition=physical

# Load required modules
module load Python/3.5.2-goolf-2015a
# Launch multiple process python code
time mpiexec -n 8 python3 Assignment1.py