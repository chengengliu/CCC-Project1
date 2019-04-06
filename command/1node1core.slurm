#!/bin/bash
#SBATCH --partition physical
#SBATCH --time=00:10:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --job-name=1node_1core

#SBATCH --mail-user=chengengl@student.unimelb.edu.au
#SBATCH --mail-type=FAIL

module load Python/3.6.4-intel-2017.u2-GCC-6.2.0-CUDA9
time mpiexec python filter.py