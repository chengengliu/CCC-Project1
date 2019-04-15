#!/bin/bash
#SBATCH --partition physical
#SBATCH --time=00:50:00
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=1
#SBATCH --job-name=2nodes_8cores

#SBATCH --mail-user=chengengl@student.unimelb.edu.au
#SBATCH --mail-type=FAIL

echo "1node1core"

module load Python/3.6.4-intel-2017.u2-GCC-6.2.0-CUDA9
time mpiexec python filter.py