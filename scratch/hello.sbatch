#!/bin/bash
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --mem=2GB
#SBATCH -p mit_normal
#SBATCH -C rocky8
#SBATCH --job-name=hello

srun --output=slurm-%t-%j.out --error=slurm-%t-%j.err hostname