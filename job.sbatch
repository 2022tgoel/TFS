#!/bin/bash
#SBATCH -N 3
#SBATCH --ntasks-per-node=1
#SBATCH -p mit_normal
#SBATCH -C rocky8
#SBATCH --mem=4GB
#SBATCH --job-name=tfs

srun --output=slurm-%t-%j.out --error=slurm-%t-%j.err bash -c '
    ./zookeeper/launch.py start
    sleep 2
    ./zookeeper/launch.py status
    ./target/debug/chunkserver
'
