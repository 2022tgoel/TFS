#!/bin/bash
# Parse the jobs with name tfs and kill them
cargo build
squeue -u $USER -n tfs -h -o "%i" | xargs scancel
rm slurm-*
sbatch job.sbatch