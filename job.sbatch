#!/bin/bash
#SBATCH -n 2
#SBATCH -N 2
#SBATCH -p mit_normal

RUST_BACKTRACE=1 ./target/debug/tfs