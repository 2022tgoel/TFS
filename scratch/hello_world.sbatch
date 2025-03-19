#!/bin/bash
#SBATCH -n 3
#SBATCH -N 3
#SBATCH -p mit_normal
python3 hello_world.py
