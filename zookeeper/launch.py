#!/usr/bin/env python3

import os
import socket
import subprocess
import argparse
import shutil

def parse_slurm_nodelist(nodelist):
    nodes = []
    for part in nodelist.split(","):
        if "[" in part:
            # Handle range format like node[2704-2705]
            prefix = part[:part.find("[")]
            range_str = part[part.find("[")+1:part.find("]")]
            start, end = map(int, range_str.split("-"))
            nodes.extend(f"{prefix}{i}" for i in range(start, end + 1))
        else:
            nodes.append(part)
    return sorted(nodes)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # Usage: python3 launch.py start
    # Usage: python3 launch.py status
    # Usage: python3 launch.py stop
    parser.add_argument("command", type=str, choices=["start", "status", "stop", "restart"])
    args = parser.parse_args()

    if args.command == "start":
        # Configure Java heap size
        os.environ["JAVA_TOOL_OPTIONS"] = "-Xms512m -Xmx2g"

        PEERS = parse_slurm_nodelist(os.environ["SLURM_NODELIST"])
        print(PEERS)

        my_name = socket.gethostname()
        my_rank = PEERS.index(my_name)

        data_dir = "/scratch/zookeeper"
        # Remove all existing files in the data directory
        shutil.rmtree(data_dir, ignore_errors=True)
        shutil.rmtree("/scratch/files", ignore_errors=True)
        # Create the data directory
        os.makedirs(data_dir, exist_ok=True)

        with open(f"/scratch/zookeeper/myid", "w") as f:
            f.write(f"{my_rank}")

        serverConfig = "\n".join(f"server.{i}={peer}:2888:3888" for i, peer in enumerate(PEERS))
        config = f"""tickTime=2000
dataDir={data_dir}
clientPort=2181
initLimit=5
syncLimit=2
{serverConfig}"""

        with open(f"/scratch/zoo.cfg", "w") as f:
            f.write(config)

        os.environ["ZOOCFG"] = "zoo.cfg"
        os.environ["ZOOCFGDIR"] = "/scratch"
        subprocess.run(["./zookeeper/apache-zookeeper-3.7.2-bin/bin/zkServer.sh", "start"], env=os.environ)
    elif args.command == "status":
        os.environ["ZOOCFG"] = "zoo.cfg"
        os.environ["ZOOCFGDIR"] = "/scratch"
        subprocess.run(["./zookeeper/apache-zookeeper-3.7.2-bin/bin/zkServer.sh", "status"], env=os.environ)
    elif args.command == "stop":
        os.environ["ZOOCFG"] = "zoo.cfg"
        os.environ["ZOOCFGDIR"] = "/scratch"
        subprocess.run(["./zookeeper/apache-zookeeper-3.7.2-bin/bin/zkServer.sh", "stop"], env=os.environ)
    elif args.command == "restart":
        os.environ["ZOOCFG"] = "zoo.cfg"
        os.environ["ZOOCFGDIR"] = "/scratch"
        subprocess.run(["./zookeeper/apache-zookeeper-3.7.2-bin/bin/zkServer.sh", "restart"], env=os.environ)
