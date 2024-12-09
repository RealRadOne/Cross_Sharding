import argparse
import subprocess
import sys

# Hardcoded test nodes
NODES = [
    "128.110.218.249:22",
    "128.110.218.240:22"
]

parser = argparse.ArgumentParser(description='Run benchmark client')
parser.add_argument('--size', type=int, default=100)
parser.add_argument('--n_users', type=int, default=1000)
parser.add_argument('--skew_factor', type=float, default=0.01)
parser.add_argument('--prob_choose_mtx', type=float, default=1.0)
parser.add_argument('--rate', type=int, default=100)

args = parser.parse_args()

print("Using test nodes: {}".format(NODES))

cmd = [
    "cargo", "run", "--",
    "--size={}".format(args.size),
    "--n_users={}".format(args.n_users),
    "--skew_factor={}".format(args.skew_factor),
    "--prob_choose_mtx={}".format(args.prob_choose_mtx),
    "--rate={}".format(args.rate),
    "--num_shards=2"
]

for node in NODES:
    cmd.append("--nodes={}".format(node))

subprocess.call(cmd)