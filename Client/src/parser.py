import argparse
import xml.etree.ElementTree as ET
import subprocess
import sys

class ManifestParser:
    def __init__(self):
        self.parser = argparse.ArgumentParser(description='Parse manifest and run benchmark client')
        self._add_arguments()

    def _add_arguments(self):
        self.parser.add_argument('--size', type=int, default=100,
                               help='The size of each transaction in bytes')
        self.parser.add_argument('--n_users', type=int, default=1000,
                               help='Number of users in small-bank')
        self.parser.add_argument('--skew_factor', type=float, default=0.01,
                               help='Skew factor for users in small-bank')
        self.parser.add_argument('--prob_choose_mtx', type=float, default=1.0,
                               help='Probability of choosing modifying transactions')
        self.parser.add_argument('--rate', type=int, default=100,
                               help='The rate (txs/s) at which to send transactions')
        self.parser.add_argument('--num_shards', type=int, default=2,
                               help='Number of shards')
        self.parser.add_argument('--manifest', type=str, default='manifest.xml',
                               help='Path to manifest.xml file')

    def parse_manifest(self, manifest_path):
        try:
            tree = ET.parse(manifest_path)
            root = tree.getroot()
            
            # Extract IP addresses from manifest
            nodes = []
            for node in root.findall(".//{*}interface"):
                ip = node.find(".//{*}ip")
                if ip is not None:
                    address = ip.get('address')
                    if address:
                        # Add default port 8000
                        nodes.append(f"{address}:8000")
            
            return nodes
        except ET.ParseError as e:
            print(f"Error parsing manifest file: {e}")
            sys.exit(1)
        except FileNotFoundError:
            print(f"Manifest file not found: {manifest_path}")
            sys.exit(1)

    def run(self):
        args = self.parser.parse_args()
        nodes = self.parse_manifest(args.manifest)

        if not nodes:
            print("No nodes found in manifest file")
            sys.exit(1)

        # Construct cargo run command
        cmd = [
            "cargo", "run", "--",
            f"--size={args.size}",
            f"--n_users={args.n_users}",
            f"--skew_factor={args.skew_factor}",
            f"--prob_choose_mtx={args.prob_choose_mtx}",
            f"--rate={args.rate}",
            f"--num_shards={args.num_shards}"
        ]

        # Add node addresses
        for node in nodes:
            cmd.append(f"--nodes={node}")

        try:
            # Run the command
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running benchmark client: {e}")
            sys.exit(1)

if __name__ == "__main__":
    parser = ManifestParser()
    parser.run()