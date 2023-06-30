import argparse
import bz2
import logging
import pickle
import sys
from ipaddress import ip_network
from socket import AF_INET

import pandas as pd

from defines import NO_MONITOR_IP


def main() -> None:
    desc = """Create a fake IP for each monitor that will map to the correct ASN based on the
           specified rtree."""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('monitor_list')
    parser.add_argument('rtree')
    parser.add_argument('output_file')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    monitor_info = pd.read_csv(args.monitor_list)
    with bz2.open(args.rtree, 'rb') as f:
        rtree = pickle.load(f)
    prefix_map = dict()
    for node in rtree.nodes():
        asn = node.data['as']
        if node.family != AF_INET:
            # Only IPv4.
            continue
        if asn not in prefix_map \
                or asn in prefix_map and prefix_map[asn].prefixlen < node.prefixlen:
            # Chose most specific prefix for best best-match probability.
            prefix_map[asn] = ip_network(node.prefix)

    output = list()
    used_ips = set()
    for entry in monitor_info.itertuples():
        asn_str = str(entry.asn)
        if asn_str not in prefix_map:
            logging.warning(f'Failed to find map entry for monitor {entry}')
            output.append((entry.name, entry.asn, NO_MONITOR_IP))
            continue
        monitor_prefix = prefix_map[asn_str]
        monitor_ip = monitor_prefix.network_address + 1
        # Ensure unique IPs for multiple monitors in the same AS.
        while monitor_ip in used_ips:
            monitor_ip += 1
        if monitor_ip.is_private:
            logging.error(f'Derived monitor IP is private ({monitor_ip}) for monitor {entry}')
            continue
        used_ips.add(monitor_ip)
        output.append((entry.name, entry.asn, monitor_ip))
    output_pd = pd.DataFrame(output, columns=('name', 'asn', 'ip'))
    output_pd.to_csv(args.output_file, index=False)


if __name__ == '__main__':
    main()
    sys.exit(0)
