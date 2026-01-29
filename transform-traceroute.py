import argparse
import gzip
import json
import logging
import os
import subprocess as sp
import sys
from collections import defaultdict
from ipaddress import ip_address
from json import JSONDecodeError
from typing import Tuple

import pandas as pd

from defines import NO_MONITOR_IP

INPUT_SUFFIX = '.warts.gz'
OUTPUT_SUFFIX = '.json.gz'
VALID_MODES = {'prefix_probing', 'probe_data'}


def transform(ark: dict, ark_metadata: dict) -> dict:
    atlas = {'fw': 5020,  # Required for the parser
             'prb_id': ark_metadata['hostname'],
             'msm_id': 0,
             'ark_metadata': ark_metadata,
             'dst_addr': ark['dst'],
             'af': 4,
             'timestamp': ark['start']['sec'],
             'from': ark['src'],
             'result': list()
             }
    if ip_address(ark['src']).is_private:
        if ark_metadata['fake_ip'] == NO_MONITOR_IP:
            logging.debug(f'Failed to replace private source IP {ark["src"]} for monitor {ark_metadata["hostname"]} '
                          f'due to missing prefix entry.')
            return dict()
        atlas['from'] = ark_metadata['fake_ip']

    stop_reason = ark['stop_reason']
    has_valid_hop = False

    result = defaultdict(list)
    # There are traceroutes without any valid hops.
    if 'hops' in ark:
        for hop in ark['hops']:
            ttl = hop['probe_ttl']
            icmp_type = hop['icmp_type']
            reply = {'from': hop['addr'],
                     'rtt': hop['rtt']}
            if icmp_type == 0 or icmp_type == 11:
                # "Echo reply" or "Time exceeded" (normal hop behavior)
                result[ttl].append(reply)
                if not has_valid_hop and not ip_address(reply['from']).is_private:
                    has_valid_hop = True
            elif icmp_type == 3:
                if stop_reason != 'UNREACH':
                    logging.warning(f'Got ICMP type 3 hop, but stop reason is '
                                    f'not "UNREACH", but {stop_reason}.')
                # Destination unreachable
                icmp_code = hop['icmp_code']
                if icmp_code == 0:
                    # Network unreachable
                    reply['err'] = 'N'
                elif icmp_code == 1:
                    # Host unreachable
                    reply['err'] = 'H'
                elif icmp_code == 2:
                    # Protocol unreachable
                    reply['err'] = 'P'
                elif icmp_code == 3:
                    # Port unreachable
                    reply['err'] = 'p'
                elif icmp_code in (9, 10, 13):
                    # Administratively forbidden
                    reply['err'] = 'A'
                else:
                    reply['err'] = icmp_code
                result[ttl].append(reply)
            else:
                logging.warning(f'Unknown ICMP type: {icmp_type}')
    if not has_valid_hop:
        logging.debug(f'No valid hop in traceroute {ark}')
        return dict()

    # Insert non-responsive hops
    star_reply = {'x': '*'}
    last_ttl = 0
    for ttl in sorted(result.keys()):
        while last_ttl + 1 < ttl:
            # Gap in the hops
            last_ttl += 1
            result[last_ttl].append(star_reply)
        last_ttl = ttl

    if stop_reason == 'GAPLIMIT':
        # GAPLIMIT is triggered after five consecutive timeouts.
        for _ in range(5):
            last_ttl += 1
            result[last_ttl].append(star_reply)
    elif stop_reason == 'LOOP':
        # In case of a LOOP the traceroute stops without any errors
        # in the hops, so add a fake non-responsive hop to show that
        # there was a problem.
        last_ttl += 1
        result[last_ttl].append(star_reply)
    elif stop_reason == 'COMPLETED':
        # Sanity check.
        reply_found = False
        for reply in result[last_ttl]:
            if reply['from'] == ark['dst']:
                reply_found = True
                break
        if not reply_found:
            logging.warning(f'Stop reason is COMPLETED, but destination '
                            f'address {ark["dst"]} not in last result: '
                            f'{result[last_ttl]}')

    # Convert to structure used in Atlas.
    for ttl, replies in sorted(result.items(), key=lambda t: t[0]):
        atlas['result'].append({'hop': ttl, 'result': replies})

    return atlas


def warts2json(warts_file: str, ark_metadata: dict) -> list:
    cycle_id = None
    if 'cycle_id' in ark_metadata:
        cycle_id = ark_metadata['cycle_id']

    with sp.Popen(['sc_warts2json'],
                  stdin=sp.PIPE,
                  stdout=sp.PIPE) as process:
        if warts_file.endswith('.gz'):
            with gzip.open(warts_file, 'rb') as f:
                stdout, stderr = process.communicate(f.read())
        else:
            with open(warts_file, 'rb') as f:
                stdout, stderr = process.communicate(f.read())
        if process.returncode != 0:
            logging.error(f'sc_warts2json exited with non-zero returncode: '
                          f'{process.returncode}')
            return list()
    json_data = stdout.decode('utf-8').split('\n')
    logging.debug(f'Read {len(json_data)} JSON lines.')

    traceroutes = list()
    for traceroute in json_data:
        # Ignore empty lines.
        if not traceroute:
            continue

        try:
            traceroute_json = json.loads(traceroute)
        except JSONDecodeError as e:
            logging.warning(f'Failed to parse JSON: {traceroute}')
            logging.warning(e)
            continue

        if 'type' not in traceroute_json:
            logging.warning(f'Skipping malformed JSON entry: '
                            f'{traceroute_json}')
            continue

        if traceroute_json['type'].startswith('cycle'):
            # Sanity check.
            if 'hostname' not in traceroute_json or 'id' not in traceroute_json:
                logging.warning(f'Skipping malformed cycle entry: '
                                f'{traceroute_json}')
                continue
            if cycle_id is not None \
                    and traceroute_json['id'] != cycle_id:
                logging.error(f'Cycle id extracted from filename does not '
                              f'match metadata in warts file. File cycle id: '
                              f'{cycle_id} | Metadata cycle id: '
                              f'{traceroute_json["id"]}')
                return list()

        elif traceroute_json['type'] == 'trace':
            traceroutes.append(traceroute_json)
        else:
            logging.warning(f'Skipping unknown JSON entry type: '
                            f'{traceroute_json}')

    logging.debug(f'Read {len(traceroutes)} traceroutes.')

    return traceroutes


def generate_output_file_name(input_file: str, output_dir: str) -> str:
    if not input_file.endswith(INPUT_SUFFIX):
        logging.error(f'Expected {INPUT_SUFFIX} input file.')
        return str()
    base = os.path.basename(input_file)
    return f'{output_dir}{base[:-len(INPUT_SUFFIX)]}{OUTPUT_SUFFIX}'


def write_output(traceroutes: list, output_file: str) -> None:
    with gzip.open(output_file, 'wt') as f:
        for traceroute in traceroutes:
            if not traceroute:
                continue
            json.dump(traceroute, f)
            f.write('\n')


def get_host_and_cycle(warts_file: str) -> Tuple[str, int]:
    file_name = os.path.basename(warts_file)
    # Files before 2018-09-16 used a different naming scheme:
    #   daily.l7.t<team-number>.<cycle>.<date>.<hostname>.warts.gz
    # From 2018-09-16 onwards:
    #   <hostname>.team-probing.<cycle>.<date>.warts.gz
    # See also: https://publicdata.caida.org/datasets/topology/ark/ipv4/probe-data/README.txt
    if file_name.startswith('daily'):
        file_name_split = file_name.split('.')
        cycle = file_name_split[3]
        hostname = file_name_split[5]
    else:
        hostname, _, cycle, _ = file_name.split('.', maxsplit=3)
    cycle_id = int(cycle[1:])
    return hostname, cycle_id


def get_host_and_date(warts_file: str) -> Tuple[str, str]:
    file_name = os.path.basename(warts_file)
    hostname, date, _ = file_name.split('.', maxsplit=2)
    return hostname, date


def main() -> None:
    desc = """Parse traceroutes from a (gzipped) warts file to a gzipped ndjson file
           that contains the traceroutes in RIPE Atlas format.

           This script uses a list of fake monitor IPs (generated by
           create-fake-monitor-ips.py) to ensure that the results will have a public
           source IP address, which is always the case in Atlas' 'from' field.

           If a monitor uses a private IP and no fake IP could be derived, e.g., because
           the monitor AS does not announce any prefixes, the results from this monitor
           are ignored entirely.

           The script also ignores traceroutes consisting entirely of private IPs and/or
           timeouts."""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('input', help='input warts file')
    parser.add_argument('fake_monitor_ips', help='list with fake monitor IPs derived from ASN')
    parser.add_argument('mode',
                        help=f'warts file mode based on the data set. Must be '
                             f'one of: {VALID_MODES}')
    parser.add_argument('output_dir')
    parser.add_argument('-f', '--force',
                        action='store_true',
                        help='overwrite existing files')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        level=logging.INFO,
        filename='transform-traceroute.log',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    input_file = args.input
    output_dir = args.output_dir
    if not output_dir.endswith('/'):
        output_dir += '/'
    output_file = generate_output_file_name(input_file, output_dir)
    if not output_file:
        sys.exit(1)

    if output_file and os.path.exists(output_file) and not args.force:
        logging.info(f'Output file {output_file} already exists. Use -f to '
                     f'force overwrite.')
        return

    mode = args.mode
    if mode not in VALID_MODES:
        logging.error(f'Invalid mode specified: {mode}. Must be one of: '
                      f'{VALID_MODES}')
        sys.exit(1)

    if mode == 'probe_data':
        hostname, cycle_id = get_host_and_cycle(input_file)
        ark_metadata = {'mode': 'probe_data',
                        'hostname': hostname,
                        'cycle_id': cycle_id}
    else:
        hostname, date = get_host_and_date(input_file)
        ark_metadata = {'mode': 'prefix_probing',
                        'hostname': hostname,
                        'date': date}

    fake_monitor_ips_file = args.fake_monitor_ips
    fake_monitor_ips = pd.read_csv(fake_monitor_ips_file, index_col=0)  # Map to hostname
    if hostname not in fake_monitor_ips.index:
        logging.warning('Hostname not found in monitor prefix file.')
        sys.exit(1)

    ark_metadata['asn'] = str(fake_monitor_ips.loc[hostname].asn)
    ark_metadata['fake_ip'] = fake_monitor_ips.loc[hostname].ip
    if ark_metadata['fake_ip'] == '0':
        logging.warning(f'No fake IP found for monitor {hostname}. If the monitor uses a private source IP, results '
                        f'will be filtered.')

    logging.debug(f'Reading file {input_file}')
    traceroutes = warts2json(input_file, ark_metadata)

    atlas_traceroutes = [transform(traceroute, ark_metadata)
                         for traceroute in traceroutes]
    if not atlas_traceroutes:
        sys.exit(1)
    os.makedirs(output_dir, exist_ok=True)
    write_output(atlas_traceroutes, output_file)


if __name__ == '__main__':
    main()
    sys.exit(0)
