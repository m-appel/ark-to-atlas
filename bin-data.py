import argparse
import gzip
import json
import logging
import os
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone

from shared_functions import sanitize_dir

INPUT_FILE_SUFFIX = '.json.gz'
OUTPUT_FILE_SUFFIX = '.json.gz'


def get_input_files(input_dir: str) -> list:
    logging.info(f'Reading input directory: {input_dir}')
    ret = list()

    for entry in os.scandir(input_dir):
        if not entry.is_file() or not entry.name.endswith(INPUT_FILE_SUFFIX):
            continue
        ret.append(f'{input_dir}{entry.name}')
    logging.info(f'Found {len(ret)} input files.')
    return ret


def read_input_file(input_file: str) -> list:
    logging.debug(f'Reading input file: {input_file}')
    ret = list()
    with gzip.open(input_file, 'rt') as f:
        for line in f:
            try:
                json_data = json.loads(line)
            except json.JSONDecodeError as e:
                logging.warning(f'Failed to decode input line: {e}')
                logging.warning(line.strip())
                continue
            if 'timestamp' not in json_data:
                logging.warning(f'Missing "timestamp" key in line: '
                                f'{json_data}')
                continue
            ret.append((json_data['timestamp'], line))
    logging.debug(f'Read {len(ret)} lines from input file {input_file}')
    return ret


def find_offsets(traceroutes: list, bin_size: int) -> list:
    ret = list()
    first_ts = datetime.fromtimestamp(traceroutes[0][0], tz=timezone.utc)
    first_bin_hour = first_ts.hour - (first_ts.hour % bin_size)
    curr_bin_start = first_ts.replace(hour=first_bin_hour, minute=0, second=0)
    curr_bin_end = curr_bin_start + timedelta(hours=bin_size)

    bin_start_offset = 0
    offset = 0
    while offset < len(traceroutes):
        curr_bin_end_ts = int(curr_bin_end.timestamp())
        while offset < len(traceroutes) and traceroutes[offset][0] < curr_bin_end_ts:
            offset += 1
        # End of list reached or next bin.
        if offset > bin_start_offset:
            # Add bin entry only if there is at least one traceroute.
            ret.append((int(curr_bin_start.timestamp()), (bin_start_offset, offset)))

        bin_start_offset = offset
        curr_bin_start = curr_bin_end
        curr_bin_end = curr_bin_start + timedelta(hours=bin_size)

    return ret


def assemble_worker_params(traceroutes: list, output_dir: str, offsets: list) -> list:
    ret = list()
    for bin_start, (offset_start, offset_end) in offsets:
        bin_file = f'{output_dir}{bin_start}{OUTPUT_FILE_SUFFIX}'
        ret.append((traceroutes, offset_start, offset_end, bin_file))
    return ret


def write_offsets_to_bin(params: tuple) -> None:
    traceroutes, offset_start, offset_end, bin_file = params
    logging.debug(f'{len(traceroutes)} {offset_start} {offset_end} {bin_file}')
    with gzip.open(bin_file, 'at') as f:
        for timestamp, traceroute in traceroutes[offset_start:offset_end]:
            f.write(traceroute)


def main() -> None:
    desc = """Read gzipped ndjson files containing traceroute data in RIPE Atlas format
           from the input directory and write them into bins according to the specified
           size, all in parallel.

           Depending on the number of files in the input directory, may require a lot of
           memory since all files are read first, then sorted, before being written to
           the bin files.

           This script can be run sequentially on different input directories with the
           same output directory, since data is appended to existing bin files. However,
           you should not run this script itself in parallel since writing the bin files
           might not be thread safe.

           Note also that the traceroutes within each bin are not sorted by time if you
           ran this script sequentially."""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('input_dir')
    parser.add_argument('output_dir')
    parser.add_argument('-b', '--bin-size',
                        type=int,
                        default=1,
                        help='bin size (hours)')
    parser.add_argument('-n', '--num-proc',
                        type=int,
                        default=4,
                        help='number of parallel processes')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='bin-data.log',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logging.info(f'Started {sys.argv}')

    input_dir = sanitize_dir(args.input_dir)
    output_dir = sanitize_dir(args.output_dir)

    bin_size = args.bin_size
    if bin_size < 1 or bin_size > 23:
        logging.error(f'Invalid bin size specified: {bin_size}. Should be in '
                      f'range 1-23.')
        sys.exit(1)

    start_all = datetime.now().timestamp()
    input_files = get_input_files(input_dir)
    logging.info('Reading input files...')
    num_proc = args.num_proc
    with ProcessPoolExecutor(num_proc) as p:
        traceroutes = [tr for l in p.map(read_input_file, input_files) for tr in l]
    read_end = datetime.now().timestamp()
    logging.info(f'Read completed in {read_end - start_all:.2f} seconds')

    if not traceroutes:
        logging.info('No traceroutes found, exiting early.')
        sys.exit(0)

    logging.info('Sorting traceroutes...')
    traceroutes.sort()
    sort_end = datetime.now().timestamp()
    logging.info(f'Sort completed in {sort_end - read_end:.2f} seconds')

    logging.info('Calculating offsets...')
    offsets = find_offsets(traceroutes, bin_size)
    offset_end = datetime.now().timestamp()
    logging.info(f'Calculating offsets completed in {offset_end - sort_end:.2f} seconds')

    worker_params = assemble_worker_params(traceroutes, output_dir, offsets)

    logging.info('Writing traceroutes to bins...')
    executor = ThreadPoolExecutor(num_proc)
    res = [executor.submit(write_offsets_to_bin, param) for param in worker_params]
    wait(res)
    bin_end = datetime.now().timestamp()
    logging.info(f'Writing completed in {bin_end - sort_end:.2f} seconds')

    end_all = datetime.now().timestamp()
    logging.info(f'Process finished in {end_all - start_all:.2f} seconds')


if __name__ == '__main__':
    main()
    sys.exit(0)
