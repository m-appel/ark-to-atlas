import argparse
import configparser
import gzip
import json
import logging
import os
import sys

from kafka_wrapper.kafka_writer import KafkaWriter
from shared_functions import parse_timestamp_argument

INPUT_FILE_SUFFIX = '.json.gz'


def check_config(config_path: str) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(config_path)
    try:
        config.get('output', 'kafka_topic')
        config.get('kafka', 'bootstrap_servers')
    except configparser.NoSectionError as e:
        logging.error(f'Missing section in config file: {e}')
        return configparser.ConfigParser()
    except configparser.NoOptionError as e:
        logging.error(f'Missing option in config file: {e}')
        return configparser.ConfigParser()
    try:
        if config.has_option('kafka', 'num_partitions'):
            config.getint('kafka', 'num_partitions')
        if config.has_option('kafka', 'replication_factor'):
            config.getint('kafka', 'replication_factor')
        if config.has_option('kafka', 'retention_ms'):
            config.getint('kafka', 'retention_ms')
    except ValueError as e:
        logging.error(f'Failed to parse some option of the config file: {e}')
        return configparser.ConfigParser()

    return config


def get_timestamp_from_filename(filename: str) -> int:
    ts_str = filename[:-len(INPUT_FILE_SUFFIX)]
    if not ts_str.isdigit():
        logging.error(f'Failed to extract timestamp from filename: {filename}')
        return 0
    return int(ts_str)


def get_input_files(input_dir: str, start_ts: int, stop_ts: int) -> list:
    logging.info(f'Reading input directory: {input_dir}')
    if start_ts:
        logging.info(f'Start timestamp: {start_ts}')
    if stop_ts:
        logging.info(f'Stop timestamp: {stop_ts}')
    if start_ts is None and stop_ts is None:
        logging.info('Reading entire directory.')
    ret = list()

    for entry in os.scandir(input_dir):
        if not entry.is_file() or not entry.name.endswith(INPUT_FILE_SUFFIX):
            continue

        file_ts = get_timestamp_from_filename(entry.name)
        if file_ts == 0:
            continue

        if (start_ts is not None and file_ts < start_ts
                or stop_ts is not None and file_ts >= stop_ts):
            continue
        ret.append((file_ts, f'{input_dir}{entry.name}'))
    ret.sort()
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
                logging.warning(f'Missing "timestamp" key in line: {json_data}')
                continue
            ret.append((json_data['timestamp'], json_data))
    ret.sort(key=lambda t: t[0])
    logging.debug(f'Read {len(ret)} lines from input file {input_file}')
    return ret


def main() -> None:
    desc = """Read bin files in order from the bin directory, sort traceroutes from each
           bin file before pushing them to Kafka.

           --start and --stop parameters can be used to restrict the time range that
           will be pushed, e.g., if not all bins should be pushed or only part of a bin."""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('bin_dir')
    parser.add_argument('config')
    parser.add_argument('-s', '--start',
                        help='Start timestamp (as UNIX epoch in seconds or '
                             'milliseconds, or in YYYY-MM-DDThh:mm format)')
    parser.add_argument('-e', '--stop',
                        help='Stop timestamp (as UNIX epoch in seconds or '
                        'milliseconds, or in YYYY-MM-DDThh:mm format)')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
        filename='push-bins-to-kafka.log'
    )

    logging.info(f'Started {sys.argv}')

    config = check_config(args.config)
    if not config.sections():
        sys.exit(1)

    input_dir = args.bin_dir
    if not input_dir.endswith('/'):
        input_dir += '/'

    start_ts = None
    start_ts_arg = args.start
    if start_ts_arg:
        start_ts = parse_timestamp_argument(start_ts_arg)
        if start_ts == 0:
            logging.error(f'Invalid start timestamp specified: {start_ts_arg}')
            sys.exit(1)

    stop_ts = None
    stop_ts_arg = args.stop
    if stop_ts_arg:
        stop_ts = parse_timestamp_argument(stop_ts_arg)
        if stop_ts == 0:
            logging.error(f'Invalid stop timestamp specified: {stop_ts_arg}')
            sys.exit(1)

    input_files = get_input_files(input_dir, start_ts, stop_ts)
    if not input_files:
        return

    output_topic = config.get('output', 'kafka_topic')
    bootstrap_servers = config.get('kafka', 'bootstrap_servers')
    topic_config = None
    if config.has_option('kafka', 'retention_ms'):
        topic_config = {'retention.ms': config.getint('kafka', 'retention_ms')}
    num_partitions = config.getint('kafka', 'num_partitions', fallback=10)
    replication_factor = config.getint('kafka', 'replication_factor', fallback=2)

    writer = KafkaWriter(output_topic,
                         bootstrap_servers,
                         num_partitions,
                         replication_factor,
                         topic_config)
    with writer:
        for file_timestamp, input_file in input_files:
            traceroutes = read_input_file(input_file)
            for timestamp, traceroute in traceroutes:
                if (start_ts is not None and timestamp < start_ts
                        or stop_ts is not None and timestamp >= stop_ts):
                    continue
                key = None
                if 'ark_metadata' in traceroute and 'hostname' in traceroute['ark_metadata']:
                    key = traceroute['ark_metadata']['hostname']
                elif 'from' in traceroute:
                    key = traceroute['from']
                else:
                    logging.warning('No valid key field found ("ark_metadata>hostname", "from"), setting key to None')
                writer.write(key, traceroute, timestamp * 1000)


if __name__ == '__main__':
    main()
    sys.exit(0)
