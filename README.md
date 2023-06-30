# ark-to-atlas

Scripts to convert [CAIDA Ark](https://www.caida.org/projects/ark/) traceroutes to [RIPE Atlas
format](https://atlas.ripe.net/docs/apis/result-format/#version-5000-traceroute-v6-traceroute).

To emulate RIPE Atlas' `from` field, we want to have a public IP address for every
monitor. However, some monitors use a private IP and we only know their AS from CAIDA's
website. Therefore, we fetch the AS information and assign a unique "fake" IP, which is a
random IP taken from a prefix announced by the monitor's AS, to each monitor.

If the monitor uses a private IP and the AS does not announce any prefixes, the results
of the monitor will be ignored.

The conversion also removes traceroutes that only consist of private IPs and/or
timeouts.

In addition to conversion scripts, this repository contains some postprocessing scripts
to push the converted data to Kafka.

## Installation

Clone the repository and install the required dependencies.

```bash
git clone https://github.com/m-appel/ark-to-atlas.git
pip install -r requirements.txt
```

If you plan to work with Kafka, initialize the submodule and install the dependencies as
well.

```bash
git submodule update --init
pip install -r kafka_wrapper/requirements.txt
```

## Usage

Since the monitor table is generated via JavaScript, we can not easily fetch it fully automatic. Go
to [Archipelago Monitor Locations](https://www.caida.org/projects/ark/locations/) and make sure to
tick all checkboxes listed under `Locations Map` to get the full table. Then use the *inspector*
tool of the browser (**not** `Show Source`) and copy the HTML source into a file.  This script
automatically searches for the table inside the file so as long as you copy at least the table, the
script will find it.

After you downloaded the HTML table, parse it into a list.

```bash
python3 ./parse-monitor-table.py table.html monitor_list.csv
```

Create a list of fake monitor IPs based on the specified radix tree. This process
guarantees that each monitor gets a unique IP that will resolve to the correct ASN when
resolved with the specified radix tree.

```bash
python3 ./create-fake-monitor-ips.py monitor_list.csv rtree.pickle.bz2 fake_ips.csv
```

This process can fail for some monitors for different reasons. Some monitors are listed with
multiple ASes, e.g., `ord-us` with `20130_54728`, which makes it impossible to decide which AS to
use. Others do not announce any prefixes, e.g., `akl-nz` with `AS9503` and therefore do not show up
in the radix tree.

However, this does not necessarily mean that traceroutes from these monitors will be
ignored. If the source IP of the traceroute is not private, we do not need the fake IP
and can still use the data.

Finally, convert the traceroute data.

```bash
python3 ./transform-traceroute.py host.team-probing.c000000.YYYYmmdd.warts.gz fake_ips.csv probe_data output_dir/
```

You need to specify which kind of files you convert (either `probe_data` or
`prefix_probing`) as the naming scheme is different and other metadata is included in
the results based on the type.

Like mentioned above, if a monitor uses a private IP and no fake IP is contained in the
list, the results of that monitor will be ignored. The reason for this is that the
`from` field in Atlas traceroute results contains the public IP of the probe and we want
to replicate that behavior.

Also, traceroutes that consist entirely of private IPs and/or timeouts, will be ignored.

The results contain an additional field `ark_metadata` that provides handy information
if processed by a script which is aware that the results were transformed from Ark:

```
{
    'ark_metadata': {
        'mode': str([probe_data|prefix_probing]),
        'hostname': str,
        // ASN of the monitor according to fake_ips.csv entry
        'asn': str
        'fake_ip': str
        // probe_data mode only
        'cycle_id': int
        // prefix_probing mode only
        'date': str('YYYYmmdd')
    }
}
```

This concludes the conversion to the Atlas format itself. For further postprocessing and
pushing data into Kafka, read more below.

## Postprocessing and Kafka usage

To write data from multiple monitors to a single Kafka topic, we apply a two step
approach:

1. Aggregate data from all monitors into fixed-length bins
2. Push the bins sequentially to Kafka

Kafka requires that the messages are pushed ordered by timestamp, so we need to
aggregate and sort data from all monitors first. However, if we just put everything into
one big file we might run out of memory when pushing the data to the topic. This is why
we first sort the data into bins (usually in 1-hour increments) that have a more
manageable size.

First, separate data into (1-hour) bins.

```bash
python3 ./bin-data.py -b 1 input_dir output_dir
```

This process can be run sequentially on different input directories, since data is
appended to existing bin files. **Do not run this process in parallel for input
directories with overlapping data**, since writing the bin files is probably not thread
safe.

To push the bins to Kafka, you need to create a configuration file that contains at
least the topic name and the bootstrap server(s). In addition, you can specify some
topic configuration parameters, which will be used if the topic does not already exist.

```ini
[output]
kafka_topic = topic_name

[kafka]
# Used for consuming the input topic and producing the output topic.
bootstrap_servers = localhost:9092
# Topic configuration for the creation of the output topic.
# num_partitions = 10
# replication_factor = 2
# retention_ms = 2592000000
```

Finally, push the bins to kafka.

```bash
python3 ./push-bins-to-kafka.py bin_dir example_config.ini
```

This script reads the bin files in time-sorted order, sorts the traceroutes from each
file before pushing them to the topic specified in the config.

If you do not want to read all bin files, or only part of some files, you can use the
`--start` and `--stop` parameters to specify the time range that will actually be
pushed.
