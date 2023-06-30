import argparse
import logging
import sys

import pandas as pd
from bs4 import BeautifulSoup


def main() -> None:
    desc = """Since the monitor table is generated via JavaScript, we can not easily fetch it fully
           automatic. Go to https://www.caida.org/projects/ark/locations/ and make sure to tick all
           checkboxes listed under 'Locations Map' to get the full table. Then use the _inspector_
           tool of the browser (_not_ Show Source) and copy the HTML source into a file.
           This script automatically searches for the table inside the file so as long as you copy
           at least the table, the script will find it.
     """
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('html_file')
    parser.add_argument('output_file')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    with open(args.html_file, 'r') as f:
        soup = BeautifulSoup(f, 'html.parser')

    table = soup.find(id='list-monitors')
    if table is None:
        logging.error('Failed to find table (id="list-monitors") in HTML file.')
        sys.exit(1)

    monitor_list = {'name': list(),
                    'activation': list(),
                    'city': list(),
                    'cc': list(),
                    'asn': list(),
                    'org': list()}

    rows = table.find_all('tr')
    for row in rows:
        cells = row.find_all('td')
        if len(cells) != 11:
            logging.warning('Unexpected cell count in row. Might produce wrong output.')
            logging.warning(row)
        # Some monitors have a star in their label...
        monitor_list['name'].append(cells[0].text.strip('★'))
        monitor_list['activation'].append(cells[1].text)
        monitor_list['city'].append(cells[2].text)
        monitor_list['cc'].append(cells[3].text)
        monitor_list['asn'].append(cells[4].text)
        monitor_list['org'].append(cells[5].text)

    logging.info(f'Parsed {len(rows)} monitors.')

    df = pd.DataFrame(monitor_list)
    df.to_csv(args.output_file, index=False)


if __name__ == '__main__':
    main()
    sys.exit(0)
