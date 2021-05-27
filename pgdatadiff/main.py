"""
Usage:
  pgdatadiff --firstdb=<firstconnectionstring> --seconddb=<secondconnectionstring> [--schema=<schema>] [--only-data|--only-sequences] [--count-only] [--count-with-max] [--chunk-size=<size>] [--exclude-tables=<table1,table2>]
  pgdatadiff --version

Options:
  -h --help          Show this screen.
  --version          Show version.
  --firstdb=postgres://postgres:password@localhost/firstdb        The connection string of the first DB
  --seconddb=postgres://postgres:password@localhost/seconddb         The connection string of the second DB
  --schema="public"         The schema of tables in comparison
  --only-data        Only compare data, exclude sequences
  --only-sequences   Only compare seqences, exclude data
  --exclude-tables=""   Exclude tables from data comparison         Must be a comma separated string [default: empty string]
  --count-only       Do a quick test based on counts alone
  --chunk-size=10000       The chunk size when comparing data [default: 10000]
  --count-with-max    Use MAX(id) when a table uses a sequence, otherwise use COUNT.
"""

import pkg_resources
from fabulous.color import red

from pgdatadiff.pgdatadiff import DBDiff
from docopt import docopt


def main():
    arguments = docopt(
        __doc__, version=pkg_resources.require("pgdatadiff")[0].version)
    first_db_connection_string=arguments['--firstdb']
    second_db_connection_string=arguments['--seconddb']
    if not first_db_connection_string.startswith("postgres://") or \
            not second_db_connection_string.startswith("postgres://"):
        print(red("Only Postgres DBs are supported"))
        return 1

    differ = DBDiff(first_db_connection_string, second_db_connection_string,
                    chunk_size=arguments['--chunk-size'],
                    count_only=arguments['--count-only'],
                    count_with_max=arguments['--count-with-max'],
                    exclude_tables=arguments['--exclude-tables'],
                    schema=arguments['--schema'])

    if not arguments['--only-sequences']:
        if differ.diff_all_table_data():
            return 1
    if not arguments['--only-data']:
        if differ.diff_all_sequences():
            return 1
    return 0
