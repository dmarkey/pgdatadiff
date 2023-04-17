import warnings

from fabulous.color import bold, green, red, yellow
from halo import Halo
from sqlalchemy import exc as sa_exc
from sqlalchemy.engine import create_engine
from sqlalchemy.exc import NoSuchTableError, ProgrammingError
from sqlalchemy.inspection import inspect
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.sql.schema import MetaData, Table


def make_session(connection_string):
    engine = create_engine(connection_string, echo=False,
                           convert_unicode=True)
    Session = sessionmaker(bind=engine)
    return Session(), engine


class DBDiff(object):

    def __init__(self, firstdb, seconddb, schema, chunk_size=10000, count_only=False, count_with_max=False, progress=True, exclude_tables="", include_tables=""):
        firstsession, firstengine = make_session(firstdb)
        secondsession, secondengine = make_session(seconddb)
        self.firstsession = firstsession
        self.firstengine = firstengine
        self.secondsession = secondsession
        self.secondengine = secondengine
        self.firstmeta = MetaData(bind=firstengine)
        self.secondmeta = MetaData(bind=secondengine)
        self.firstinspector = inspect(firstengine)
        self.secondinspector = inspect(secondengine)
        self.chunk_size = int(chunk_size)
        self.count_only = count_only
        self.count_with_max = count_with_max
        self.progress = progress
        if exclude_tables is None:
            self.exclude_tables = []
        else:
            self.exclude_tables = exclude_tables.split(',')
        if include_tables is None:
            self.include_tables = []
        else:
            self.include_tables = (include_tables or "").split(',')
        self.schema_names = self.firstinspector.get_schema_names()
        self.schema = schema or 'public'
        if self.schema not in self.schema_names:
            raise ValueError("Schema not found, check if argument has valid schema name")
        print(f"Comparing for schema {self.schema}")

    def diff_table_data(self, tablename):
        try:
            firsttable = Table(tablename, self.firstmeta, autoload=True)
            firstquery = self.firstsession.query(firsttable)
            secondtable = Table(tablename, self.secondmeta, autoload=True)
            secondquery = self.secondsession.query(secondtable)
            if self.count_with_max is True:
                column = self.column_using_sequence(tablename)
                pk_columns = self.firstinspector.get_pk_constraint(tablename)['constrained_columns']
                if column is not None and column in pk_columns:
                    GET_MAX_SQL = f"SELECT MAX({column}) FROM {tablename}"
                    first_max_count = self.firstsession.execute(GET_MAX_SQL).fetchone()[0]
                    second_max_count = self.secondsession.execute(GET_MAX_SQL).fetchone()[0]
                    if first_max_count != second_max_count:
                        return False, f"MAX value are different" \
                                      f" {first_max_count} != {second_max_count}"
                    if first_max_count == 0:
                        return None, "using MAX value, tables are empty because MAX on first db is zero"
                    return True, "MAX Value is same for both tables"
            first_count = firstquery.count()
            second_count = secondquery.count()
            if first_count != second_count:
                return False, f"counts are different" \
                              f" {first_count} != {second_count}"
            if first_count == 0:
                return None, "tables are empty"
            if self.count_only is True or self.count_with_max is True:
                return True, "Counts are the same"
            pk = ",".join(self.firstinspector.get_pk_constraint(tablename)[
                              'constrained_columns'])
            if not pk:
                return None, "no primary key(s) on this table." \
                             " Comparison is not possible."

        except NoSuchTableError:
            return False, "table is missing"

        SQL_TEMPLATE_HASH = f"""
        SELECT md5(array_agg(md5((t.*)::text))::text)
        FROM (
                SELECT *
                FROM {self.schema}.{tablename}
                ORDER BY {pk} limit :row_limit offset :row_offset
            ) AS t;
                        """

        position = 0

        while position <= first_count:
            firstresult = self.firstsession.execute(
                SQL_TEMPLATE_HASH,
                {"row_limit": self.chunk_size,
                 "row_offset": position}).fetchone()
            secondresult = self.secondsession.execute(
                SQL_TEMPLATE_HASH,
                {"row_limit": self.chunk_size,
                 "row_offset": position}).fetchone()
            if firstresult != secondresult:
                return False, f"data is different - for rows from {position} - to" \
                              f" {position + self.chunk_size}"
            position += self.chunk_size
            self.display_progress(position, first_count)
        return True, "data is identical."

    def display_progress(self, position, first_count):
        if position > first_count:
            position = first_count
        if first_count > self.chunk_size and self.progress is True:
            print(f' Progress: {"{:2.1f}".format(position/first_count*100)}%')

    def column_using_sequence(self, tablename):
        GET_COLUMN_OF_TABLES_WITH_SEQUENCES =  f"""SELECT
               attrib.attname AS column_name
        FROM   pg_class AS seqclass
               JOIN pg_depend AS dep
                 ON ( seqclass.relfilenode = dep.objid )
               JOIN pg_class AS depclass
                 ON ( dep.refobjid = depclass.relfilenode )
               JOIN pg_attribute AS attrib
                 ON ( attrib.attnum = dep.refobjsubid
                      AND attrib.attrelid = dep.refobjid )
        WHERE  seqclass.relkind = 'S' AND depclass.relname = '{tablename}';"""
        response = self.firstsession.execute(GET_COLUMN_OF_TABLES_WITH_SEQUENCES).fetchone()
        if response is None:
            return None
        return response[0]

    def get_all_sequences(self):
        GET_SEQUENCES_SQL = f"""SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = '{self.schema}';"""
        return [x[0] for x in
                self.firstsession.execute(GET_SEQUENCES_SQL).fetchall()]

    def diff_sequence(self, seq_name):
        GET_SEQUENCES_VALUE_SQL = f"SELECT last_value FROM {self.schema}.{seq_name};"

        try:
            firstvalue = \
                self.firstsession.execute(GET_SEQUENCES_VALUE_SQL).fetchone()[
                    0]
            secondvalue = \
                self.secondsession.execute(GET_SEQUENCES_VALUE_SQL).fetchone()[
                    0]
        except ProgrammingError:
            self.firstsession.rollback()
            self.secondsession.rollback()

            return False, "sequence doesnt exist in second database."
        if firstvalue < secondvalue:
            return None, f"first sequence is less than" \
                         f" the second({firstvalue} vs {secondvalue})."
        if firstvalue > secondvalue:
            return False, f"first sequence is greater than" \
                          f" the second({firstvalue} vs {secondvalue})."
        return True, f"sequences are identical- ({firstvalue})."

    def diff_all_sequences(self):
        print(bold(red(f'Starting sequence analysis for schema -> {self.schema}')))
        sequences = sorted(self.get_all_sequences())
        failures = 0
        for sequence in sequences:
            with Halo(
                    text=f"Analysing sequence {sequence}. "
                         f"[{sequences.index(sequence) + 1}/{len(sequences)}]",
                    spinner='dots') as spinner:
                result, message = self.diff_sequence(sequence)
                if result is True:
                    spinner.succeed(f"{sequence} - {message}")
                elif result is None:
                    spinner.warn(f"{sequence} - {message}")
                else:
                    failures += 1
                    spinner.fail(f"{sequence} - {message}")
        print(bold(green('Sequence analysis complete.')))
        if failures > 0:
            return 1
        return 0

    def diff_all_table_data(self):
        failures = 0
        print(bold(red('Starting table analysis.')))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=sa_exc.SAWarning)
            tables = sorted(self.firstinspector.get_table_names(schema=self.schema))
            if len(self.include_tables) > 0:
                # Intersection of 2 array
                tables = [value for value in tables if value in self.include_tables]
            if len(tables) == 0:
                print(bold(red(f'No tables found in schema: {self.schema}')))
                return 0
            for table in tables:
                if table in self.exclude_tables:
                    print(bold(yellow(f"Ignoring table {table} (excluded)")))
                    continue
                with Halo(
                        text=f"Analysing table {table}. "
                             f"[{tables.index(table) + 1}/{len(tables)}]",
                        spinner='dots') as spinner:
                    result, message = self.diff_table_data(table)
                    if result is True:
                        spinner.succeed(f"{table} - {message}")
                    elif result is None:
                        spinner.warn(f"{table} - {message}")
                    else:
                        failures += 1
                        spinner.fail(f"{table} - {message}")
        print(bold(green('Table analysis complete.')))
        if failures > 0:
            return 1
        return 0

