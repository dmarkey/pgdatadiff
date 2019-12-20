import warnings

from fabulous.color import bold, green, red
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

    def __init__(self, firstdb, seconddb, chunk_size=10000, count_only=False):
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

    def diff_table_data(self, tablename):
        try:
            firsttable = Table(tablename, self.firstmeta, autoload=True)
            firstquery = self.firstsession.query(
                firsttable)
            secondtable = Table(tablename, self.secondmeta, autoload=True)
            secondquery = self.secondsession.query(
                secondtable)
            if firstquery.count() != secondquery.count():
                return False, f"counts are different" \
                              f" {firstquery.count()} != {secondquery.count()}"
            if firstquery.count() == 0:
                return None, "tables are empty"
            if self.count_only is True:
                return True, "Counts are the same"
            pk = ",".join(self.firstinspector.get_pk_constraint(tablename)[
                              'constrained_columns'])
            if not pk:
                return None, "no primary key(s) on this table." \
                             " Comparision is not possible."

        except NoSuchTableError:
            return False, "table is missing"

        SQL_TEMPLATE_HASH = f"""
        SELECT md5(array_agg(md5((t.*)::varchar))::varchar)
        FROM (
                SELECT *
                FROM {tablename}
                ORDER BY {pk} limit :row_limit offset :row_offset
            ) AS t;
                        """

        position = 0

        while position <= firstquery.count():
            firstresult = self.firstsession.execute(
                SQL_TEMPLATE_HASH,
                {"row_limit": self.chunk_size,
                 "row_offset": position}).fetchone()
            secondresult = self.secondsession.execute(
                SQL_TEMPLATE_HASH,
                {"row_limit": self.chunk_size,
                 "row_offset": position}).fetchone()
            if firstresult != secondresult:
                return False, f"data is different - position {position} -" \
                              f" {position + self.chunk_size}"
            position += self.chunk_size
        return True, "data is identical."

    def get_all_sequences(self):
        GET_SEQUENCES_SQL = """SELECT c.relname FROM 
        pg_class c WHERE c.relkind = 'S';"""
        return [x[0] for x in
                self.firstsession.execute(GET_SEQUENCES_SQL).fetchall()]

    def diff_sequence(self, seq_name):
        GET_SEQUENCES_VALUE_SQL = f"SELECT last_value FROM {seq_name};"

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
        print(bold(red('Starting sequence analysis.')))
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
            tables = sorted(
                self.firstinspector.get_table_names(schema="public"))
            for table in tables:
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

