# -*- encoding: utf-8 -*-
import logging
from contextlib import contextmanager

import pandas as pd
from sqlalchemy import Engine, text, Delete, Select
from tqdm import tqdm

# local imports
from .utils import infer_sql_types


class DatabaseHandler:
    """

    - The `DatabaseHandler` class is responsible for handling database operations using the SQLAlchemy library.
    - The class requires a `db_engine` parameter, which should be a SQLAlchemy engine object, and a `schema` parameter,
     which should be the name of the database schema.
    - The `db_engine` and `schema` parameters are set as instance variables in the constructor.
    - The class provides a property `schema` to access the schema name, and a setter method to change the value of the
    `schema` instance variable.
    - The `_manage_session()` method is a context manager that handles the creation and closing of a database
    connection session.
    - The `_validate_table_name()` method validates if a table exists in the database schema.
    - The `_get_user_confirmation()` method asks the user for confirmation before modifying a table in the database.
    - The `execute_sql_query_to_dataframe()` method executes an SQL query and returns the results as a Pandas DataFrame.
    - The `empty_table()` method truncates a table in the database.
    - The `delete_records()` method deletes records from a table in the database using a delete statement.
    - The `write_dataframe_to_db()` method writes one or more Pandas DataFrames to a table in the database.

    """

    def __init__(self, db_engine: Engine, schema: str, logger: logging.Logger) -> None:
        """
        Constructor for DatabaseHandler class.
        :param db_engine: SQLAlchemy engine object.
        :param schema: Database schema name.
        """
        self.db_engine = db_engine
        self._schema = schema
        self.logger = logger

    @property
    def schema(self) -> str:
        return self._schema

    def __str__(self):
        return f"DatabaseHandler(" f"db_engine={self.db_engine}, schema={self.schema})"

    @contextmanager
    def _manage_session(self):
        """
        Context manager for handling session related tasks.
        """
        session = self.db_engine.connect()
        try:
            yield session
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def execute_query(self, statement: Select) -> pd.DataFrame:
        """
        Execute SQL query.
        :param statement: SQL query.
        :return: List of rows.
        """
        self.logger.info(f"Executing SQL query: \033[94m{statement}\033[0m")

        with self._manage_session() as session:
            result_proxy = session.execute(statement)
            column_names = result_proxy.keys()
            rows = result_proxy.fetchall()

        return pd.DataFrame(rows, columns=column_names)

    def empty_table(self, table_name: str) -> None:
        """
        Truncate a table in the database.
        :param table_name: Table name to operate on.
        """
        sql_command: str = "TRUNCATE TABLE {}.{}".format(self.schema, table_name)
        self.logger.info(f"\033[91mTruncating table {table_name}...\033[0m")

        with self._manage_session() as session:
            session.execute(text(sql_command))
        self.logger.info(f"Table {table_name} truncated.")

    def delete_records(self, delete_statement: Delete) -> None:
        """
        Delete records from a table in the database.
        :param delete_statement: Sqlalchemy delete statement.
        """
        self.logger.info(f"\033[93Deleting records from table...\033[0m")

        with self._manage_session() as session:
            count_deleted_records = session.execute(delete_statement)
            self.logger.info(
                f"\033[94mDeleted {count_deleted_records.rowcount:,.0f}"
                f" records from table.\033[0m"
            )
            session.commit()

    def write_dataframe_to_db(
        self, table_name: str, *dataframes: pd.DataFrame, **kwargs
    ) -> None:
        """
        Write dataframe to a database.
        :param table_name: Table name to operate on.
        :param dataframes: Dataframes to write to the database.
        :param kwargs: Additional arguments. Passed to pandas to_sql method.
        """
        table = table_name.lower()

        with self._manage_session() as session:
            for df in dataframes:
                rows: int = df.shape[0]
                column_sql_types: dict = infer_sql_types(df)
                # Load data in chunks
                with tqdm(
                    total=rows,
                    desc=f"Writing {rows:,.0f} rows to {table}",
                    unit="rows",
                    bar_format="{desc}: {percentage:3.0f}%|{bar}| "
                    "{n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
                    colour="blue",
                ) as progress_bar:
                    for chunk_start in range(0, rows, kwargs["chunksize"]):
                        chunk_end = min(chunk_start + kwargs["chunksize"], rows)
                        data_chunk = df.iloc[chunk_start:chunk_end]
                        data_chunk.to_sql(
                            name=table,
                            con=session,
                            schema=self.schema,
                            dtype=column_sql_types,
                            **kwargs,
                        )
                        progress_bar.update(data_chunk.shape[0])
                self.logger.info(
                    f"\033[94mSuccessfully wrote {rows:,.0f} "
                    f"rows to \033[92m{table}\033[0m."
                )
