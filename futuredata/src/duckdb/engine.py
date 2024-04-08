from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Union

import duckdb
from duckdb import DuckDBPyConnection
from loguru import logger


class DuckEngine:
    """DuckDB engine creation and definition class."""

    def __init__(self, database: Optional[Union[Path, str]], read_only: bool = True):
        """Instantiate the class for in-memory usage.

        For persistence usage, usage the persist context manager.
        It will prevent keeping the database file open, as non read-only connections blocks all other connections.

        Args:
            database (Optional[str]): Database path. This database won't be created unless in a persisting context.
            read_only (bool, optional): Whether to enable read_only. Defaults to True.
        """
        self._database = str(database)
        self._read_only = read_only
        self._connection = duckdb.connect()
        self._in_memory = database is None

    def get_connection(self) -> DuckDBPyConnection:
        """Returns the connection object.

        Returns:
            DuckDBPyConnection: DuckDB connection object.
        """
        return self._connection

    @contextmanager
    def persist(self):
        """Context manager to persist the DuckDB database commands on the database file.

        DuckDB persistent connections should not be kept open, especially when not read-only.

        Raises:
            Exception: If the database path was not provided.
            e: Error during the query.

        Yields:
            DuckEngine: DuckDB engine with persistent connection.
        """
        if self._in_memory:
            raise Exception("Cannot persist without database file.")

        self._connection = duckdb.connect(database=self._database, read_only=self._read_only)

        try:
            yield self

        except Exception as e:
            logger.exception(e)
            raise e

        finally:
            self._connection.close()
            self._connection = duckdb.connect()

    @contextmanager
    def begin_transaction(self) -> DuckDBPyConnection:
        """Context manager to start a new transaction.

        This is useful if you want to chain DML and make sure everything is set up before going to prod.

        Raises:
            e: Error during the transaction.

        Yields:
            DuckDBPyConnection: DuckDB database connection.
        """
        self._connection.begin()

        try:
            yield self._connection

        except Exception as e:
            logger.error("Error during the transaction")
            logger.exception(e)
            self._connection.rollback()
            raise e

        finally:
            self._connection.commit()
