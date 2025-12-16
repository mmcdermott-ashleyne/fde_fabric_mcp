from __future__ import annotations

from contextlib import contextmanager
from typing import Iterable, Optional, Sequence, Union

import struct
import pandas as pd
import pyodbc
from azure.core.credentials import TokenCredential

from ..auth import get_credential
from ..config import settings

Param = Union[str, int, float, None]
ParamsList = Iterable[Sequence[Param]]


def _best_sql_driver() -> str:
    preferred = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server",
    ]
    installed = {d.lower(): d for d in pyodbc.drivers()}
    for name in preferred:
        if name.lower() in installed:
            return "{" + installed[name.lower()] + "}"
    raise RuntimeError("No SQL Server ODBC driver found.")


class SQLServerConnection:
    """
    Minimal Fabric Warehouse connector using Azure AD Access Tokens.

    Auth comes from DefaultAzureCredential / ClientSecretCredential, via get_credential().
    """

    def __init__(
        self,
        server: str,
        database: str,
        credential: Optional[TokenCredential] = None,
        driver: Optional[str] = None,
    ):
        self.server = server
        self.database = database
        self.credential = credential or get_credential()
        self.driver = driver or settings.sql_default_driver or _best_sql_driver()

    def _conn_open(self):
        """
        Open a connection to Fabric SQL using an Azure AD access token.

        Uses DefaultAzureCredential/ClientSecretCredential via get_credential(),
        and passes the token to the ODBC driver via SQL_COPT_SS_ACCESS_TOKEN (1256).
        """
        # Acquire access token for SQL/Fabric
        token = self.credential.get_token("https://database.windows.net/.default")

        # Connection string: NO Authentication=... here, just standard settings.
        conn_str = (
            f"Driver={self.driver};"
            f"Server=tcp:{self.server};"
            f"Database={self.database};"
            f"Encrypt=Yes;"
            f"TrustServerCertificate=No;"
        )

        # ODBC expects the token as UTF-16LE bytes prefixed with 4-byte length
        token_bytes = token.token.encode("utf-16-le")
        packed_token = struct.pack("<I", len(token_bytes)) + token_bytes

        # 1256 = SQL_COPT_SS_ACCESS_TOKEN
        conn = pyodbc.connect(conn_str, attrs_before={1256: packed_token})
        return conn

    @contextmanager
    def get_connection(self):
        conn = self._conn_open()
        try:
            yield conn
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def run_query(self, sql: str, params=None) -> pd.DataFrame:
        with self.get_connection() as conn:
            return pd.read_sql(sql, conn, params=params)

    def execute(self, sql: str, params=None) -> int:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params or [])
            rc = cursor.rowcount
            conn.commit()
            return rc

    def execute_many(self, sql: str, params_list: ParamsList) -> int:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(sql, params_list)
            total = cursor.rowcount
            conn.commit()
            return total


def get_sql_connection(*, server: str, database: str) -> SQLServerConnection:
    """
    Factory used by MCP tools.

    Only requires the Fabric SQL endpoint server + database name.
    Auth automatically comes from Azure credentials.
    """
    return SQLServerConnection(
        server=server,
        database=database,
        credential=get_credential(),
    )
