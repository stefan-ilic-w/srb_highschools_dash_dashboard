from sqlalchemy.engine import URL, create_engine
from sqlalchemy.pool import NullPool
import os


connection_string = os.environ["AZURE_SQL_SREDNJE_EDU"]
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string
                                                  , "autocommit": "True"})

engine = create_engine(connection_url, echo=True, poolclass=NullPool).execution_options(
    isolation_level="AUTOCOMMIT")
