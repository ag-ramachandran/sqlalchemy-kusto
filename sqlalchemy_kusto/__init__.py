from sqlalchemy_kusto.dbapi import connect
from sqlalchemy_kusto.errors import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)

__all__ = [
    "connect",
    "apilevel",
    "threadsafety",
    "paramstyle",
    "DataError",
    "DatabaseError",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
]


# Rename custom Warning to avoid conflict with built-in
class KustoWarning(Exception):
    pass


APILEVEL = "2.0"
THREADSAFETY = 1
PARAMSTYLE = "pyformat"

# Add paramstyle attribute for SQLAlchemy DBAPI compatibility

paramstyle = "pyformat"
