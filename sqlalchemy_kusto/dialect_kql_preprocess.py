import os
import logging
from sqlalchemy.sql.elements import ColumnClause, Label

logger = logging.getLogger(__name__)


def handle_sensitive_columns(expr, pii_col_list):
    is_column = isinstance(expr, ColumnClause) or isinstance(expr, Label)
    logger.debug(
        "Is instance of column %s and name is %s. TypeOf %s",
        is_column,
        expr.name,
        type(expr),
    )
    # Check if the expression is a column or label
    if is_column:
        # Handle aliasing
        if hasattr(expr, "name"):
            if expr.name in pii_col_list:
                logger.warning(
                    "Access to sensitive column '%s' is not allowed.", expr.name
                )
                raise SensitiveColumnError(
                    f"Access to sensitive column '{expr.name}' is prohibited."
                )
        if str(expr) in pii_col_list:
            logger.warning("Access to sensitive column '%s' is not allowed.", str(expr))
            raise SensitiveColumnError(
                f"Access to sensitive column '{str(expr)}' is prohibited."
            )
    return expr


class KqlPreprocessor:
    """
    This class is used to handle PII (Personally Identifiable Information) columns
    in Kusto queries. It masks sensitive information in the query results.
    """

    pii_cols_set = None

    def __init__(self):
        """Initialize the KqlPreprocessor class."""
        # Get the PII columns from the environment variable

    @property
    def pii_cols(self):
        # Only reads env var when accessed
        if self.pii_cols_set is None:
            pii_cols_env = os.getenv("PII_COLS", "")
            self.pii_cols_set = set(pii_cols_env.split(",")) if pii_cols_env else {}
        return self.pii_cols_set

    def process(self, expr) -> ColumnClause | Label:
        """
        Process the expression to handle sensitive columns.
        :param expr: The expression to process.
        :return: The processed expression.
        """
        if (len(self.pii_cols)) == 0:
            logger.info("No sensitive columns to handle.")
            return expr
        # handle the sensitive columns
        return handle_sensitive_columns(expr, self.pii_cols)


class SensitiveColumnError(Exception):
    """Custom exception for sensitive column access."""

    pass
