import os
import pytest
from sqlalchemy.sql.elements import ColumnClause, Label
from sqlalchemy_kusto.dialect_kql_preprocess import (
    KqlPreprocessor,
    SensitiveColumnError,
)


def test_handles_sensitive_column_when_no_pii_cols_env_var():
    kql_processor = KqlPreprocessor()
    expr = ColumnClause("NonSensitiveColumn")
    assert kql_processor.process(expr) == expr


def test_raises_error_when_accessing_sensitive_column():
    os.environ["PII_COLS"] = "SensitiveColumn"
    kql_processor = KqlPreprocessor()
    expr = ColumnClause("SensitiveColumn")
    with pytest.raises(
        SensitiveColumnError,
        match="Access to sensitive column 'SensitiveColumn' is prohibited.",
    ):
        kql_processor.process(expr)


def test_logs_warning_when_accessing_sensitive_column(caplog):
    os.environ["PII_COLS"] = "SensitiveColumn"
    kql_processor = KqlPreprocessor()
    expr = ColumnClause("SensitiveColumn")
    with pytest.raises(SensitiveColumnError):
        kql_processor.process(expr)
    assert "Access to sensitive column 'SensitiveColumn' is not allowed." in caplog.text


def test_processes_expression_with_non_sensitive_column():
    os.environ["PII_COLS"] = "SensitiveColumn"
    kql_processor = KqlPreprocessor()
    expr = ColumnClause("NonSensitiveColumn")
    assert kql_processor.process(expr) == expr


def test_handles_empty_pii_cols_env_var():
    os.environ["PII_COLS"] = ""
    kql_processor = KqlPreprocessor()
    expr = ColumnClause("AnyColumn")
    assert kql_processor.process(expr) == expr


def test_handles_label_expression_with_sensitive_column():

    os.environ["PII_COLS"] = "SensitiveColumn"
    kql_processor = KqlPreprocessor()
    expr = Label("SensitiveColumn", ColumnClause("SensitiveColumn"))
    with pytest.raises(SensitiveColumnError):
        kql_processor.process(expr)


def test_handles_label_expression_with_non_sensitive_column():

    os.environ["PII_COLS"] = "SensitiveColumn"
    kql_processor = KqlPreprocessor()
    expr = Label("NonSensitiveColumn", ColumnClause("NonSensitiveColumn"))
    assert kql_processor.process(expr) == expr
