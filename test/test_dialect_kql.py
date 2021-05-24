from test.conftest import (
    KUSTO_KQL_ALCHEMY_URL,
    KUSTO_SQL_ALCHEMY_URL,
    DATABASE,
    AZURE_AD_CLIENT_ID,
    AZURE_AD_CLIENT_SECRET,
    AZURE_AD_TENANT_ID,
)
from sqlalchemy.sql.selectable import TextAsFrom
from sqlalchemy import Table, Column, String, MetaData, create_engine, column
import sqlalchemy as sa


def test_limit():
    metadata = MetaData()
    stream = Table(
        "MaterialTransferStream",
        metadata,
        Column("MaterialTypeId", String),
        Column("UnitId", String),
    )

    query = stream.select().limit(5)
    engine = create_engine(
        f"{KUSTO_SQL_ALCHEMY_URL}/{DATABASE}?"
        f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
        f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
        f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}"
    )
    engine.connect()
    result = engine.execute(query)
    result_length = len(result.fetchall())
    assert result_length == 5


def test_compilator():
    statement_str = "MaterialTransferStream | take 10"
    # statement_str = "Select top 100 * from MaterialTransferStream"

    engine = create_engine(
        f"{KUSTO_KQL_ALCHEMY_URL}/{DATABASE}?"
        f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
        f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
        f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}"
    )
    engine.connect()

    stmt = TextAsFrom(sa.text(statement_str), []).alias("virtual_table")
    query = sa.select(from_obj=stmt, columns=[column("UnitId").label("uId")])
    query = query.select_from(stmt)
    query = query.limit(10)
    print("========")
    print(str(query))
    print("========")
    engine.execute(query)