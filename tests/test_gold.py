from decimal import Decimal

import pytest
from pyspark.sql import DataFrame

import gold


@pytest.mark.parametrize("name", list(gold.TABLES))
def test_every_table_builds(silver_df, name):
    table = gold.TABLES[name](silver_df)
    assert isinstance(table, DataFrame)
    assert table.count() > 0


# ---------- daily_volume_by_type ----------

@pytest.fixture(scope="module")
def daily(silver_df):
    rows = gold.daily_volume_by_type(silver_df).collect()
    return {(r["tx_day"], r["tx_type"]): r for r in rows}


def test_daily_counts_add_up(silver_df):
    total = gold.daily_volume_by_type(silver_df).agg({"tx_count": "sum"}).first()[0]
    assert total == silver_df.count()


def test_daily_values(daily):
    assert len(daily) == 6  # combinaciones (dia, tipo) presentes en la muestra
    assert daily[(1, "PAYMENT")]["tx_count"] == 1
    assert daily[(1, "PAYMENT")]["total_amount"] == Decimal("100.00")
    cash_out_d2 = daily[(2, "CASH_OUT")]
    assert cash_out_d2["tx_count"] == 3
    assert cash_out_d2["total_amount"] == Decimal("4937.32")
    assert cash_out_d2["avg_amount"] == Decimal("1645.77")


def test_only_last_day_is_partial(daily):
    # la muestra termina en step 49: el dia 3 solo tiene 1 hora
    partial_days = {day for (day, _), r in daily.items() if r["is_partial_day"]}
    assert partial_days == {3}


# ---------- fraud_by_type ----------

@pytest.fixture(scope="module")
def fraud(silver_df):
    return {r["tx_type"]: r for r in gold.fraud_by_type(silver_df).collect()}


def test_fraud_total(fraud):
    assert sum(r["fraud_count"] for r in fraud.values()) == 2


def test_fraud_rate_and_amounts(fraud):
    transfer = fraud["TRANSFER"]
    assert transfer["tx_count"] == 2
    assert transfer["fraud_count"] == 2
    assert transfer["fraud_flagged_count"] == 1
    assert transfer["flagged_count"] == 1
    assert transfer["fraud_amount"] == Decimal("3000.00")
    assert transfer["fraud_rate_pct"] == 100.0
    assert fraud["CASH_OUT"]["fraud_rate_pct"] == 0.0
    assert fraud["PAYMENT"]["fraud_count"] == 0


# ---------- balance_mismatch_summary ----------

@pytest.fixture(scope="module")
def mismatch(silver_df):
    rows = gold.balance_mismatch_summary(silver_df).collect()
    return {(r["tx_type"], r["cause"]): r for r in rows}


def test_mismatch_causes(mismatch):
    assert mismatch[("CASH_OUT", "matches")]["tx_count"] == 2
    assert mismatch[("CASH_OUT", "zero_origin_balance")]["tx_count"] == 1
    assert mismatch[("CASH_OUT", "account_emptied")]["tx_count"] == 1
    assert mismatch[("PAYMENT", "other")]["tx_count"] == 1
    assert mismatch[("TRANSFER", "matches")]["pct_of_type"] == 100.0


def test_mismatch_total_excludes_matches(mismatch):
    total = sum(r["tx_count"] for (_, cause), r in mismatch.items() if cause != "matches")
    assert total == 3


def test_mismatch_pct_sums_to_100_per_type(mismatch):
    sums = {}
    for (tx_type, _), r in mismatch.items():
        sums[tx_type] = sums.get(tx_type, 0) + r["pct_of_type"]
    assert all(abs(v - 100) < 0.05 for v in sums.values())
