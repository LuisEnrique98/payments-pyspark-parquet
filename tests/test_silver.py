import pytest

EXPECTED_COLUMNS = {
    "step", "tx_type", "amount", "name_orig", "old_balance_orig", "new_balance_orig",
    "name_dest", "old_balance_dest", "new_balance_dest", "is_fraud", "is_flagged_fraud",
    "tx_day", "tx_hour", "dest_is_merchant",
    "orig_delta_matches_amount", "dest_delta_matches_amount", "orig_mismatch_cause",
}


@pytest.fixture(scope="module")
def by_orig(silver_df):
    """Filas de plata indexadas por name_orig (unico en la muestra)."""
    return {row["name_orig"]: row for row in silver_df.collect()}


def test_keeps_all_rows(bronze_df, silver_df):
    assert silver_df.count() == bronze_df.count()


def test_columns(silver_df):
    assert set(silver_df.columns) == EXPECTED_COLUMNS


def test_flags_are_boolean(by_orig):
    assert by_orig["C3"]["is_fraud"] is True
    assert by_orig["C3"]["is_flagged_fraud"] is False
    assert by_orig["C13"]["is_flagged_fraud"] is True
    assert by_orig["C1"]["is_fraud"] is False


@pytest.mark.parametrize(
    "name_orig, day, hour",
    [
        ("C1", 1, 0),    # step 1: primera hora del dia 1
        ("C2", 1, 23),   # step 24: ultima hora del dia 1
        ("C3", 2, 0),    # step 25: primera hora del dia 2
        ("C13", 3, 0),   # step 49: primera hora del dia 3
    ],
)
def test_day_and_hour_from_step(by_orig, name_orig, day, hour):
    assert by_orig[name_orig]["tx_day"] == day
    assert by_orig[name_orig]["tx_hour"] == hour


def test_dest_is_merchant(by_orig):
    assert by_orig["C1"]["dest_is_merchant"] is True    # M1
    assert by_orig["C2"]["dest_is_merchant"] is False   # C9


def test_dest_delta_is_null_for_merchants(by_orig):
    assert by_orig["C1"]["dest_delta_matches_amount"] is None
    assert by_orig["C10"]["dest_delta_matches_amount"] is None


def test_dest_delta_for_customers(by_orig):
    assert by_orig["C2"]["dest_delta_matches_amount"] is True
    assert by_orig["C3"]["dest_delta_matches_amount"] is False  # el destino no recibe nada


def test_orig_delta_matches(by_orig):
    assert by_orig["C1"]["orig_delta_matches_amount"] is True
    assert by_orig["C2"]["orig_delta_matches_amount"] is True
    assert by_orig["C4"]["orig_delta_matches_amount"] is False


def test_float_noise_does_not_break_tolerance(by_orig):
    """Diferencia real de 0.01 (= TOLERANCE) debe cuadrar aunque el double diga 0.01000000000022."""
    row = by_orig["C11"]
    assert row["orig_delta_matches_amount"] is True
    assert row["orig_mismatch_cause"] is None


@pytest.mark.parametrize(
    "name_orig, cause",
    [
        ("C1", None),                    # cuadra
        ("C4", "zero_origin_balance"),   # saldo inicial 0
        ("C5", "account_emptied"),       # saldo final 0 y no cuadra
        ("C10", "other"),                # descuadre sin causa conocida
    ],
)
def test_orig_mismatch_cause(by_orig, name_orig, cause):
    assert by_orig[name_orig]["orig_mismatch_cause"] == cause


def test_quality_summary_counts(silver_df):
    rows = silver_df.collect()
    assert sum(r["is_fraud"] for r in rows) == 2
    assert sum(r["dest_is_merchant"] for r in rows) == 2
    assert sum(not r["orig_delta_matches_amount"] for r in rows) == 3
    assert sum(r["dest_delta_matches_amount"] is False for r in rows) == 1
