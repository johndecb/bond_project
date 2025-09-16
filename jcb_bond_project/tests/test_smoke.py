# tests/test_smoke.py
from datetime import date, timedelta

import pandas as pd

from jcb_bond_project.jcb_bond_project.database.db import connect
from jcb_bond_project.jcb_bond_project.database.schema import (
    create_instruments_table,
    create_instrument_data_table,
    create_instrument_identifier_table,
    create_calendar_holidays_table,
)
from jcb_bond_project.jcb_bond_project.database.insert import (
    save_instrument,
    insert_instrument_identifier,
    insert_calendar_holidays,
    insert_instrument_data,
    update_instrument_field_by_id,
)
from jcb_bond_project.jcb_bond_project.database.query import (
    get_instrument,
    list_instruments,
    load_instrument_data,
    resolve_isin_from_alt_id,
    get_holidays_for_calendar,
)
from jcb_bond_project.jcb_bond_project.models.instrument import Instrument
from jcb_bond_project.jcb_bond_project.models.instrument_data import InstrumentData


def _bootstrap_schema(conn):
    """Create all required tables for a clean test DB."""
    create_instruments_table(conn)
    create_instrument_data_table(conn)
    create_instrument_identifier_table(conn)
    create_calendar_holidays_table(conn)


def test_instrument_roundtrip(tmp_path):
    db = tmp_path / "test.db"
    conn = connect(str(db))
    try:
        _bootstrap_schema(conn)

        isin = "JCB_TEST_GBP_BOND_1"
        inst = Instrument(
            isin=isin,
            short_code="TESTBOND",
            name="Test GBP Bond",
            instrument_type="bond",
            issuer="Test Issuer",
            country="UK",
            currency="GBP",
            first_issue_date=date(2024, 1, 1),
            maturity_date=date(2029, 1, 1),
            coupon_rate=3.0,
            is_green=False,
            is_linker=False,
        )

        res = save_instrument(conn, inst)
        assert res in {"inserted", "updated"}

        got = get_instrument(conn, isin)
        assert got is not None and got.isin == isin

        # update one field (whitelisted)
        res2 = update_instrument_field_by_id(conn, isin, "short_code", "TESTBOND2")
        assert res2 in {"updated", "not_found"}
        got2 = get_instrument(conn, isin)
        assert got2.short_code == "TESTBOND2"

        # list filter
        df = list_instruments(
            conn,
            instrument_types=["bond"],
            country="UK",
            is_green=False,
            is_linker=False,
        )
        assert isinstance(df, pd.DataFrame)
        assert (df["isin"] == isin).any()
    finally:
        conn.close()


def test_identifiers_and_holidays(tmp_path):
    db = tmp_path / "test.db"
    conn = connect(str(db))
    try:
        _bootstrap_schema(conn)

        isin = "JCB_TEST_GBP_BOND_2"
        save_instrument(
            conn,
            Instrument(
                isin=isin,
                short_code="T2",
                name="Test 2",
                instrument_type="bond",
                issuer="Issuer",
                country="UK",
                currency="GBP",
                first_issue_date=date(2024, 1, 1),
                maturity_date=date(2028, 1, 1),
            ),
        )

        # identifiers
        status = insert_instrument_identifier(conn, isin, "BBG000TEST2", "Bloomberg")
        assert status in {"inserted", "skipped"}
        resolved = resolve_isin_from_alt_id(conn, "BBG000TEST2")
        assert resolved == isin

        # holidays
        n_ins = insert_calendar_holidays(
            conn,
            "UK",
            [
                (date(2025, 12, 25), "Christmas Day"),
                (date(2025, 12, 26), "Boxing Day"),
            ],
        )
        assert n_ins >= 0
        hols = get_holidays_for_calendar(conn, "UK")
        assert hols is not None
    finally:
        conn.close()


def test_timeseries_roundtrip(tmp_path):
    db = tmp_path / "test.db"
    conn = connect(str(db))
    try:
        _bootstrap_schema(conn)

        isin = "JCB_TEST_GBP_BOND_3"
        save_instrument(
            conn,
            Instrument(
                isin=isin,
                short_code="T3",
                name="Test 3",
                instrument_type="bond",
                issuer="Issuer",
                country="UK",
                currency="GBP",
                first_issue_date=date(2024, 1, 1),
                maturity_date=date(2027, 1, 1),
            ),
        )

        start = date.today() - timedelta(days=5)
        rows = []
        for i in range(5):
            d = start + timedelta(days=i)
            rows.append(
                InstrumentData(
                    instrument_id=isin,
                    data_date=d,
                    data_type="yield",
                    value=3.00 + 0.01 * i,
                    source="Test",
                    resolution="daily",
                    unit="percent",
                    attrs={"session": "close", "quote_side": "mid"},
                )
            )

        statuses = [insert_instrument_data(conn, r) for r in rows]
        assert all(s in {"inserted", "skipped"} for s in statuses)
        # duplicate should skip
        assert insert_instrument_data(conn, rows[0]) == "skipped"

        # basic load
        df = load_instrument_data(
            conn,
            instrument_id=isin,
            source="Test",
            data_type="yield",
            start_date=start,
            session="close",       # JSON attr filter (SQL if JSON1; else Python filter)
            quote_side="mid",
            long_format=True,
            parse_dates=True,
        )
        assert df is not None and len(df) >= 1
        assert {"data_date", "data_type", "value"}.issubset(df.columns)

        # wide pivot
        dfw = load_instrument_data(
            conn,
            instrument_id=isin,
            source="Test",
            data_type="yield",
            start_date=start,
            long_format=False,
            parse_dates=True,
        )
        # pivoted columns exist (likely just "yield")
        assert hasattr(dfw, "columns")
        assert "yield" in dfw.columns
    finally:
        conn.close()
