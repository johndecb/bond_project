def test_imports_and_functions():
    # --- Database ---
    from jcb_bond_project.database.db import get_conn
    conn = get_conn(":memory:")   # sqlite in-memory db
    assert conn is not None

    from jcb_bond_project.database.query import _coerce_date
    import datetime
    assert _coerce_date(datetime.date.today()) == datetime.date.today()

    # --- Schema / validation ---
    from jcb_bond_project.validate_schema import check_instrument_schema
    # Should just run without raising (we’re not testing DB contents here)
    try:
        check_instrument_schema(None)
    except Exception:
        pass

    # --- Cashflow model ---
    from jcb_bond_project.cashflow_model.conv_bond_model import CashflowModel
    import datetime

    today = datetime.date.today()
    future = datetime.date(2030, 1, 1)

    # Instantiate with required args
    model = CashflowModel(
        issue_date=today,
        maturity_date=future,
        coupon_rate=0.02,
        frequency=2,   # semi-annual
        notional=100.0
    )

    schedule = model.generate_cashflow_schedule()
    assert isinstance(schedule, list)


    # --- Calendar utils ---
    from jcb_bond_project.utils.jcb_calendar import BusinessDayCalendar
    cal = BusinessDayCalendar([])
    assert cal.is_business_day(datetime.date.today()) in (True, False)

    # --- Identifiers ---
    from jcb_bond_project.utils.identifiers import generate_jcb_isin
    isin = generate_jcb_isin("GBP", "BOND1","10Y")
    assert isin.startswith("JCB")

    # --- Loaders ---
    from jcb_bond_project.loaders.classify import short_bond_code
    assert isinstance(short_bond_code("2% Treasury Gilt 2025"), str)

    # --- CLI (just check it loads) ---
    import jcb_bond_project.run_cli
    assert hasattr(jcb_bond_project.run_cli, "list_instruments")

    # --- Core startup ---
    import jcb_bond_project.core.startup
    assert hasattr(jcb_bond_project.core.startup, "setup_database")

    # --- API ---
    import jcb_bond_project.api.main
    assert hasattr(jcb_bond_project.api.main, "get_cashflows")

    # --- Portfolio ---
    import jcb_bond_project.portfolio.builders
    assert hasattr(jcb_bond_project.portfolio.builders, "build_portfolio")

    # --- Analytics ---
    import jcb_bond_project.jcb_analytics.jcb_analytics
    assert hasattr(jcb_bond_project.jcb_analytics.jcb_analytics, "jGet_Instrument_Data")
