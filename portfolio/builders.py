import numpy as np
import pandas as pd
from datetime import date, datetime
from database.db import get_conn
from database.query import list_instruments, get_instrument, get_holidays_for_calendar
from utils.jcb_calendar import BusinessDayCalendar
from cashflow_model.builders import (
    cashflows_df, 
    cashflow_matrix,
    filter_bonds_by_maturity,
    generate_target_cashflows,
    calculate_running_totals,
    solve_portfolio_weights,
    create_unified_timeline,
)

DB_PATH = "jcb_db.db"

def build_portfolio(
    select_start_date: date | datetime,
    select_end_date: date | datetime,
    settlement_date: date | datetime,
    target_amount: float = 100,
    frequency: str = "monthly",
    country: str = "UK",
    is_green: bool = False,
    is_linker: bool = False,
) -> dict:
    """Construct portfolio weights for target cashflows."""
    # 1. Load and filter bonds
    with get_conn(DB_PATH) as conn:
        raw = list_instruments(
            conn,
            instrument_types=["bond"],
            country=country,
            is_green=is_green,
            is_linker=is_linker,
        )
        bonds = [r if hasattr(r, "instrument_type") else get_instrument(conn, r) for r in raw]
        uk = BusinessDayCalendar(set(get_holidays_for_calendar(conn, country)))

    # 2. Filter bonds by maturity
    filtered_bonds = filter_bonds_by_maturity(bonds, settlement_date, select_end_date)
    if not filtered_bonds:
        raise ValueError("No bonds found matching maturity criteria")

    # 3. Generate bond cashflows
    cf_long = cashflows_df(filtered_bonds, calendar=uk)
    cf_mat = cashflow_matrix(cf_long)

    # 4. Generate target cashflows
    cf_target = generate_target_cashflows(select_start_date, select_end_date, frequency, target_amount)

    # 5. Create unified timeline
    unified_cf = create_unified_timeline(cf_target, cf_mat, settlement_date)

    # 6. Running totals
    unified_running = calculate_running_totals(unified_cf)

    # 7. Split into target and bonds
    Y_running = unified_running["target"].values
    C_matrix = unified_running.drop("target", axis=1).values

    # 8. Solve weights
    weights = solve_portfolio_weights(C_matrix, Y_running)

    # 9. Diagnostics
    predicted_running = C_matrix @ weights
    residuals = Y_running - predicted_running
    mse = float(np.mean(residuals**2))
    r_squared = float(1 - np.sum(residuals**2) / np.sum((Y_running - np.mean(Y_running))**2))

    bond_weights_df = pd.DataFrame({
        "isin": [bond.isin for bond in filtered_bonds],
        "name": [getattr(bond, "name", "N/A") for bond in filtered_bonds],
        "maturity": [bond.maturity_date for bond in filtered_bonds],
        "weight": weights,
    })

    return {
        "unified_timeline": unified_cf.index,
        "bond_weights": bond_weights_df,
        "running_totals_target": unified_running["target"].values,
        "unified_cashflows": unified_cf,
        "unified_running_totals": unified_running,
        "predicted_running": predicted_running,
        "residuals": residuals,
        "mse": mse,
        "r_squared": r_squared,
        "num_bonds": len(filtered_bonds),
    }
