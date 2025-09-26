import os
from datetime import date, datetime
from typing import List, Dict, Optional, Any
import pandas as pd
import numpy as np
import pathlib

from dateutil.relativedelta import relativedelta

from jcb_bond_project.models.instrument import Instrument
from jcb_bond_project.database.db import get_conn
from jcb_bond_project.database.query import list_instruments, get_holidays_for_calendar
from jcb_bond_project.utils.jcb_calendar import BusinessDayCalendar
from jcb_bond_project.portfolio.portfolio_optimiser import solve_portfolio_weights
from jcb_bond_project.cashflow_model.builders import cashflows_from_instrument

from jcb_bond_project.database.query import get_latest_data
from jcb_bond_project.utils.settlement import get_settlement_date



# Get DB connection string (Postgres on Render, fallback to SQLite locally)
DB_PATH = os.getenv("DATABASE_URL", "jcb_db.db")

def cashflows_df(
    instruments: List[Instrument],
    *,
    calendar=None,
    frequency: int = 2,
    notional: float = 100.0,
    convention: str = "mf",
    first_coupon_dates: Optional[Dict[str, date]] = None,
) -> pd.DataFrame:
    first_coupon_dates = first_coupon_dates or {}
    rows: List[Dict[str, Any]] = []
    for inst in instruments:
        if (inst.instrument_type or "").strip().lower() != "bond":
            continue
        for r in cashflows_from_instrument(
            inst,
            calendar=calendar,
            frequency=frequency,
            notional=notional,
            convention=convention,
            first_coupon_date=first_coupon_dates.get(inst.isin),
        ):
            rows.append({
                "instrument_id": inst.isin,
                "short_code": inst.short_code,
                "maturity_date": inst.maturity_date,
                "cashflow_date": r.adjusted_date,  # business-day rolled if calendar provided
                "coupon": float(r.coupon_amount),
                "principal": float(r.principal),
                "amount": float(r.coupon_amount + r.principal),
                "is_stub": bool(r.is_stub),
                "accrual_factor": float(r.accrual_factor),
            })
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["cashflow_date", "maturity_date"], kind="mergesort").reset_index(drop=True)
    return df

def cashflow_matrix(df_long: pd.DataFrame) -> pd.DataFrame:
    if df_long.empty:
        return pd.DataFrame()

    # Pivot into wide format
    mat = (
        df_long
        .pivot_table(
            index="cashflow_date",
            columns="instrument_id",
            values="amount",
            aggfunc="sum",
            fill_value=0.0
        )
        .sort_index()
    )
    mat.columns.name = None

    # Reorder columns by maturity
    if "maturity_date" in df_long.columns:
        maturity_order = (
            df_long.drop_duplicates("instrument_id")
            .set_index("instrument_id")["maturity_date"]
            .sort_values()
            .index
        )
        mat = mat.reindex(columns=maturity_order)

    return mat

def generate_target_cashflows(
    target_start_date: date | datetime,
    target_end_date: date | datetime,
    frequency: str = "monthly",
    target_amount: float = 100,
) -> pd.DataFrame:
    """
    Generate target cashflow schedule with cashflow_date as the index.

    Args:
        target_start_date (date/datetime): Start date for target cashflows.
        target_end_date (date/datetime): End date for target cashflows.
        frequency (str): 'monthly' or 'annually'.
        amount (float): Cashflow amount per period.

    Returns:
        pd.DataFrame: DataFrame indexed by cashflow_date with column ['target'].
    """
    if isinstance(target_start_date, datetime):
        target_start_date = target_start_date.date()
    if isinstance(target_end_date, datetime):
        target_end_date = target_end_date.date()

    if frequency == "monthly":
        delta = relativedelta(months=1)
    elif frequency == "annually":
        delta = relativedelta(years=1)
    else:
        raise ValueError("Frequency must be 'monthly' or 'annually'")

    cashflow_dates = []
    current_date = target_start_date
    while current_date <= target_end_date:
        cashflow_dates.append(current_date)
        current_date += delta

    return (
        pd.DataFrame({"target": [target_amount] * len(cashflow_dates)}, index=pd.to_datetime(cashflow_dates))
        .rename_axis("cashflow_date")
    )

def filter_bonds_by_maturity(
    bonds: List[object],
    select_start_date: date | datetime,
    select_end_date: date | datetime
) -> List[object]:
    """
    Filter bonds based on maturity criteria.
    
    Args:
        bonds (list): List of Instrument-like objects with a `maturity_date` attribute.
        select_start_date (datetime.date or datetime): Start date for maturity filtering.
        select_end_date (datetime.date or datetime): End date for maturity filtering.
    
    Returns:
        list: Filtered list of bonds.
    """
    # Normalise to `date`
    if isinstance(select_start_date, datetime):
        select_start_date = select_start_date.date()
    if isinstance(select_end_date, datetime):
        select_end_date = select_end_date.date()

    def to_date(d: date | datetime | None) -> date | None:
        if d is None:
            return None
        return d.date() if isinstance(d, datetime) else d

    # Filter bonds
    filtered = [
        bond for bond in bonds
        if (maturity := to_date(getattr(bond, "maturity_date", None)))
        and select_start_date <= maturity <= select_end_date
    ]

    # Sort by maturity date
    filtered.sort(key=lambda b: to_date(getattr(b, "maturity_date", None)))

    return filtered

def calculate_running_totals(cashflow_matrix: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate running totals (cumulative sums) of a cashflow matrix.
    
    Assumes rows represent time (e.g., cashflow dates) 
    and columns represent instruments or bonds.
    
    Args:
        cashflow_matrix (pd.DataFrame): Cashflow matrix with numeric values.
    
    Returns:
        pd.DataFrame: Matrix of running totals with the same shape, index, and columns.
    """
    return cashflow_matrix.cumsum(axis=0)

# bond_project/portfolio/portfolio_optimiser.py

def create_unified_timeline(
    cf_target: pd.DataFrame,
    cf_mat: pd.DataFrame,
    settlement_date: date | datetime | None = None,
    drop_zeros: bool = True,
) -> pd.DataFrame:
    """
    Create unified timeline combining target dates and bond cashflow dates.
    Structure: rows = dates, columns = ['target'] + bond ISINs.

    Args:
        cf_target (pd.DataFrame): target schedule (index=cashflow_date, column ['target']).
        cf_mat (pd.DataFrame): bond cashflow matrix (index=cashflow_date, columns=ISINs).
        settlement_date (date/datetime, optional): only include dates after this.
        drop_zeros (bool): if True, drop rows where all values are zero.

    Returns:
        pd.DataFrame: unified matrix with aligned target and bond cashflows.
    """
    # Normalise to DatetimeIndex
    cf_target.index = pd.to_datetime(cf_target.index)
    cf_mat.index = pd.to_datetime(cf_mat.index)

    # Combine all dates
    all_dates = cf_target.index.union(cf_mat.index).sort_values()

    # Settlement filter
    if settlement_date is not None:
        settlement_date = pd.to_datetime(settlement_date)
        all_dates = all_dates[all_dates > settlement_date]

    if all_dates.empty:
        raise ValueError("No dates found after settlement date")

    # Reindex both frames
    cf_target = cf_target.reindex(all_dates, fill_value=0.0)
    cf_mat = cf_mat.reindex(all_dates, fill_value=0.0)

    # Concatenate with target first
    unified_cf = pd.concat([cf_target, cf_mat], axis=1)

    # Optionally drop rows with all zeros
    if drop_zeros:
        unified_cf = unified_cf.loc[(unified_cf != 0).any(axis=1)]

    return unified_cf



# bond_project/portfolio/portfolio_optimiser.py
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
        bonds = list_instruments(
            conn,
            instrument_types=["bond"],
            country=country,
            is_green=is_green,
            is_linker=is_linker,
        )
        uk = BusinessDayCalendar(set(get_holidays_for_calendar(conn, country)))

    if not bonds:
        raise ValueError("No bonds found in database for given filters")

    # 2. Filter bonds by maturity
    filtered_bonds = filter_bonds_by_maturity(bonds, settlement_date, select_end_date)
    if not filtered_bonds:
        raise ValueError("No bonds found matching maturity criteria")

    # Settlement date from env var (or default today+1)
    settlement_date = get_settlement_date()

    # ⚠️ MVP version: query one price per bond
    # TODO: replace with batch query for performance
# 2b. Open a new connection for prices
    with get_conn(DB_PATH) as conn:
        prices = []
        for bond in filtered_bonds:
            price = get_latest_data(
                conn,
                instrument_id=bond.isin,
                data_type="dirty_price",
                as_of=settlement_date,
            )
            if price is None:
                raise ValueError(f"No dirty price found for {bond.isin} as of {settlement_date}")
            prices.append(price)

    prices = np.array(prices)

    # 3. Generate bond cashflows
    cf_long = cashflows_df(filtered_bonds, calendar=uk)
    cf_mat = cashflow_matrix(cf_long)

    # 4. Generate target cashflows
    cf_target = generate_target_cashflows(
        select_start_date, select_end_date, frequency, target_amount=100
    )

    # 5. Create unified timeline
    unified_cf = create_unified_timeline(cf_target, cf_mat, settlement_date)
    # DEBUG: export unified cashflows
    unified_cf.to_csv("debug_unified_cf.txt", sep="\t")

    # 6. Running totals
    unified_running = calculate_running_totals(unified_cf)
    # DEBUG: export running totals
    # unified_running.to_csv("debug_unified_running.txt", sep="\t")

    # 7. Split into target and bonds
    Y_running = unified_running["target"].values
    C_matrix = unified_running.drop("target", axis=1).values

    # 8. Solve nominal weights
    nominal_weights = solve_portfolio_weights(C_matrix, Y_running)

    # 9. Scale weights to match user budget AND target cashflows
    # Total cost of nominal weights (value invested at settlement)
    total_cost = np.sum(nominal_weights * prices)

    if total_cost <= 0:
        raise ValueError("Invalid portfolio: total cost of weights is non-positive")

    # Scale factor so invested amount matches user's target_amount
    scale_factor = target_amount / total_cost
    scaled_weights = nominal_weights * scale_factor

    # Recompute predicted running totals with scaled weights
    predicted_running = C_matrix @ scaled_weights
    residuals = Y_running - predicted_running

    # Diagnostics
    mse = float(np.mean(residuals**2))
    r_squared = float(
        1 - np.sum(residuals**2) / np.sum((Y_running - np.mean(Y_running))**2)
    )

    bond_weights_df = pd.DataFrame({
        "isin": [bond.isin for bond in filtered_bonds],
        "name": [getattr(bond, "name", "N/A") for bond in filtered_bonds],
        "maturity": [bond.maturity_date for bond in filtered_bonds],
        "nominal_weight": nominal_weights,
        "scaled_weight": scaled_weights,
        "price": prices,
    })

    # Actual value invested (currency)
    bond_weights_df["value_invested"] = bond_weights_df["scaled_weight"] * bond_weights_df["price"]

    # Sanity check: should sum to ≈ target_amount
    total_invested = bond_weights_df["value_invested"].sum()
    print(f"DEBUG: Total invested = {total_invested:.2f} vs budget {target_amount}")


    bond_weights_df.to_csv("debug_bond_weights_df.txt", sep="\t")

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

def build_portfolio_json(
    select_start_date: date | datetime,
    select_end_date: date | datetime,
    settlement_date: date | datetime,
    target_amount: float = 100,
    frequency: str = "monthly",
    country: str = "UK",
    is_green: bool = False,
    is_linker: bool = False,
) -> dict:
    """Wrapper: run build_portfolio() and convert results to JSON-friendly dict."""
    result = build_portfolio(
        select_start_date,
        select_end_date,
        settlement_date,
        target_amount,
        frequency,
        country,
        is_green,
        is_linker,
    )

    # ✅ Extract weights in JSON-safe format
    weights = result["bond_weights"]

    # 🔍 DEBUG: export weights DataFrame to file
    # debug_path = pathlib.Path("debug_weights_json.txt")
    # weights.to_csv(debug_path, sep="\t", index=False)

    # Convert to JSON-safe format
    weights = weights[["isin", "name", "nominal_weight", "value_invested"]].copy()
    weights["nominal_weight"] = weights["nominal_weight"] * 100
    weights_dicts = weights.to_dict(orient="records")

    df = result["unified_running_totals"].copy()

    # Add explicit portfolio_total column
    df["portfolio_total"] = df.drop(columns=["target"]).sum(axis=1)

    cashflows = {
        "portfolio": [
            {"date": d.strftime("%Y-%m-%d"), "cumulative": float(row["portfolio_total"])}
            for d, row in df.iterrows()
        ],
        "target": [
            {"date": d.strftime("%Y-%m-%d"), "cumulative": float(row["target"])}
            for d, row in df.iterrows()
        ],
    }


    return {
        "mse": result["mse"],
        "r_squared": result["r_squared"],
        "num_bonds": result["num_bonds"],
        "weights": weights_dicts,
        "total_invested": float(sum(w["value_invested"] for w in weights_dicts)),
        "cashflows": cashflows
    }




