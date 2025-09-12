from datetime import date, datetime
from typing import List, Dict, Optional, Any
import pandas as pd
import numpy as np

from models.instrument import Instrument
from cashflow_model.conv_bond_model import CashflowModel, CashflowRow
from dateutil.relativedelta import relativedelta

def cashflows_from_instrument(
    inst: Instrument,
    *,
    calendar=None,
    frequency: int = 2,
    notional: float = 100.0,
    convention: str = "mf",
    first_coupon_date: Optional[date] = None,
) -> List[CashflowRow]:
    if (inst.instrument_type or "").strip().lower() != "bond":
        raise ValueError(f"Expected a bond, got {inst.instrument_type!r}")
    if inst.first_issue_date is None or inst.maturity_date is None or inst.coupon_rate is None:
        raise ValueError(f"Missing dates/coupon for {inst.isin}")

    model = CashflowModel(
        issue_date=inst.first_issue_date,
        maturity_date=inst.maturity_date,
        coupon_rate=float(inst.coupon_rate),
        frequency=frequency,
        notional=notional,
        calendar=calendar,
        convention=convention,
        first_coupon_length=inst.first_coupon_length,
        first_coupon_date=first_coupon_date,
    )
    return model.generate_cashflow_schedule()

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
    amount: float = 1_000_000,
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
        pd.DataFrame({"target": [amount] * len(cashflow_dates)}, index=pd.to_datetime(cashflow_dates))
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

def solve_portfolio_weights(C_matrix, Y_vector):
    """
    Solve for portfolio weights using least squares: B = [C^T C]^-1  C^T  Y
    
    Args:
        C_matrix: numpy.ndarray - running totals matrix for bonds (M time periods, N bonds)
        Y_vector: numpy.ndarray - running totals vector for target (M time periods, a target series)
    
    Returns:
        numpy.ndarray - portfolio weights (N bonds)
    """
    
    # Debug: Print shapes
    print(f"solve_portfolio_weights input shapes:")
    print(f"  C_matrix: {C_matrix.shape} (should be M_periods x N_bonds)")
    print(f"  Y_vector: {Y_vector.shape} (should be M_periods)")
    

    # Calculate [C^T C]^-1  C^T  Y
    C_transpose = C_matrix.T  # N_bonds x M_periods
    
    print(f"  C_transpose: {C_transpose.shape}")
    
    # Check for singularity and use pseudo-inverse if needed
    try:
        # Normal equation: (C^T @ C) @ weights = C^T @ Y
        CTc = C_transpose @ C_matrix  # N_bonds x N_bonds
        CTy = C_transpose @ Y_vector  # N_bonds,
        
        print(f"  CTc: {CTc.shape}")
        print(f"  CTy: {CTy.shape}")
        
        CTc_inv = np.linalg.inv(CTc)
        weights = CTc_inv @ CTy
        
        print(f"  Using normal inverse")
        
    except np.linalg.LinAlgError:
        # Use pseudo-inverse for singular matrices
        print(f"  Matrix is singular, using pseudo-inverse")
        
        # Alternative: use numpy's least squares solver
        weights, residuals, rank, s = np.linalg.lstsq(C_matrix, Y_vector, rcond=None)
        
        print(f"  Least squares rank: {rank}/{min(C_matrix.shape)}")
        if len(residuals) > 0:
            print(f"  Least squares residuals: {residuals[0]:.2f}")
    
    print(f"  Output weights shape: {weights.shape}")
    print()
    
    return weights

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