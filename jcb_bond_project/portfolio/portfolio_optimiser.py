from datetime import date, datetime
from typing import List, Dict, Optional, Any
import pandas as pd
import numpy as np

from jcb_bond_project.models.instrument import Instrument
from jcb_bond_project.cashflow_model.conv_bond_model import CashflowModel, CashflowRow
from dateutil.relativedelta import relativedelta


def solve_portfolio_weights(C_matrix, Y_vector):
    """
    Solve for portfolio weights as fractions (sum = 1).
    Assumes the target cashflows have been generated
    with a dummy value of 100 per period.
    """
    import numpy as np

    C_transpose = C_matrix.T
    CTc = C_transpose @ C_matrix
    CTy = C_transpose @ Y_vector
    try:
        weights = np.linalg.solve(CTc, CTy)
    except np.linalg.LinAlgError:
        weights, _, _, _ = np.linalg.lstsq(C_matrix, Y_vector, rcond=None)

    # Normalise to sum = 1
    weights_norm = weights / weights.sum()

    return weights_norm

class DummyResult:
    """Minimal stand-in for scipy.optimize result object."""
    def __init__(self, x, success=True, fun=0.0):
        self.x = x
        self.success = success
        self.fun = fun


def optimise_bond_portfolio(
    nominal_weights: np.ndarray,
    C_matrix: np.ndarray,
    dates,
    prices: np.ndarray,
    amount: float,
    *,
    r_pos: float = 0.02,
    r_neg: float = -0.50,
) -> DummyResult:
    """
    Placeholder for nonlinear bond portfolio optimisation.
    Currently just returns the LSQ weights unchanged.

    Args:
        nominal_weights: np.ndarray of LSQ solution.
        C_matrix: matrix of cumulative cashflows (dates x bonds).
        dates: timeline of cashflows.
        prices: current dirty prices per bond.
        amount: target investment amount.
        r_pos/r_neg: positive/negative balance annualised rates.

    Returns:
        DummyResult with .x (weights), .success, and .fun.
    """

    # For now, simply return LSQ weights as-is
    x = np.copy(nominal_weights)
    success = True

    # Optionally simulate a "final balance" just for reporting
    predicted_running = C_matrix @ x
    final_balance = float(predicted_running[-1])

    # Negative fun since typical optimisers minimise -objective
    fun = -final_balance

    return DummyResult(x=x, success=success, fun=fun)

