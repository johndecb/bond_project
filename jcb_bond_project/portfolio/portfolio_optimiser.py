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
