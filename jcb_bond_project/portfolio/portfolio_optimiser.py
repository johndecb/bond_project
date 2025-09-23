from datetime import date, datetime
from typing import List, Dict, Optional, Any
import pandas as pd
import numpy as np

from jcb_bond_project.models.instrument import Instrument
from jcb_bond_project.cashflow_model.conv_bond_model import CashflowModel, CashflowRow
from dateutil.relativedelta import relativedelta


def solve_portfolio_weights(C_matrix: np.ndarray, Y_vector: np.ndarray) -> np.ndarray:
    """
    Solve for portfolio weights using least squares:
        w = argmin ||C w - Y||^2
    
    Args:
        C_matrix: np.ndarray, shape (M_periods, N_bonds)
            Running totals (or cashflow) matrix for bonds.
        Y_vector: np.ndarray, shape (M_periods,)
            Running totals vector for target.
    
    Returns:
        np.ndarray of shape (N_bonds,)
            Portfolio weights.
    """
    C_matrix = np.asarray(C_matrix, dtype=float)
    Y_vector = np.asarray(Y_vector, dtype=float).ravel()

    # Debugging
    print(f"[solve_portfolio_weights] Input shapes:")
    print(f"  C_matrix: {C_matrix.shape}")
    print(f"  Y_vector: {Y_vector.shape}")

    # Normal equations
    C_T = C_matrix.T
    CTc = C_T @ C_matrix
    CTy = C_T @ Y_vector

    try:
        # Prefer solve() over explicit inverse
        weights = np.linalg.solve(CTc, CTy)
        print("  Using np.linalg.solve (normal equations).")
    except np.linalg.LinAlgError:
        print("  Matrix singular/ill-conditioned, falling back to lstsq.")
        weights, residuals, rank, s = np.linalg.lstsq(C_matrix, Y_vector, rcond=None)
        print(f"  Least squares rank: {rank}/{min(C_matrix.shape)}")
        if len(residuals) > 0:
            print(f"  Residual norm: {residuals[0]:.4f}")

    print(f"  Output weights shape: {weights.shape}")
    return weights