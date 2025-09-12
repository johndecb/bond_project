from fastapi import FastAPI
from pydantic import BaseModel
from datetime import date, timedelta
from typing import List
import pandas as pd

from portfolio.builders import build_portfolio

app = FastAPI(title="Bond Project API")

class PortfolioRequest(BaseModel):
    amount: float
    start: date
    tenor: int   # years

class CashflowResponse(BaseModel):
    date: date
    amount: float

class BondWeightResponse(BaseModel):
    isin: str
    name: str
    maturity: date
    weight: float

@app.post("/portfolio/cashflows")
def get_cashflows(req: PortfolioRequest):
    settlement = date.today() + timedelta(days=1)
    select_start_date = req.start
    select_end_date = req.start.replace(year=req.start.year + req.tenor)

    portfolio = build_portfolio(
        select_start_date=select_start_date,
        select_end_date=select_end_date,
        settlement_date=settlement,
        target_amount=req.amount,
        frequency="monthly",
        country="UK",
        is_green=False,
        is_linker=False,
    )

    running = []
    if isinstance(portfolio["unified_running_totals"], pd.DataFrame):
        for d, row in portfolio["unified_running_totals"].iterrows():
            running.append({
                "date": d.date(),
                "target": float(row["target"]),
                "bonds": float(row.drop("target").sum())
            })

    return {
        "running_totals": running,
        "mse": portfolio["mse"],
        "r_squared": portfolio["r_squared"],
    }
