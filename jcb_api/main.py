from fastapi import FastAPI
from pydantic import BaseModel
from datetime import date, timedelta
import pandas as pd
import traceback

from jcb_bond_project.portfolio.builders import build_portfolio

app = FastAPI(title="Bond Project API")

class PortfolioRequest(BaseModel):
    amount: float
    start: date
    tenor: int   # years

@app.get("/")
def root():
    return {"status": "ok"}

@app.post("/portfolio/summary")
def get_portfolio_summary(req: PortfolioRequest):
    try:
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

        # 1. Cashflows
        cashflows_portfolio = []
        cashflows_target = []
        if isinstance(portfolio["unified_running_totals"], pd.DataFrame):
            for d, row in portfolio["unified_running_totals"].iterrows():
                cashflows_portfolio.append({
                    "date": d.date(),
                    "cumulative": float(row.drop("target").sum())
                })
                cashflows_target.append({
                    "date": d.date(),
                    "cumulative": float(row["target"])
                })

        # 2. Weights (dummy for now)
        weights = [
            {"isin": "GB00B1VWPJ53", "weight": 0.45},
            {"isin": "GB00B52WS153", "weight": 0.30},
            {"isin": "GB00B84Z9V04", "weight": 0.25}
        ]

        # 3. Performance (dummy for now)
        performance = [
            {"date": "2024-09-01", "value": 100.0},
            {"date": "2024-10-01", "value": 101.2},
            {"date": "2024-11-01", "value": 99.5}
        ]

        return {
            "cashflows": {
                "portfolio": cashflows_portfolio,
                "target": cashflows_target,
            },
            "weights": weights,
            "performance": performance,
            "mse": portfolio["mse"],
            "r_squared": portfolio["r_squared"],
        }

    except Exception as e:
        # Print full traceback to logs
        traceback.print_exc()
        return {"error": str(e)}


