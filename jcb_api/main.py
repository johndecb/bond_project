from fastapi import FastAPI
from pydantic import BaseModel
from datetime import date, timedelta
import traceback
from jcb_bond_project.portfolio.builders import build_portfolio_json

app = FastAPI(title="Bond Project API")

class PortfolioRequest(BaseModel):
    amount: float
    start: date
    tenor: int   # years

@app.get("/")
def root():
    return {"status": "ok"}

# 🥕 Sous chef = detailed raw cashflows
@app.post("/portfolio/cashflows")
def get_cashflows(req: PortfolioRequest):
    settlement = date.today() + timedelta(days=1)
    select_start_date = req.start
    select_end_date = req.start.replace(year=req.start.year + req.tenor)

    return build_portfolio_json(
        select_start_date=select_start_date,
        select_end_date=select_end_date,
        settlement_date=settlement,
        target_amount=req.amount,
    )

# 🍳 Head chef = summary built on top of raw cashflows
@app.post("/portfolio/summary")
def get_portfolio_summary(req: PortfolioRequest):
    try:
        settlement = date.today() + timedelta(days=1)
        select_start_date = req.start
        select_end_date = req.start.replace(year=req.start.year + req.tenor)

        raw = build_portfolio_json(
            select_start_date=select_start_date,
            select_end_date=select_end_date,
            settlement_date=settlement,
            target_amount=req.amount,
        )

        # summary layer (presentation-friendly)
        summary = {
            "mse": raw["mse"],
            "r_squared": raw["r_squared"],
            "num_bonds": raw["num_bonds"],
            "weights": raw["bond_weights"],
            "cashflows": {
                "portfolio": [
                    {
                        "date": row["date"],
                        "cumulative": float(sum(v for k, v in row.items() if k not in ["date", "target"]))
                    }
                    for row in raw["unified_cashflows"]
                ],
                "target": [
                    {"date": row["date"], "cumulative": row["target"]}
                    for row in raw["unified_cashflows"]
                ],
            },
        }
        return summary

    except Exception as e:
        traceback.print_exc()
        return {"error": str(e)}




