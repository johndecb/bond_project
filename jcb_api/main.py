import os
import pathlib
from dotenv import load_dotenv

# Load .env from funderly/ before importing project code
load_dotenv()

# Now safe to import project modules
from fastapi import FastAPI
from fastapi import Request
from pydantic import BaseModel
from datetime import date, timedelta
import traceback
from jcb_bond_project.portfolio.portfolio_builders import build_portfolio_json


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
def get_portfolio_summary(req: PortfolioRequest, request: Request):
    """Return portfolio summary. If ?debug=1, include full unified_cashflows."""
    # --- existing logic ---
    settlement = date.today() + timedelta(days=1)
    select_start_date = req.start
    select_end_date = req.start.replace(year=req.start.year + req.tenor)

    # --- new: check for debug flag ---
    debug = request.query_params.get("debug") == "1"

    # --- call the JSON wrapper with debug flag ---
    portfolio = build_portfolio_json(
        select_start_date=select_start_date,
        select_end_date=select_end_date,
        settlement_date=settlement,
        target_amount=req.amount,
        frequency="monthly",
        country="UK",
        is_green=False,
        is_linker=False,
        debug=debug,  # 👈 pass it through
    )

    return portfolio





