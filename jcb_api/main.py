import os
import pathlib
from dotenv import load_dotenv

# ---------------------------------------------------------------------
# Load environment file
# ---------------------------------------------------------------------
# Determine project root (bond_project/)
root = pathlib.Path(__file__).resolve().parents[2]

# Choose .env.local or .env.render automatically
env_name = os.getenv("ENV", "local")
env_file = root / f".env.{env_name}"

# Try local or Render env file
if env_file.exists():
    load_dotenv(dotenv_path=env_file)
else:
    # Fallback: look for funderly/.env (if run from there)
    funderly_env = root.parent / "funderly" / f".env.{env_name}"
    if funderly_env.exists():
        load_dotenv(dotenv_path=funderly_env)
    else:
        load_dotenv(dotenv_path=root / ".env")

# Read database connection string
DB_PATH = os.getenv("DATABASE_URL")

# ---------------------------------------------------------------------
# Import project modules
# ---------------------------------------------------------------------
from fastapi import FastAPI, Request
from pydantic import BaseModel
from datetime import date, timedelta
import traceback
from jcb_bond_project.portfolio.portfolio_builders import build_portfolio_json

# ---------------------------------------------------------------------
# FastAPI app setup
# ---------------------------------------------------------------------
app = FastAPI(title="Bond Project API")

class PortfolioRequest(BaseModel):
    amount: float
    start: date
    tenor: int  # years


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
    settlement = date.today() + timedelta(days=1)
    select_start_date = req.start
    select_end_date = req.start.replace(year=req.start.year + req.tenor)
    debug = request.query_params.get("debug") == "1"

    portfolio = build_portfolio_json(
        select_start_date=select_start_date,
        select_end_date=select_end_date,
        settlement_date=settlement,
        target_amount=req.amount,
        frequency="monthly",
        country="UK",
        is_green=False,
        is_linker=False,
        debug=debug,
    )
    return portfolio

