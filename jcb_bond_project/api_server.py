from fastapi import FastAPI
from pydantic import BaseModel
from jcb_bond_project.cashflow_model.conv_bond_model import CashflowModel
import datetime

app = FastAPI(title="JCB Bond Project API")

class CashflowRequest(BaseModel):
    issue_date: datetime.date
    maturity_date: datetime.date
    coupon_rate: float
    frequency: int
    notional: float = 100.0

@app.post("/cashflows")
def get_cashflows(req: CashflowRequest):
    model = CashflowModel(
        issue_date=req.issue_date,
        maturity_date=req.maturity_date,
        coupon_rate=req.coupon_rate,
        frequency=req.frequency,
        notional=req.notional,
    )
    cashflows = model.generate_cashflow_schedule()
    return {"cashflows": [cf.__dict__ for cf in cashflows]}
