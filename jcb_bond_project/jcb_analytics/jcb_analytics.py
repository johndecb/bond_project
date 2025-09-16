import xlwings as xw
import requests

def main():
    wb = xw.Book.caller()
    wb.sheets[0]["A1"].value = "✅ JCB Analytics is connected!"

@xw.func
@xw.arg('isin', str)
@xw.arg('data_type', str, default='amount_outstanding')
def jGet_Instrument_Data(isin, data_type='amount_outstanding'):
    url = f"http://127.0.0.1:8000/instrument-data?isin={isin}&data_type={data_type}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if data and "value" in data[-1]:
                return data[-1]["value"]  # return latest
            else:
                return "No data"
        else:
            return f"Error {response.status_code}"
    except Exception as e:
        return str(e)