# the following code uses the data on the DMO spreadsheet to give cleaner more enhance data
# parse_coupon_decimal string in the long name to a flaot to record the coupon
# short_bond_code uses a UK standard for short codes on UK bonds
# classify_bond is_linker, is_greeen

import re
from fractions import Fraction

# Unicode → ASCII
UNICODE_FRACTIONS = {
    '¼': '1/4', '½': '1/2', '¾': '3/4',
    '⅛': '1/8', '⅜': '3/8', '⅝': '5/8', '⅞': '7/8'
}

# ASCII → short code letters
FRACTION_MAP = {
    '1/8': 'e', '1/4': 'q', '3/8': 't',
    '1/2': 'h', '5/8': 'f', '3/4': 'r', '7/8': 's'
}

def parse_coupon_decimal(name: str) -> float:
    match = re.match(r'(?:(\d+)\s+)?(\d+)?(?:\s*([¼½¾⅛⅜⅝⅞]|[1-7]/8))?%', name)
    if not match:
        return None
    whole, leading, fraction = match.groups()
    number = whole if whole is not None else (leading if leading is not None else '0')
    fraction_str = UNICODE_FRACTIONS.get(fraction, fraction)
    if fraction_str:
        return float(number) + float(Fraction(fraction_str))
    return float(number)

def short_bond_code(name: str) -> str:
    match = re.match(r'(?:(\d+)\s+)?(\d+)?(?:\s*([¼½¾⅛⅜⅝⅞]|[1-7]/8))?\s*%.*?(\d{4})', name)
    if not match:
        return None
    whole, leading, fraction, year = match.groups()
    number = whole if whole is not None else (leading if leading is not None else '0')
    fraction = UNICODE_FRACTIONS.get(fraction, fraction)
    letter = FRACTION_MAP.get(fraction or '', '_')
    if "index-linked" in name.lower():
        return f"il{year[-2:]}"
    return f"{number}{letter}{year[-2:]}"

def classify_bond(name: str):
    name_lower = name.lower()
    return {
        "Short Code": short_bond_code(name),
        "Coupon": parse_coupon_decimal(name),
        "Is Green": "green" in name_lower,
        "Is Linker": "index-linked" in name_lower,
        "Index Lag": (
            3 if "index-linked" in name_lower and "gilt" in name_lower else
            8 if "index-linked" in name_lower and "stock" in name_lower else
            None
        )
    }