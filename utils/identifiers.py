def generate_jcb_isin(
    currency: str,
    instrument_type: str,
    tenor: str,
    index: str = None
) -> str:
    """
    Generate a synthetic ISIN-like identifier for internal use.

    Example: JCBGBPIRS10YSONIA
    """
    parts = ["JCB", currency.upper(), instrument_type.upper(), tenor.upper()]
    if index:
        parts.append(index.upper())
    return "".join(parts)
