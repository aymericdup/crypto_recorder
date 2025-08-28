from decimal import Decimal, InvalidOperation

def is_valid_decimal(value):
    """Check if string is filled and can be parsed as decimal"""
    if not value or not value.strip():  # Check if empty or whitespace
        return False
    
    try:
        Decimal(value.strip())
        return True
    except (InvalidOperation, ValueError):
        return False