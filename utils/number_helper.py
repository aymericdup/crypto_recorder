from decimal import Decimal, InvalidOperation

def is_valid_decimal(value):
    """Check if value can be parsed as decimal"""
    # Handle None, non-string types
    if value is None:
        return False
    
    # Convert to string if needed (handles numbers)
    if not isinstance(value, str):
        value = str(value)
    
    # Check if empty or whitespace
    if not value.strip():
        return False
    
    try:
        Decimal(value.strip())
        return True
    except (InvalidOperation, ValueError, TypeError):
        return False