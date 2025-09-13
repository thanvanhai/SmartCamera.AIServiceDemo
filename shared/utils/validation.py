# shared/utils/validation.py

from typing import Any, Dict

class ValidationError(Exception):
    """Custom exception for validation errors."""
    pass

def validate_response(response: Dict[str, Any], required_keys: list = None) -> bool:
    """
    Validate a dictionary response from WebAPI or external service.
    
    Args:
        response (Dict[str, Any]): Response data.
        required_keys (list, optional): List of keys that must exist in response.

    Returns:
        bool: True if valid, raises ValidationError if invalid.
    """
    if not isinstance(response, dict):
        raise ValidationError(f"Response is not a dict: {response}")

    if required_keys:
        missing_keys = [k for k in required_keys if k not in response]
        if missing_keys:
            raise ValidationError(f"Missing keys in response: {missing_keys}")

    return True
