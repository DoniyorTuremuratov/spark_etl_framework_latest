from datetime import date


def is_str(obj):
    return type(obj) == str


def is_iso_date_str(obj):
    if not is_str(obj):
        return False
    try:
        date.fromisoformat(obj)
        return True
    except (ValueError, TypeError):
        return False


def is_list_of_strings(obj):
    return type(obj) == list and all(map(is_str, obj))
