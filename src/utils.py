def unnest_dict(properties: dict[str, str], sep: str = ".") -> dict[str, str | dict]:
    """
    Unnest dictionnary. Explode keys with "sep" in name to dict.

    Example:
    "headers.api-key": "123" -> "headers": {"api-key": "123"}
    """
    res = {}

    for key, value in properties.items():
        parts = key.split(sep)
        current = res
        for i, part in enumerate(parts):
            if i == len(parts) - 1:
                current[part] = value
            else:
                if part not in current:
                    current[part] = {}
                current = current[part]

    return res
