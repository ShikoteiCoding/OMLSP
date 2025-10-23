from typing import Any


def jq_dict(data: dict[str, Any], jq: Any = None) -> list[dict[str, Any]]:
    """Apply jq transformation to a dictionnary."""
    return jq.input(data).all()
