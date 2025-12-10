from dataclasses import dataclass

@dataclass
class ServerResponse:
    success: bool
    error: str | None = None