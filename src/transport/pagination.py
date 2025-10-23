from httpx import Response

from typing import Any, Protocol


class Pagination(Protocol):
    def update_params(self):
        pass

    def extract_next_cursor(self):
        pass

    def has_cursor(self):
        pass


class BasePagination:
    def __init__(self, meta: dict[str, Any]):
        self.meta = meta

    def update_params(self, cursor: str, params: dict[str, str]):
        pass

    def extract_next_cursor(self, batch: list[dict[str, Any]], response: Response):
        pass

    def has_cursor(self):
        pass


class NoPagination(BasePagination):
    def __init__(self, meta: dict[str, Any]):
        self.meta = meta

    def update_params(self, cursor: str, params: dict[str, str]) -> dict:
        return {}

    def extract_next_cursor(self, batch: list[dict[str, Any]], response: Response):
        """Return the next cursor or None"""
        return None

    def has_cursor(self) -> bool:
        """Returns True if this strategy uses cursors"""
        return False


class PageBasedPagination(BasePagination):
    def __init__(self, meta):
        super().__init__(meta)
        self.page_param = meta.get("page_param", "page")
        self.size_param = meta.get("size_param", "limit")
        self.page = 0

    def update_params(self, cursor: str, params: dict[str, Any]):
        params[self.page_param] = self.page
        self.page += 1
        return params


class CursorBasedPagination(BasePagination):
    def __init__(self, meta):
        super().__init__(meta)
        self.cursor_param = meta.get("cursor_param", "cursor")
        self.cursor_id = meta.get("cursor_id", "id")

    def update_params(self, cursor: str, params: dict[str, str]):
        if cursor:
            params[self.cursor_param] = cursor
        return params

    def has_cursor(self) -> bool:
        return True

    def extract_next_cursor(self, batch: list[dict[str, Any]], response: Response):
        return batch[-1].get(self.cursor_id)


class HeaderBasedPagination(BasePagination):
    def __init__(self, meta):
        super().__init__(meta)
        self.cursor_param = meta.get("cursor_param", "cursor")
        self.next_cursor_header = meta.get("next_header")

    def update_params(self, cursor: str, params: dict[str, str]):
        if cursor:
            params[self.cursor_param] = cursor
        return params

    def has_cursor(self) -> bool:
        return True

    def extract_next_cursor(self, batch: list[dict[str, Any]], response: Response):
        if not self.next_cursor_header:
            return None
        return response.headers.get(self.next_cursor_header)


PAGINATION_DISPATCH: dict[str, type[BasePagination]] = {
    "no-pagination": NoPagination,
    "page-based": PageBasedPagination,
    "cursor-based": CursorBasedPagination,
    "header-based": HeaderBasedPagination,
}
