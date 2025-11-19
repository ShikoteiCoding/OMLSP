from httpx import Response

from typing import Any


class BasePagination:
    def __init__(self, meta: dict[str, Any]):
        self.meta = meta

    def update_params(self, cursor: str | None, params: dict[str, str]):
        pass

    def extract_next_cursor(self, batch: list[dict[str, Any]], response: Response):
        pass


class NoPagination(BasePagination):
    def __init__(self, meta: dict[str, Any]):
        self.meta = meta

    def update_params(self, _: str | None, params: dict[str, str]) -> dict:
        return {}

    def extract_next_cursor(self, batch: list[dict[str, Any]], response: Response):
        """Return the next cursor or None"""
        return None


class PageBasedPagination(BasePagination):
    def __init__(self, meta):
        super().__init__(meta)
        self.page_param = meta.get("page_param", "page")
        self.size_param = meta.get("size_param", "limit")
        self.page = 0

    def update_params(self, _: str | None, params: dict[str, Any]) -> dict[str, Any]:
        params[self.page_param] = self.page
        self.page += 1
        return params


class CursorBasedPagination(BasePagination):
    def __init__(self, meta):
        super().__init__(meta)
        self.cursor_param = meta.get("cursor_param", "cursor")
        self.cursor_id = meta.get("cursor_id", "id")

    def update_params(
        self, cursor: str | None, params: dict[str, str]
    ) -> dict[str, Any]:
        if cursor:
            params[self.cursor_param] = cursor
        return params

    def extract_next_cursor(
        self, batch: list[dict[str, Any]], response: Response
    ) -> Any:
        return batch[-1].get(self.cursor_id)


class HeaderBasedPagination(BasePagination):
    def __init__(self, meta):
        super().__init__(meta)
        self.cursor_param = meta.get("cursor_param", "cursor")
        self.next_cursor_header = meta.get("next_header")

    def update_params(self, cursor: str, params: dict[str, str]) -> dict[str, Any]:
        if cursor:
            params[self.cursor_param] = cursor
        return params

    def extract_next_cursor(
        self, batch: list[dict[str, Any]], response: Response
    ) -> Any:
        if not self.next_cursor_header:
            return None
        return response.headers.get(self.next_cursor_header)


PAGINATION_DISPATCH: dict[str, type[BasePagination]] = {
    "no-pagination": NoPagination,
    "page-based": PageBasedPagination,
    "cursor-based": CursorBasedPagination,
    "header-based": HeaderBasedPagination,
}
