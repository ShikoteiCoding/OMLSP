from catalog.secret_catalog import SecretCatalog
from catalog.sink_catalog import SinkCatalog
from catalog.source_catalog import SourceCatalog
from catalog.table_catalog import TableCatalog
from catalog.view_catalog import ViewCatalog


class Catalog:
    """
    In-memory catalog to store entities.
    """

    def __init__(self) -> None:
        self._tables: dict[str, TableCatalog] = {}
        self._sources: dict[str, SourceCatalog] = {}
        self._views: dict[str, ViewCatalog] = {}
        self._sinks: dict[str, SinkCatalog] = {}
        self._secrets: dict[str, SecretCatalog] = {}

    # Write methods
    def create_table(self, context: TableCatalog) -> None:
        self._tables[context.name] = context

    def create_source(self, context: SourceCatalog) -> None:
        self._sources[context.name] = context

    def create_view(self, context: ViewCatalog) -> None:
        self._views[context.name] = context

    def create_sink(self, context: SinkCatalog) -> None:
        self._sinks[context.name] = context

    def create_secret(self, context: SecretCatalog) -> None:
        self._secrets[context.name] = context

    def drop_table(self, name: str) -> None:
        if name in self._tables:
            del self._tables[name]

    def drop_source(self, name: str) -> None:
        if name in self._sources:
            del self._sources[name]

    def drop_view(self, name: str) -> None:
        if name in self._views:
            del self._views[name]

    def drop_sink(self, name: str) -> None:
        if name in self._sinks:
            del self._sinks[name]

    def drop_secret(self, name: str) -> None:
        if name in self._secrets:
            del self._secrets[name]

    # Read methods
    def get_table(self, name: str) -> TableCatalog | None:
        return self._tables.get(name)

    def get_source(self, name: str) -> SourceCatalog | None:
        return self._sources.get(name)

    def get_view(self, name: str) -> ViewCatalog | None:
        return self._views.get(name)

    def get_sink(self, name: str) -> SinkCatalog | None:
        return self._sinks.get(name)

    def get_secret(self, name: str) -> SecretCatalog | None:
        return self._secrets.get(name)

    def get_tables(self) -> dict[str, TableCatalog]:
        return self._tables.copy()

    def get_sources(self) -> dict[str, SourceCatalog]:
        return self._sources.copy()

    def get_views(self) -> dict[str, ViewCatalog]:
        return self._views.copy()

    def get_sinks(self) -> dict[str, SinkCatalog]:
        return self._sinks.copy()

    def get_secrets(self) -> dict[str, SecretCatalog]:
        return self._secrets.copy()


class CatalogReader:
    """
    Catalog reader class for read-only access.
    """

    def __init__(self, catalog: Catalog) -> None:
        self._catalog = catalog

    def get_table(self, name: str) -> TableCatalog | None:
        return self._catalog.get_table(name)

    def get_source(self, name: str) -> SourceCatalog | None:
        return self._catalog.get_source(name)

    def get_view(self, name: str) -> ViewCatalog | None:
        return self._catalog.get_view(name)

    def get_sink(self, name: str) -> SinkCatalog | None:
        return self._catalog.get_sink(name)

    def get_secret(self, name: str) -> SecretCatalog | None:
        return self._catalog.get_secret(name)

    def get_tables(self) -> dict[str, TableCatalog]:
        return self._catalog.get_tables()

    def get_sources(self) -> dict[str, SourceCatalog]:
        return self._catalog.get_sources()

    def get_views(self) -> dict[str, ViewCatalog]:
        return self._catalog.get_views()

    def get_sinks(self) -> dict[str, SinkCatalog]:
        return self._catalog.get_sinks()

    def get_secrets(self) -> dict[str, SecretCatalog]:
        return self._catalog.get_secrets()


class CatalogWriter:
    """
    Catalog writer class for write access.
    """

    def __init__(self, catalog: Catalog) -> None:
        self._catalog = catalog

    def create_table(self, context: TableCatalog) -> None:
        self._catalog.create_table(context)

    def create_source(self, context: SourceCatalog) -> None:
        self._catalog.create_source(context)

    def create_view(self, context: ViewCatalog) -> None:
        self._catalog.create_view(context)

    def create_sink(self, context: SinkCatalog) -> None:
        self._catalog.create_sink(context)

    def create_secret(self, context: SecretCatalog) -> None:
        self._catalog.create_secret(context)

    def drop_table(self, name: str) -> None:
        self._catalog.drop_table(name)

    def drop_source(self, name: str) -> None:
        self._catalog.drop_source(name)

    def drop_view(self, name: str) -> None:
        self._catalog.drop_view(name)

    def drop_sink(self, name: str) -> None:
        self._catalog.drop_sink(name)

    def drop_secret(self, name: str) -> None:
        self._catalog.drop_secret(name)
