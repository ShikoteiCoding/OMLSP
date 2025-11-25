from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

from task.task import BaseTask
from task.types import ArtifactId, HasData
from context.context import CreateContext
from store.db import (
    METADATA_TABLE_TABLE_NAME,
    METADATA_VIEW_TABLE_NAME,
    METADATA_SINK_TABLE_NAME,
    METADATA_SOURCE_TABLE_NAME,
)
from context.context import (
    CreateSinkContext,
    CreateHTTPSourceContext,
    CreateHTTPTableContext,
    CreateWSSourceContext,
    CreateWSTableContext,
    CreateViewContext,
    CreateHTTPLookupTableContext,
)


@dataclass
class ArtifactInfo:
    task: BaseTask | None
    has_data: bool
    metadata_table: str
    metadata_column: str
    cleanup_callback: Optional[Callable] = None


class ArtifactCatalog:
    """
    Catalog of artifact.
    Stores BaseTask objects, context type, HasData bool, and metadata by their task_id.
    """

    #: Flag to detect init
    _instanciated: bool = False

    #: Singleton instance
    _instance: ArtifactCatalog

    CTX_METADATA_MAP: dict[type, tuple[str, str]] = {
        CreateHTTPTableContext: (METADATA_TABLE_TABLE_NAME, "table_name"),
        CreateWSTableContext: (METADATA_TABLE_TABLE_NAME, "table_name"),
        CreateHTTPLookupTableContext: (METADATA_TABLE_TABLE_NAME, "table_name"),
        CreateViewContext: (METADATA_VIEW_TABLE_NAME, "view_name"),
        CreateSinkContext: (METADATA_SINK_TABLE_NAME, "sink_name"),
        CreateHTTPSourceContext: (METADATA_SOURCE_TABLE_NAME, "source_name"),
        CreateWSSourceContext: (METADATA_SOURCE_TABLE_NAME, "source_name"),
    }

    _artifact_id_to_artifact: dict[ArtifactId, ArtifactInfo]

    def __init__(self):
        raise NotImplementedError("Singleton — use ArtifactCatalog.get_instance()")

    def init(self):
        self._artifact_id_to_artifact: dict[ArtifactId, ArtifactInfo] = {}

    @classmethod
    def get_instance(cls) -> ArtifactCatalog:
        if cls._instanciated:
            return cls._instance

        cls._instance = cls.__new__(cls)
        cls._instance.init()
        cls._instanciated = True

        return cls._instance

    def resolve_metadata(self, ctx_type: type) -> tuple[str, str]:
        return self.CTX_METADATA_MAP.get(
            ctx_type,
            (METADATA_TABLE_TABLE_NAME, "table_name"),  # default
        )

    def add(
        self,
        task_id: ArtifactId,
        *,
        task: BaseTask | None,
        has_data: HasData,
        ctx_type: type[CreateContext],
        cleanup_callback: Optional[Callable] = None,
    ) -> None:
        """Add an artifact to the catalog."""
        metadata_table, metadata_column = self.resolve_metadata(ctx_type)

        self._artifact_id_to_artifact[task_id] = ArtifactInfo(
            task=task,
            has_data=has_data,
            metadata_table=metadata_table,
            metadata_column=metadata_column,
            cleanup_callback=cleanup_callback,
        )

    def remove(self, task_id: ArtifactId) -> None:
        """Remove an artifact."""
        self._artifact_id_to_artifact.pop(task_id, None)

    def get(self, task_id: ArtifactId) -> ArtifactInfo:
        """Retrieve an artifact by ID."""
        return self._artifact_id_to_artifact.get(
            task_id,
            ArtifactInfo(None, False, "", "", None),  # default
        )

    def has_task(self, task_id: ArtifactId) -> bool:
        """Check if the task is in the catalog."""
        return task_id in self._artifact_id_to_artifact


catalog = ArtifactCatalog.get_instance()
