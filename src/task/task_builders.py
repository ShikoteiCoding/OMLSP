from typing import Type, Callable, Dict

from context.context import (
    CreateSinkContext,
    CreateHTTPSourceContext,
    CreateHTTPTableContext,
    CreateWSSourceContext,
    CreateWSTableContext,
    CreateViewContext,
    CreateHTTPLookupTableContext,
)

from engine.engine import (
    build_lookup_table_prehook,
    build_continuous_source_executable,
    build_scheduled_source_executable,
    build_sink_executable,
    build_transform_executable,
)

from task.task import (
    SinkTask,
    TransformTask,
    ScheduledSourceTask,
    ContinuousSourceTask,
)
from loguru import logger

TASK_BUILDERS: Dict[Type, Callable] = {}

# ----------------------------
# Registry decorator
# ----------------------------


def task_builder(ctx_type: Type):
    def wrapper(func):
        TASK_BUILDERS[ctx_type] = func
        return func

    return wrapper


# ----------------------------
# Concrete builders
# ----------------------------


@task_builder(CreateSinkContext)
def build_sink(manager, ctx: CreateSinkContext):
    task = SinkTask[ctx._out_type](ctx.name, manager.transform_conn)
    # TODO: subscribe to many upstreams
    for upstream in ctx.upstreams:
        task.subscribe(manager._sources[upstream])

    task.register(build_sink_executable(ctx, manager.backend_conn))
    logger.warning(f"Registering task={ctx.name}, upstreams={ctx.upstreams}")
    return task


@task_builder(CreateHTTPTableContext)
@task_builder(CreateHTTPSourceContext)
def build_http_scheduled(manager, ctx):
    task = ScheduledSourceTask[ctx._out_type](
        ctx.name,
        manager.backend_conn,
        manager._scheduled_executables,
        ctx.trigger,
    )

    manager._sources[ctx.name] = task.register(build_scheduled_source_executable(ctx))

    return task


@task_builder(CreateWSTableContext)
@task_builder(CreateWSSourceContext)
def build_ws(manager, ctx):
    task = ContinuousSourceTask[ctx._out_type](
        ctx.name, manager.backend_conn, manager._nursery
    )

    # TODO: make WS Task dynamic by registering the on_start function
    manager._sources[ctx.name] = task.register(
        build_continuous_source_executable(ctx, manager.backend_conn)
    )

    return task


@task_builder(CreateHTTPLookupTableContext)
def build_lookup(manager, ctx):
    build_lookup_table_prehook(ctx, manager.backend_conn)
    return None


@task_builder(CreateViewContext)
def build_view(manager, ctx):
    task = TransformTask[ctx._out_type](ctx.name, manager.transform_conn)

    for upstream in ctx.upstreams:
        task.subscribe(manager._sources[upstream])

    task.register(build_transform_executable(ctx, manager.backend_conn))
    return task
