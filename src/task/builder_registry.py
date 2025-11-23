from typing import Callable, Dict

from context.context import (
    CreateContext,
    CreateSinkContext,
    CreateHTTPSourceContext,
    CreateHTTPTableContext,
    CreateWSSourceContext,
    CreateWSTableContext,
    CreateViewContext,
    CreateHTTPLookupTableContext,
)

from engine.engine import (
    build_lookup_callback,
    build_continuous_source_executable,
    build_scheduled_source_executable,
    build_sink_executable,
    build_transform_executable,
)
from store.lookup import callback_store
from task.task import (
    SinkTask,
    TransformTask,
    ScheduledSourceTask,
    ContinuousSourceTask,
)
from loguru import logger

TASK_REGISTER: Dict[type, Callable] = {}


# Registry decorator
def task_register(ctx_type: type[CreateContext]):
    def wrapper(func):
        TASK_REGISTER[ctx_type] = func
        return func

    return wrapper


@task_register(CreateSinkContext)
def build_sink(manager, ctx: CreateSinkContext):
    task = SinkTask[ctx._out_type](ctx.name, manager.transform_conn)
    # TODO: subscribe to many upstreams
    for upstream in ctx.upstreams:
        task.subscribe(manager._sources[upstream])

    task.register(build_sink_executable(ctx, manager.backend_conn))
    logger.warning(f"Registering task={ctx.name}, upstreams={ctx.upstreams}")
    return task


@task_register(CreateHTTPTableContext)
@task_register(CreateHTTPSourceContext)
def build_http_scheduled(
    manager, ctx: CreateHTTPTableContext | CreateHTTPSourceContext
):
    task = ScheduledSourceTask[ctx._out_type](
        ctx.name,
        manager.backend_conn,
        manager._scheduled_executables,
        ctx.trigger,
    )

    manager._sources[ctx.name] = task.register(build_scheduled_source_executable(ctx))

    return task


@task_register(CreateWSTableContext)
@task_register(CreateWSSourceContext)
def build_ws(manager, ctx: CreateWSTableContext | CreateWSSourceContext):
    task = ContinuousSourceTask[ctx._out_type](
        ctx.name, manager.backend_conn, manager._nursery
    )

    # TODO: make WS Task dynamic by registering the on_start function
    manager._sources[ctx.name] = task.register(
        build_continuous_source_executable(ctx, manager.backend_conn)
    )

    return task


@task_register(CreateHTTPLookupTableContext)
def build_lookup(manager, ctx: CreateHTTPLookupTableContext):
    callback_store.add(*build_lookup_callback(ctx, manager.backend_conn))
    return None


@task_register(CreateViewContext)
def build_view(manager, ctx: CreateViewContext):
    task = TransformTask[ctx._out_type](ctx.name, manager.transform_conn)

    for upstream in ctx.upstreams:
        task.subscribe(manager._sources[upstream])

    task.register(build_transform_executable(ctx, manager.backend_conn))
    return task
