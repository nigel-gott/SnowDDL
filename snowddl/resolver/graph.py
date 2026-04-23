from collections import defaultdict
from concurrent.futures import as_completed
from traceback import format_exc
from typing import Dict, List, TYPE_CHECKING

from snowddl.blueprint import AbstractBlueprint, DependsOnMixin
from snowddl.error import SnowDDLExecuteError, SnowDDLUnsupportedError
from snowddl.resolver.abc_resolver import ResolveResult

if TYPE_CHECKING:
    from snowddl.engine import SnowDDLEngine
    from snowddl.resolver.abc_resolver import AbstractResolver


def _topological_batches(blueprints: Dict[str, AbstractBlueprint]) -> List[List[str]]:
    """
    Kahn's algorithm: returns ordered batches of blueprint full_name strings.
    Objects in the same batch have no intra-batch dependencies.
    """
    in_degree: Dict[str, int] = defaultdict(int)
    edges: Dict[str, list] = defaultdict(list)

    for name_str, bp in blueprints.items():
        if name_str not in in_degree:
            in_degree[name_str] = 0
        if isinstance(bp, DependsOnMixin):
            for dep_ident in bp.depends_on:
                dep_str = str(dep_ident)
                if dep_str in blueprints:
                    edges[dep_str].append(name_str)
                    in_degree[name_str] += 1

    batches: List[List[str]] = []
    current = [n for n, d in in_degree.items() if d == 0]

    while current:
        batches.append(current)
        nxt: List[str] = []
        for name in current:
            for dependent in edges[name]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    nxt.append(dependent)
        current = nxt

    return batches


def _get_graph_resolver_classes():
    from snowddl.resolver.dynamic_table import DynamicTableResolver
    from snowddl.resolver.function import FunctionResolver
    from snowddl.resolver.materialized_view import MaterializedViewResolver
    from snowddl.resolver.procedure import ProcedureResolver
    from snowddl.resolver.view import ViewResolver

    return [FunctionResolver, ProcedureResolver, DynamicTableResolver, MaterializedViewResolver, ViewResolver]


class CrossTypeGraphResolver:
    """
    Resolves Function, Procedure, DynamicTable, MaterializedView and View objects
    together in cross-type topological dependency order derived from depends_on sets
    that were populated by DependencyGraphBuilder.
    """

    def __init__(self, engine: "SnowDDLEngine"):
        self.engine = engine
        self.errors: Dict[str, Exception] = {}

    def resolve(self):
        all_blueprints: Dict[str, AbstractBlueprint] = {}
        resolver_for: Dict[str, "AbstractResolver"] = {}
        active_resolvers: List["AbstractResolver"] = []

        for cls in _get_graph_resolver_classes():
            resolver = cls(self.engine)

            if resolver._is_skipped():
                continue

            resolver._pre_process()
            resolver.blueprints = resolver.get_blueprints()

            try:
                resolver.existing_objects = resolver.get_existing_objects()
            except SnowDDLExecuteError as e:
                self.engine.logger.info(
                    f"Could not get existing objects for resolver [{cls.__name__}]: \n{e.verbose_message()}"
                )
                raise e.snow_exc

            for name, bp in resolver.blueprints.items():
                all_blueprints[name] = bp
                resolver_for[name] = resolver

            active_resolvers.append(resolver)

        batches = _topological_batches(all_blueprints)

        for batch in batches:
            self._process_batch(batch, resolver_for)

        for resolver in active_resolvers:
            resolver._resolve_drop()
            resolver._post_process()
            self.errors.update(resolver.errors)

    def _process_batch(self, batch: List[str], resolver_for: Dict[str, "AbstractResolver"]):
        tasks: Dict[str, tuple] = {}

        for full_name in sorted(batch):
            resolver = resolver_for.get(full_name)
            if resolver is None:
                continue
            bp = resolver.blueprints[full_name]
            if full_name in resolver.existing_objects:
                tasks[full_name] = (resolver, resolver.compare_object, bp, resolver.existing_objects[full_name])
            else:
                tasks[full_name] = (resolver, resolver.create_object, bp)

        futures = {}
        for full_name, (resolver, method, *args) in tasks.items():
            futures[self.engine.executor.submit(method, *args)] = (resolver, full_name)

        for f in as_completed(futures):
            resolver, full_name = futures[f]

            try:
                result = f.result()

                if result in (ResolveResult.REPLACE, ResolveResult.DROP):
                    self.engine.intention_cache.add_object_drop_intention(resolver.object_type, full_name)

                if result == ResolveResult.NOCHANGE:
                    self.engine.logger.debug(f"Resolved {resolver.object_type.name} [{full_name}]: {result.value}")
                else:
                    self.engine.logger.info(f"Resolved {resolver.object_type.name} [{full_name}]: {result.value}")

            except Exception as e:
                if isinstance(e, SnowDDLUnsupportedError):
                    result = ResolveResult.UNSUPPORTED
                else:
                    result = ResolveResult.ERROR

                if isinstance(e, SnowDDLExecuteError):
                    error_text = e.verbose_message()
                else:
                    error_text = format_exc()

                self.engine.logger.warning(
                    f"Resolved {resolver.object_type.name} [{full_name}]: {result.value}\n{error_text}"
                )
                resolver.errors[full_name] = e

            resolver.resolved_objects[full_name] = result

        self.engine.flush_thread_buffers()
