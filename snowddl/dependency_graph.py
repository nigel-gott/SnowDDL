import re

from collections import defaultdict
from typing import Dict, Optional, Set, TYPE_CHECKING

from snowddl.blueprint import AbstractBlueprint, DependsOnMixin

if TYPE_CHECKING:
    from snowddl.config import SnowDDLConfig


# Matches three-part dotted identifiers: DB.SCHEMA.OBJECT (quoted or unquoted parts)
_IDENT3_RE = re.compile(
    r'"?([A-Za-z0-9_$]+)"?\."?([A-Za-z0-9_$]+)"?\."?([A-Za-z0-9_$]+)"?',
    re.IGNORECASE,
)


def _graph_eligible_classes():
    from snowddl.blueprint import (
        DynamicTableBlueprint,
        FunctionBlueprint,
        MaterializedViewBlueprint,
        ProcedureBlueprint,
        ViewBlueprint,
    )
    return (
        DynamicTableBlueprint,
        FunctionBlueprint,
        MaterializedViewBlueprint,
        ProcedureBlueprint,
        ViewBlueprint,
    )


class CyclicDependencyError(Exception):
    pass


class DependencyGraphBuilder:
    """
    Scans SQL text / body of graph-eligible blueprints for references to other
    managed graph-eligible objects and builds cross-type depends_on sets.
    """

    def __init__(self, config: "SnowDDLConfig"):
        self.config = config

    def collect_graph_blueprints(self) -> Dict[str, AbstractBlueprint]:
        result: Dict[str, AbstractBlueprint] = {}
        for cls in _graph_eligible_classes():
            result.update(self.config.get_blueprints_by_type(cls))
        return result

    def build(self) -> None:
        """
        Populate depends_on on every graph-eligible blueprint by scanning SQL text/body.
        Raises CyclicDependencyError on cycles.
        """
        all_bps = self.collect_graph_blueprints()
        lookup = self._build_lookup(all_bps)

        for name_str, bp in all_bps.items():
            sql = self._get_sql(bp)
            if sql is None:
                continue
            bp.depends_on = self._scan_refs(sql, name_str, lookup, all_bps)

        self._assert_no_cycles(all_bps)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_lookup(self, blueprints: Dict[str, AbstractBlueprint]) -> Dict[str, str]:
        """
        Map uppercase identifier strings → canonical full_name strings.
        For functions/procedures (SchemaObjectIdentWithArgs), also maps the base
        name without argument types so call-site references like DB.SCHEMA.F are found.
        """
        lookup: Dict[str, str] = {}
        for name_str in blueprints:
            upper = name_str.upper()
            lookup[upper] = name_str
            if "(" in upper:
                base = upper[: upper.index("(")]
                if base not in lookup:
                    lookup[base] = name_str
        return lookup

    def _get_sql(self, bp: AbstractBlueprint) -> Optional[str]:
        if hasattr(bp, "text"):
            return bp.text
        body = getattr(bp, "body", None)
        return body if body else None

    def _scan_refs(
        self,
        sql: str,
        self_name: str,
        lookup: Dict[str, str],
        all_bps: Dict[str, AbstractBlueprint],
    ) -> Set:
        deps: Set = set()
        env_prefix_upper = self.config.env_prefix.upper()

        for m in _IDENT3_RE.finditer(sql):
            # Only follow references that start with the env_prefix (managed objects).
            # When env_prefix is empty every 3-part ref is a candidate.
            if env_prefix_upper and not m.group(1).upper().startswith(env_prefix_upper):
                continue

            ident = f"{m.group(1)}.{m.group(2)}.{m.group(3)}".upper()
            canonical = lookup.get(ident)
            if canonical is not None and canonical != self_name:
                deps.add(all_bps[canonical].full_name)

        return deps

    def _assert_no_cycles(self, blueprints: Dict[str, AbstractBlueprint]) -> None:
        """Kahn's algorithm; raises CyclicDependencyError if a cycle exists."""
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

        queue = [n for n, d in in_degree.items() if d == 0]
        processed = 0

        while queue:
            node = queue.pop()
            processed += 1
            for dependent in edges[node]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        if processed < len(blueprints):
            cycle_nodes = sorted(n for n, d in in_degree.items() if d > 0)
            raise CyclicDependencyError(
                f"Circular dependency detected among graph-eligible objects: "
                + ", ".join(cycle_nodes)
            )
