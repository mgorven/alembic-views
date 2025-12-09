# Copyright © 2025, Michael Gorven

import logging
import re

from alembic.autogenerate import comparators
from alembic.autogenerate.api import AutogenContext
from alembic.operations.ops import UpgradeOps
from sqlalchemy import text

from alembic_views.operations import CreateViewOp, DropViewOp, ReplaceViewOp

log = logging.getLogger(__name__)


def reflect_sqlite(autogen_context: AutogenContext) -> dict[str, str]:
    views = autogen_context.connection.execute(  # type: ignore[union-attr]
        text("SELECT name, sql FROM sqlite_master WHERE type='view'")
    )
    return {view[0]: normalise_sqlite(view[1]) for view in views}


def normalise_sqlite(definition: str) -> str:
    return re.sub(r"^CREATE VIEW \S+ AS ", "", definition, flags=re.I).replace("\n", "")


def reflect_postgresql(autogen_context: AutogenContext) -> dict[str, str]:
    views = autogen_context.connection.execute(  # type: ignore[union-attr]
        text("SELECT viewname, definition FROM pg_views WHERE schemaname = :schema"),
        {"schema": autogen_context.dialect.default_schema_name},  # type: ignore[union-attr]
    )
    return {view[0]: normalise_postgresql(view[1]) for view in views}


def normalise_postgresql(definition: str) -> str:
    definition = definition.strip().rstrip(";")
    definition = re.sub("\n *", " ", definition)
    return re.sub("::[a-z]+", "", definition)


REFLECT_DIALECT = {
    "sqlite": reflect_sqlite,
    "postgresql": reflect_postgresql,
}


@comparators.dispatch_for("schema")
def compare_views(
    autogen_context: AutogenContext,
    upgrade_ops: UpgradeOps,
    schemas: list[str],  # noqa: ARG001
) -> None:
    sqla_views = {
        k: str(v.compile()).replace("\n", "")
        for k, v in autogen_context.metadata.info.get("views", {}).items()  # type: ignore[union-attr]
    }

    if autogen_context.dialect.name not in REFLECT_DIALECT:  # type: ignore[union-attr]
        raise NotImplementedError(
            f"Unsupported dialect for view reflection: {autogen_context.dialect.name}"  # type: ignore[union-attr]
        )
    db_views = REFLECT_DIALECT[autogen_context.dialect.name](autogen_context)  # type: ignore[union-attr]

    for name in set(sqla_views) - set(db_views):
        log.info("Detected added view '%s'", name)
        upgrade_ops.ops.append(CreateViewOp(name, sqla_views[name]))

    for name in set(db_views) - set(sqla_views):
        log.info("Detected removed view '%s'", name)
        upgrade_ops.ops.append(DropViewOp(name, old_definition=db_views[name]))

    for name in set(sqla_views) & set(db_views):
        if sqla_views[name] != db_views[name]:
            log.info("Detected changed view '%s'", name)
            upgrade_ops.ops.append(
                ReplaceViewOp(name, sqla_views[name], old_definition=db_views[name])
            )
