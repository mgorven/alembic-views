# Copyright © 2025, Michael Gorven

import logging
import re

from alembic.autogenerate import comparators
from alembic.autogenerate.api import AutogenContext
from alembic.operations.ops import UpgradeOps
from sqlalchemy import text

from alembic_views.operations import CreateViewOp, DropViewOp, ReplaceViewOp

log = logging.getLogger(__name__)


def reflect_sqlite(
    autogen_context: AutogenContext, schemas: list[str]
) -> dict[tuple[str | None, str], str]:
    views: dict[tuple[str | None, str], str] = {}
    for schema in schemas:
        prefix = schema + "." if schema else ""
        rows = autogen_context.connection.execute(  # type: ignore[union-attr]
            text(f"SELECT name, sql FROM {prefix}sqlite_master WHERE type='view'")
        )
        views.update({(schema, row[0]): normalise_sqlite(row[1]) for row in rows})
    return views


def normalise_sqlite(definition: str) -> str:
    return re.sub(r"^CREATE VIEW \S+ AS ", "", definition, flags=re.I).replace("\n", "")


def reflect_postgresql(
    autogen_context: AutogenContext, schemas: list[str]
) -> dict[tuple[str | None, str], str]:
    rows = autogen_context.connection.execute(  # type: ignore[union-attr]
        text(
            "SELECT schemaname, viewname, definition FROM pg_views WHERE schemaname = ANY(:schemas)"
        ),
        {
            "schemas": [
                autogen_context.dialect.default_schema_name  # type: ignore[union-attr]
                if schema is None
                else schema
                for schema in schemas
            ]
        },
    )
    return {
        (
            None if row[0] == autogen_context.dialect.default_schema_name else row[0],  # type: ignore[union-attr]
            row[1],
        ): normalise_postgresql(row[2])
        for row in rows
    }


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
    schemas: list[str],
) -> None:
    sqla_views = {
        k: str(v.compile()).replace("\n", "")
        for k, v in autogen_context.metadata.info.get("views", {}).items()  # type: ignore[union-attr]
    }

    if autogen_context.dialect.name not in REFLECT_DIALECT:  # type: ignore[union-attr]
        raise NotImplementedError(
            f"Unsupported dialect for view reflection: {autogen_context.dialect.name}"  # type: ignore[union-attr]
        )
    db_views = REFLECT_DIALECT[autogen_context.dialect.name](autogen_context, schemas)  # type: ignore[union-attr]

    for schema, name in set(sqla_views) - set(db_views):
        if autogen_context.run_name_filters(name, "view", {"schema_name": schema}):  # type: ignore[arg-type]
            log.info("Detected added view '%s'", name)
            upgrade_ops.ops.append(
                CreateViewOp(name, sqla_views[(schema, name)], schema)
            )

    for schema, name in set(db_views) - set(sqla_views):
        if autogen_context.run_name_filters(name, "view", {"schema_name": schema}):  # type: ignore[arg-type]
            log.info("Detected removed view '%s'", name)
            upgrade_ops.ops.append(
                DropViewOp(name, schema, old_definition=db_views[(schema, name)])
            )

    for schema, name in set(sqla_views) & set(db_views):
        if autogen_context.run_name_filters(name, "view", {"schema_name": schema}):  # type: ignore[arg-type]
            if sqla_views[(schema, name)] != db_views[(schema, name)]:
                log.info("Detected changed view '%s'", name)
                log.debug("SQLAlchemy definition: |%s|", sqla_views[(schema, name)])
                log.debug("Database definition: |%s|", db_views[(schema, name)])
                upgrade_ops.ops.append(
                    ReplaceViewOp(
                        name,
                        sqla_views[(schema, name)],
                        schema,
                        old_definition=db_views[(schema, name)],
                    )
                )
