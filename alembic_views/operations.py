# Copyright © 2025, Michael Gorven

from typing import Any

from alembic.autogenerate import renderers
from alembic.autogenerate.api import AutogenContext
from alembic.operations import MigrateOperation, Operations


@Operations.register_operation("create_view")
class CreateViewOp(MigrateOperation):
    def __init__(self, name: str, definition: str, schema: str | None) -> None:
        self.name = name
        self.definition = definition
        self.schema = schema

    @classmethod
    def create_view(
        cls,
        operations: Operations,
        name: str,
        definition: str,
        schema: str | None = None,
    ) -> Any:
        op = CreateViewOp(name, definition, schema)
        return operations.invoke(op)

    def reverse(self) -> "DropViewOp":
        return DropViewOp(self.name, self.schema)

    def to_diff_tuple(self) -> tuple[str, str | None, str]:
        return ("create_view", self.schema, self.name)


@Operations.implementation_for(CreateViewOp)
def create_view(operations: Operations, operation: CreateViewOp) -> None:
    quote = operations.get_bind().engine.dialect.identifier_preparer.quote
    name = (
        f"{quote(operation.schema)}.{quote(operation.name)}"
        if operation.schema
        else quote(operation.name)
    )
    operations.execute(f"CREATE VIEW {name} AS {operation.definition}")


@renderers.dispatch_for(CreateViewOp)
def render_create_view(
    _autogen_context: AutogenContext, operation: CreateViewOp
) -> str:
    return f"op.create_view({operation.name!r}, '''{operation.definition}''', schema={operation.schema!r})"


@Operations.register_operation("replace_view")
class ReplaceViewOp(MigrateOperation):
    def __init__(
        self,
        name: str,
        definition: str,
        schema: str | None,
        drop: bool = False,
        old_definition: str | None = None,
    ) -> None:
        self.name = name
        self.definition = definition
        self.schema = schema
        self.drop = drop
        self.old_definition = old_definition

    @classmethod
    def replace_view(
        cls,
        operations: Operations,
        name: str,
        definition: str,
        schema: str | None = None,
        drop: bool = False,
    ) -> Any:
        op = ReplaceViewOp(name, definition, schema, drop=drop)
        return operations.invoke(op)

    def reverse(self) -> "ReplaceViewOp":
        assert self.old_definition
        return ReplaceViewOp(
            self.name,
            self.old_definition,
            self.schema,
            old_definition=self.definition,
        )

    def to_diff_tuple(self) -> tuple[str, str | None, str]:
        return ("replace_view", self.schema, self.name)


@Operations.implementation_for(ReplaceViewOp)
def replace_view(operations: Operations, operation: ReplaceViewOp) -> None:
    quote = operations.get_bind().engine.dialect.identifier_preparer.quote
    name = (
        f"{quote(operation.schema)}.{quote(operation.name)}"
        if operation.schema
        else quote(operation.name)
    )

    if operation.drop or operations.get_bind().engine.name == "sqlite":
        operations.execute(f"DROP VIEW {name}")
        operations.execute(f"CREATE VIEW {name} AS {operation.definition}")
    else:
        operations.execute(f"CREATE OR REPLACE VIEW {name} AS {operation.definition}")


@renderers.dispatch_for(ReplaceViewOp)
def render_replace_view(
    _autogen_context: AutogenContext, operation: ReplaceViewOp
) -> str:
    return f"op.replace_view({operation.name!r}, '''{operation.definition}''', schema={operation.schema!r})"


@Operations.register_operation("drop_view")
class DropViewOp(MigrateOperation):
    def __init__(
        self, name: str, schema: str | None, old_definition: str | None = None
    ) -> None:
        self.schema = schema
        self.name = name
        self.schema = schema
        self.old_definition = old_definition

    @classmethod
    def drop_view(
        cls, operations: Operations, name: str, schema: str | None = None
    ) -> Any:
        op = DropViewOp(name, schema)
        return operations.invoke(op)

    def reverse(self) -> CreateViewOp:
        assert self.old_definition
        return CreateViewOp(self.name, self.old_definition, self.schema)

    def to_diff_tuple(self) -> tuple[str, str | None, str]:
        return ("drop_view", self.schema, self.name)


@Operations.implementation_for(DropViewOp)
def drop_view(operations: Operations, operation: DropViewOp) -> None:
    quote = operations.get_bind().engine.dialect.identifier_preparer.quote
    name = (
        f"{quote(operation.schema)}.{quote(operation.name)}"
        if operation.schema
        else quote(operation.name)
    )
    operations.execute(f"DROP VIEW {name}")


@renderers.dispatch_for(DropViewOp)
def render_drop_view(_autogen_context: AutogenContext, operation: DropViewOp) -> str:
    return f"op.drop_view({operation.name!r}, schema={operation.schema!r})"
