# Copyright © 2025, Michael Gorven

from typing import Any

from alembic.autogenerate import renderers
from alembic.autogenerate.api import AutogenContext
from alembic.operations import MigrateOperation, Operations


@Operations.register_operation("create_view")
class CreateViewOp(MigrateOperation):
    def __init__(self, name: str, definition: str) -> None:
        self.name = name
        self.definition = definition

    @classmethod
    def create_view(cls, operations: Operations, name: str, definition: str) -> Any:
        op = CreateViewOp(name, definition)
        return operations.invoke(op)

    def reverse(self) -> "DropViewOp":
        return DropViewOp(self.name, self.definition)

    def to_diff_tuple(self) -> tuple[str, str]:
        return ("create_view", self.name)


@Operations.implementation_for(CreateViewOp)
def create_view(operations: Operations, operation: CreateViewOp) -> None:
    quote = operations.get_bind().engine.dialect.identifier_preparer.quote
    operations.execute(f"CREATE VIEW {quote(operation.name)} AS {operation.definition}")


@renderers.dispatch_for(CreateViewOp)
def render_create_view(
    _autogen_context: AutogenContext, operation: CreateViewOp
) -> str:
    return f"op.create_view('{operation.name}', '''{operation.definition}''')"


@Operations.register_operation("replace_view")
class ReplaceViewOp(MigrateOperation):
    def __init__(
        self,
        name: str,
        definition: str,
        drop: bool = False,
        old_definition: str | None = None,
    ) -> None:
        self.name = name
        self.definition = definition
        self.drop = drop
        self.old_definition = old_definition

    @classmethod
    def replace_view(
        cls, operations: Operations, name: str, definition: str, drop: bool = False
    ) -> Any:
        op = ReplaceViewOp(name, definition, drop=drop)
        return operations.invoke(op)

    def reverse(self) -> "ReplaceViewOp":
        assert self.old_definition
        return ReplaceViewOp(
            self.name,
            self.old_definition,
            old_definition=self.definition,
        )

    def to_diff_tuple(self) -> tuple[str, str]:
        return ("replace_view", self.name)


@Operations.implementation_for(ReplaceViewOp)
def replace_view(operations: Operations, operation: ReplaceViewOp) -> None:
    quote = operations.get_bind().engine.dialect.identifier_preparer.quote
    if operation.drop or operations.get_bind().engine.name == "sqlite":
        operations.execute(f"DROP VIEW {quote(operation.name)}")
        operations.execute(
            f"CREATE VIEW {quote(operation.name)} AS {operation.definition}"
        )
    else:
        operations.execute(
            f"CREATE OR REPLACE VIEW {quote(operation.name)} AS {operation.definition}"
        )


@renderers.dispatch_for(ReplaceViewOp)
def render_replace_view(
    _autogen_context: AutogenContext, operation: ReplaceViewOp
) -> str:
    return f"op.replace_view('{operation.name}', '''{operation.definition}''')"


@Operations.register_operation("drop_view")
class DropViewOp(MigrateOperation):
    def __init__(self, name: str, old_definition: str | None = None) -> None:
        self.name = name
        self.old_definition = old_definition

    @classmethod
    def drop_view(cls, operations: Operations, name: str) -> Any:
        op = DropViewOp(name)
        return operations.invoke(op)

    def reverse(self) -> CreateViewOp:
        assert self.old_definition
        return CreateViewOp(self.name, self.old_definition)

    def to_diff_tuple(self) -> tuple[str, str]:
        return ("drop_view", self.name)


@Operations.implementation_for(DropViewOp)
def drop_view(operations: Operations, operation: DropViewOp) -> None:
    quote = operations.get_bind().engine.dialect.identifier_preparer.quote
    operations.execute(f"DROP VIEW {quote(operation.name)}")


@renderers.dispatch_for(DropViewOp)
def render_drop_view(_autogen_context: AutogenContext, operation: DropViewOp) -> str:
    return f'op.drop_view("{operation.name}")'
