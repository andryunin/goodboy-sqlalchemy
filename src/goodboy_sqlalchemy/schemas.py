from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Type

import goodboy as gb
import sqlalchemy as sa


class ColumnSchemaFactory(ABC):
    @abstractmethod
    def build(self, column: sa.Column) -> gb.Schema:
        ...


class SimpleColumnSchemaFactory(ColumnSchemaFactory):
    def __init__(self, schema: Type[gb.Schema]):
        self._schema = schema

    def build(self, column: sa.Column) -> gb.Schema:
        return self._schema(allow_none=column.nullable)


class StringColumnSchemaFactory(ColumnSchemaFactory):
    def build(self, column: sa.Column[sa.String]) -> gb.Str:
        return gb.Str(allow_none=column.nullable, max_length=column.type.length)


TYPE_TO_SCHEMA_FACTORY_MAPPING: dict[Any, ColumnSchemaFactory] = {
    sa.Integer: SimpleColumnSchemaFactory(gb.Int),
    sa.String: StringColumnSchemaFactory(),
}


class SchemaBuilderError(Exception):
    pass


class SchemaBuilder:
    def __init__(self, mapping: dict[Any, ColumnSchemaFactory]):
        self._mapping = mapping

    def build(self, column: sa.Column) -> gb.Schema:
        schema_factory = self._find_factory(column)
        return schema_factory.build(column)

    def _find_factory(self, column: sa.Column) -> ColumnSchemaFactory:
        for sa_type, gb_schema_factory in self._mapping.items():
            if isinstance(column.type, sa_type):
                return gb_schema_factory

        raise SchemaBuilderError(
            f"unmapped SQLAlchemy column type {repr(column.type)}"
        )


builder = SchemaBuilder(TYPE_TO_SCHEMA_FACTORY_MAPPING)
