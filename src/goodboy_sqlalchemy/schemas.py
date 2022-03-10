from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Type

import goodboy as gb
import sqlalchemy as sa
from goodboy.schema import Rule


class ColumnSchemaFactory(ABC):
    @abstractmethod
    def build(self, column: sa.Column, rules: list[Rule]) -> gb.Schema:
        ...


class SimpleColumnSchemaFactory(ColumnSchemaFactory):
    def __init__(self, schema: Type[gb.Schema]):
        self._schema = schema

    def build(self, column: sa.Column, rules: list[Rule]) -> gb.Schema:
        return self._schema(allow_none=column.nullable, rules=rules)


class StringColumnSchemaFactory(ColumnSchemaFactory):
    def build(self, column: sa.Column[sa.String], rules: list[Rule]) -> gb.Str:
        return gb.Str(
            allow_none=column.nullable, max_length=column.type.length, rules=rules
        )


TYPE_TO_SCHEMA_FACTORY_MAPPING: dict[Any, ColumnSchemaFactory] = {
    sa.Integer: SimpleColumnSchemaFactory(gb.Int),
    sa.Date: SimpleColumnSchemaFactory(gb.Date),
    sa.String: StringColumnSchemaFactory(),
}


class SchemaBuilderError(Exception):
    pass


class SchemaBuilder:
    def __init__(self, mapping: dict[Any, ColumnSchemaFactory]):
        self._mapping = mapping

    def build(self, mapped_class: type, sa_column: sa.Column) -> gb.Schema:
        schema_factory = self._find_factory(sa_column)
        rules = []

        if sa_column.unique:
            rules.append(self._get_unique_rule(mapped_class, sa_column))

        return schema_factory.build(sa_column, rules)

    def _find_factory(self, column: sa.Column) -> ColumnSchemaFactory:
        for sa_type, gb_schema_factory in self._mapping.items():
            if isinstance(column.type, sa_type):
                return gb_schema_factory

        raise SchemaBuilderError(f"unmapped SQLAlchemy column type {repr(column.type)}")

    def _get_unique_rule(self, mapped_class: type, sa_column: sa.Column):
        def unique_rule(self: gb.Dict, value, typecast: bool, context: dict):
            query = context["session"].query(mapped_class)
            query = query.filter(sa_column == value)

            if context["mapped_primary_key"]:
                query = query.filter(
                    context["mapped_pk_sa_column"] != context["mapped_primary_key"]
                )

            exists = context["session"].query(query.exists()).scalar()

            if exists:
                return value, [self._error("already_exists")]
            else:
                return value, []

        return unique_rule


builder = SchemaBuilder(TYPE_TO_SCHEMA_FACTORY_MAPPING)
