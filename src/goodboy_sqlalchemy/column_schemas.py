from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional, Type

import goodboy as gb
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sa_pg


class ColumnSchemaFactory(ABC):
    @abstractmethod
    def build(self, sa_column: sa.Column) -> gb.Schema:
        ...


class SimpleColumnSchemaFactory(ColumnSchemaFactory):
    def __init__(self, schema: Type[gb.Schema]) -> None:
        self._schema = schema

    def build(self, sa_column: sa.Column) -> gb.Schema:
        return self._schema(allow_none=sa_column.nullable)


class StringColumnSchemaFactory(ColumnSchemaFactory):
    def build(self, sa_column: sa.Column[sa.String]) -> gb.Str:
        return gb.Str(allow_none=sa_column.nullable, max_length=sa_column.type.length)


SA_TYPE_MAPPING: dict[Any, ColumnSchemaFactory] = {
    sa.BigInteger: SimpleColumnSchemaFactory(gb.Int),  # TODO: max int
    sa.Boolean: SimpleColumnSchemaFactory(gb.Bool),
    sa.Date: SimpleColumnSchemaFactory(gb.Date),
    sa.DateTime: SimpleColumnSchemaFactory(gb.DateTime),
    # sa.Enum: TODO
    sa.Float: SimpleColumnSchemaFactory(gb.Float),
    sa.Integer: SimpleColumnSchemaFactory(gb.Int),  # TODO: max int
    # sa.Interval: TODO
    # sa.LargeBinary: TODO
    # sa.Numeric: TODO
    # sa.Numeric: TODO
    sa.SmallInteger: SimpleColumnSchemaFactory(gb.Int),  # TODO: max int
    sa.String: StringColumnSchemaFactory(),
    sa.Text: SimpleColumnSchemaFactory(gb.Str),
    # sa.Time: TODO
    sa.Unicode: SimpleColumnSchemaFactory(gb.Str),
    sa.UnicodeText: SimpleColumnSchemaFactory(gb.Str),
    sa_pg.JSON: SimpleColumnSchemaFactory(gb.Dict),
    sa_pg.JSONB: SimpleColumnSchemaFactory(gb.Dict),
}


class ColumnSchemaBuilderError(Exception):
    pass


class ColumnSchemaBuilder:
    def __init__(self, sa_type_mapping: dict[Any, ColumnSchemaFactory]):
        self._sa_type_mapping = sa_type_mapping

    def build(self, sa_column: sa.Column) -> gb.Schema:
        column_schema_factory = self._find_column_schema_factory(sa_column)

        if not column_schema_factory:
            raise ColumnSchemaBuilderError(
                f"unmapped SQLAlchemy column type {repr(sa_column.type)}"
            )

        return column_schema_factory.build(sa_column)

    def _find_column_schema_factory(
        self, sa_column: sa.Column
    ) -> Optional[ColumnSchemaFactory]:
        for sa_type, column_schema_factory in self._sa_type_mapping.items():
            if isinstance(sa_column.type, sa_type):
                return column_schema_factory

        return None


column_schema_builder = ColumnSchemaBuilder(SA_TYPE_MAPPING)
