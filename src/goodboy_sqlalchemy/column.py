from __future__ import annotations

from typing import Callable, Optional

import goodboy as gb
import sqlalchemy as sa

from goodboy_sqlalchemy.column_schemas import ColumnSchemaBuilder, column_schema_builder


class Column(gb.Key):
    def __init__(
        self,
        name: str,
        schema: Optional[gb.Schema] = None,
        *,
        required: Optional[bool] = None,
        predicate: Optional[Callable[[dict], bool]] = None,
        unique: bool = False,
    ):
        super().__init__(name, schema, required=required, predicate=predicate)
        self.unique = unique

    def with_predicate(self, predicate: Callable[[dict], bool]) -> Column:
        return Column(
            self.name,
            self._schema,
            required=self.required,
            predicate=predicate,
            unique=self.unique,
        )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__

        return super().__eq__(other)


class ColumnBuilderError(Exception):
    pass


class ColumnBuilder:
    def __init__(self, column_schema_builder: ColumnSchemaBuilder):
        self._column_schema_builder = column_schema_builder

    def build(self, sa_column: sa.Column) -> Column:
        schema = self._column_schema_builder.build(sa_column)

        return Column(
            sa_column.name,
            schema,
            required=not sa_column.nullable,
            unique=sa_column.unique,
        )


column_builder = ColumnBuilder(column_schema_builder)
