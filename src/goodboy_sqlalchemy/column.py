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

    def build(self, sa_mapped_class: type, column_names: list[str]) -> list[Column]:
        sa_mapper = sa.inspect(sa_mapped_class)
        result: list[Column] = []

        for column_name in column_names:
            result.append(self._build_column(sa_mapper, column_name))

        return result

    def _build_column(self, sa_mapper, column_name: str) -> Column:
        if column_name not in sa_mapper.columns:
            raise ColumnBuilderError(
                f"mapped class {sa_mapper.class_.__name__} has no column {column_name}"
            )

        sa_column = sa_mapper.columns[column_name]
        schema = self._column_schema_builder.build(sa_column)

        return Column(
            sa_column.name,
            schema,
            required=not sa_column.nullable,
            unique=sa_column.unique or False,
        )


column_builder = ColumnBuilder(column_schema_builder)
