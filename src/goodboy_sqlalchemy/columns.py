from typing import Callable, Optional

from goodboy import Schema
from sqlalchemy import Column as SAColumn

from goodboy_sqlalchemy.schemas import SchemaBuilder
from goodboy_sqlalchemy.schemas import builder as schema_builder


class Column:
    def __init__(
        self,
        name: str,
        schema: Optional[Schema] = None,
        *,
        required: Optional[bool] = None,
        predicate: Optional[Callable[[dict], bool]] = None,
        unique: bool = False,
    ):
        self.required = required
        self.name = name
        self.unique = unique
        self._schema = schema
        self._predicate = predicate

    def predicate_result(self, prev_values: dict):
        if self._predicate:
            return self._predicate(prev_values)
        else:
            return True

    def validate(self, value, typecast: bool, context: dict):
        if self._schema:
            return self._schema(value, typecast=typecast, context=context)
        else:
            return value

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__

        return super().__eq__(other)


class ColumnBuilder:
    def __init__(self, schema_builder: SchemaBuilder):
        self._schema_builder = schema_builder

    def build(self, sa_column: SAColumn):
        schema = self._schema_builder.build(sa_column)

        return Column(
            sa_column.name,
            schema,
            required=not sa_column.nullable,
            unique=sa_column.unique,
        )


column_builder = ColumnBuilder(schema_builder)
