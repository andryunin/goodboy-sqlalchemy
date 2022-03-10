from typing import Callable, Optional

from goodboy import Key, Schema
from sqlalchemy import Column as SAColumn

from goodboy_sqlalchemy.schemas import SchemaBuilder
from goodboy_sqlalchemy.schemas import builder as schema_builder


class Column:
    def __init__(
        self,
        name: str,
        schema: Schema = None,
        *,
        required: bool = True,
        predicate: Optional[Callable[[dict], bool]] = None,
    ):
        self._required = required
        self._name = name
        self._schema = schema
        self._predicate = predicate

    def get_dict_key(self):
        return Key(self._name, self._schema, required=self._required)


class ColumnBuilder:
    def __init__(self, schema_builder: SchemaBuilder):
        self._schema_builder = schema_builder

    def build(self, sa_column: SAColumn):
        schema = self._schema_builder.build(sa_column)
        return Column(sa_column.name, schema, required=sa_column.nullable)


column_builder = ColumnBuilder(schema_builder)
