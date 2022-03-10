from typing import Callable, Optional

from goodboy import Dict, Key, Schema
from sqlalchemy import Column as SAColumn
from sqlalchemy.orm import Query

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
        unique: bool = False,
    ):
        self._required = required
        self._name = name
        self._schema = schema
        self._predicate = predicate
        self._unique = unique
        self._mapped = None

    def attach_mapped(self, mapped: type):
        self._mapped = mapped
        self._mapped_sa_column = getattr(mapped, self._name)

    def get_dict_key(self):
        return Key(self._name, self._schema, required=self._required)


class ColumnBuilder:
    def __init__(self, schema_builder: SchemaBuilder):
        self._schema_builder = schema_builder

    def build(self, mapped_class: type, sa_column: SAColumn):
        schema = self._schema_builder.build(mapped_class, sa_column)
        return Column(
            sa_column.name,
            schema,
            required=not sa_column.nullable,
            unique=sa_column.unique,
        )


column_builder = ColumnBuilder(schema_builder)
