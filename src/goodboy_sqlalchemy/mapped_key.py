from __future__ import annotations

from abc import ABC, abstractmethod, abstractproperty
from typing import Any, Mapping, Optional

import goodboy as gb
import sqlalchemy as sa
import sqlalchemy.orm as sa_orm

from goodboy_sqlalchemy.column import Column
from goodboy_sqlalchemy.messages import DEFAULT_MESSAGES


class MappedKey(ABC):
    @abstractproperty
    def name(self):
        ...

    @abstractproperty
    def result_key_name(self):
        ...

    @abstractproperty
    def required(self):
        ...

    @abstractproperty
    def has_default(self) -> bool:
        ...

    @abstractproperty
    def default(self) -> Any:
        ...

    @abstractmethod
    def predicate_result(self, prev_values: Mapping[str, Any]) -> bool:
        ...

    @abstractmethod
    def validate(
        self,
        value,
        typecast: bool,
        context: dict,
        session: sa_orm.Session,
        instance: Optional[Any] = None,
    ):
        ...


class MappedColumnKey(MappedKey):
    def __init__(
        self,
        sa_mapped_class: type,
        sa_column: sa.Column,
        sa_pk_column: sa.Column,
        column: Column,
        messages: gb.MessageCollectionType = DEFAULT_MESSAGES,
    ):
        self._sa_mapped_class = sa_mapped_class
        self._sa_column = sa_column
        self._sa_pk_column = sa_pk_column
        self._column = column
        self._messages = messages

    @property
    def name(self):
        return self._column.name

    @property
    def result_key_name(self):
        return self._column.mapped_column_name

    @property
    def required(self):
        return self._column.required

    @property
    def has_default(self) -> bool:
        return self._column.has_default

    @property
    def default(self) -> Any:
        return self._column.default

    def predicate_result(self, prev_values: Mapping[str, Any]) -> bool:
        return self._column.predicate_result(prev_values)

    def validate(
        self,
        value,
        typecast: bool,
        context: dict,
        session: sa_orm.Session,
        instance: Optional[Any] = None,
    ):
        value = self._column.validate(value, typecast, context)

        if self._column.unique:
            query = sa_orm.Query(self._sa_mapped_class).filter(self._sa_column == value)

            if instance:
                instance_pk = getattr(instance, self._sa_pk_column.key)
                query = query.filter(self._sa_pk_column != instance_pk)

            exists = session.query(query.exists()).scalar()

            if exists:
                raise gb.SchemaError([self._error("already_exists")])

        return value

    def _error(self, code: str, args: dict = {}, nested_errors: dict = {}):
        return gb.Error(code, args, nested_errors, self._messages.get_message(code))

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__

        return super().__eq__(other)


class MappedPropertyKey(MappedKey):
    def __init__(self, key: gb.Key):
        self._key = key

    @property
    def name(self):
        return self._key.name

    @property
    def result_key_name(self):
        return self._key.name

    @property
    def required(self):
        return self._key.required

    def predicate_result(self, prev_values: Mapping[str, Any]) -> bool:
        return self._key.predicate_result(prev_values)

    @property
    def has_default(self) -> bool:
        return self._key.default is not None

    @property
    def default(self) -> Any:
        return self._key.default

    def validate(
        self,
        value,
        typecast: bool,
        context: dict,
        session: sa_orm.Session,
        instance: Optional[Any] = None,
    ):
        return self._key.validate(value, typecast, context)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__

        return super().__eq__(other)


class MappedKeyBuilderError(Exception):
    pass


class MappedKeyBuilder:
    def build(
        self,
        sa_mapped_class: type,
        keys: list[gb.Key],
        messages: gb.MessageCollectionType = DEFAULT_MESSAGES,
    ) -> list[MappedKey]:
        result: list[MappedKey] = []

        for key in keys:
            result.append(self._build_mapped_key(sa_mapped_class, key, messages))

        return result

    def _build_mapped_key(
        self, sa_mapped_class: type, key: gb.Key, messages: gb.MessageCollectionType
    ) -> MappedKey:
        if isinstance(key, Column):
            return MappedColumnKey(
                sa_mapped_class,
                self._get_sa_column(sa_mapped_class, key.mapped_column_name),
                self._get_pk_sa_column(sa_mapped_class),
                key,
                messages,
            )
        else:
            return MappedPropertyKey(key)

    def _get_sa_column(self, sa_mapped_class: type, column_name: str) -> sa.Column:
        sa_mapper = sa.inspect(sa_mapped_class)

        if column_name not in sa_mapper.columns:
            raise MappedKeyBuilderError(
                f"mapped class {sa_mapped_class.__name__} has no column {column_name}"
            )

        return sa_mapper.columns[column_name]

    def _get_pk_sa_column(self, sa_mapped_class: type) -> sa.Column:
        sa_mapper = sa.inspect(sa_mapped_class)

        if len(sa_mapper.primary_key) > 1:
            raise MappedKeyBuilderError(
                "mapped classes with composite primary keys are not supported"
            )

        return sa_mapper.primary_key[0]


mapped_key_builder = MappedKeyBuilder()
