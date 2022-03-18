from __future__ import annotations

from typing import Any, Optional

import goodboy as gb
import sqlalchemy as sa
import sqlalchemy.orm as sa_orm

from goodboy_sqlalchemy.column import ColumnBuilder, column_builder
from goodboy_sqlalchemy.mapped_key import MappedKeyBuilder, mapped_key_builder
from goodboy_sqlalchemy.messages import DEFAULT_MESSAGES


class MappedInstanceProxyKeyError(Exception):
    """
    Exception used in MappedInstanceProxy instead of standard KeyError, to avoid
    accidental catching KeyError exception from an mapped instance property.
    """

    pass


class MappedInstanceProxy:
    def __init__(self, mapped_instance, key_names: list[str], override_values: dict):
        self._mapped_instance = mapped_instance
        self._key_names = key_names
        self._override_values = override_values

    def get(self, key, default=None):
        try:
            return self._getitem(key, default)
        except MappedInstanceProxyKeyError:
            return default

    def __contains__(self, key):
        return key in self._key_names

    def __getitem__(self, key):
        try:
            return self._getitem(key, None)
        except MappedInstanceProxyKeyError:
            raise KeyError(key)

    def _getitem(self, key, default):
        if key not in self._key_names:
            raise MappedInstanceProxyKeyError()

        if key in self._override_values:
            return self._override_values[key]

        if self._mapped_instance:
            return getattr(self._mapped_instance, key, default)

        return default


class MappedError(Exception):
    pass


class Mapped(gb.Schema, gb.SchemaErrorMixin):
    def __init__(
        self,
        sa_mapped_class: type,
        keys: list[gb.Key] = [],
        column_names: list[str] = [],
        column_builder: ColumnBuilder = column_builder,
        mapped_key_builder: MappedKeyBuilder = mapped_key_builder,
        messages: gb.MessageCollectionType = DEFAULT_MESSAGES,
    ):
        super().__init__()

        self._sa_mapped_class = sa_mapped_class
        self._messages = messages

        self._keys = keys + column_builder.build(sa_mapped_class, column_names)
        self._mapped_keys = mapped_key_builder.build(
            sa_mapped_class, self._keys, messages
        )

    def __call__(self, value, *, typecast=False, context: dict = {}):
        if not isinstance(context.get("session"), sa_orm.Session):
            raise MappedError(
                "session instance is required in Mapped validation context"
            )

        if not isinstance(value, dict):
            error = self._error(
                "unexpected_type", {"expected_type": gb.type_name("dict")}
            )

            raise gb.SchemaError([error])

        session: sa_orm.Session = context["session"]
        instance: Any = context.get("mapped_instance")

        value, errors = self._validate(value, typecast, context, session, instance)

        if errors:
            raise gb.SchemaError(errors)

        return value

    def _validate(
        self,
        value: dict,
        typecast: bool,
        context: dict,
        session: sa_orm.Session,
        instance: Optional[Any] = None,
    ):
        result: dict = {}

        key_errors = {}
        value_errors = {}

        unknown_keys = list(value.keys())

        instance = context.get("mapped_instance")

        for mapped_key in self._mapped_keys:
            if not mapped_key.predicate_result(result):
                continue

            if mapped_key.name in unknown_keys:
                unknown_keys.remove(mapped_key.name)

                try:
                    key_value = mapped_key.validate(
                        value[mapped_key.name], typecast, context, session, instance
                    )
                except gb.SchemaError as e:
                    value_errors[mapped_key.name] = e.errors
                else:
                    result[mapped_key.name] = key_value
            else:
                if mapped_key.required:
                    key_errors[mapped_key.name] = [self._error("required_key")]

        errors: list[gb.Error] = []

        for key_name in unknown_keys:
            key_errors[key_name] = [self._error("unknown_key")]

        if key_errors:
            errors.append(self._error("key_errors", nested_errors=key_errors))

        if value_errors:
            errors.append(self._error("value_errors", nested_errors=value_errors))

        return result, errors
