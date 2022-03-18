import pytest

from goodboy_sqlalchemy.mapped import MappedInstanceProxy


class MappedClass:
    def __init__(self, obj_key):
        self.obj_key = obj_key


def test_getitem_magic_method_when_instance_specified():
    obj_key_value = object()
    val_key_value = object()

    proxy = MappedInstanceProxy(
        MappedClass(obj_key_value), ["obj_key", "val_key"], {"val_key": val_key_value}
    )

    assert proxy["val_key"] is val_key_value
    assert proxy["obj_key"] is obj_key_value

    with pytest.raises(KeyError):
        proxy["unknown_key"]


def test_getitem_magic_method_when_instance_is_none():
    val_key_value = object()

    proxy = MappedInstanceProxy(
        None, ["obj_key", "val_key"], {"val_key": val_key_value}
    )

    assert proxy["val_key"] is val_key_value
    assert proxy["obj_key"] is None

    with pytest.raises(KeyError):
        proxy["unknown_key"]


def test_get_method_when_instance_specified():
    obj_key_value = object()
    val_key_value = object()
    default_value = object()

    proxy = MappedInstanceProxy(
        MappedClass(obj_key_value), ["obj_key", "val_key"], {"val_key": val_key_value}
    )

    assert proxy.get("val_key") is val_key_value
    assert proxy.get("val_key", default_value) is val_key_value
    assert proxy.get("obj_key") is obj_key_value
    assert proxy.get("obj_key", default_value) is obj_key_value
    assert proxy.get("unknown_key", default_value) is default_value


def test_get_method_when_instance_is_none():
    val_key_value = object()
    default_value = object()

    proxy = MappedInstanceProxy(
        None, ["obj_key", "val_key"], {"val_key": val_key_value}
    )

    assert proxy.get("val_key") is val_key_value
    assert proxy.get("val_key", default_value) is val_key_value
    assert proxy.get("obj_key") is None
    assert proxy.get("obj_key", default_value) is default_value
    assert proxy.get("unknown_key", default_value) is default_value
