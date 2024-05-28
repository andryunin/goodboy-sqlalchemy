import pytest
import sqlalchemy as sa
from goodboy import Error, Str

from goodboy_sqlalchemy.column import Column
from goodboy_sqlalchemy.mapped_key import MappedColumnKey
from tests.conftest import assert_errors

# Use in-memory SQLite
engine = sa.create_engine("sqlite://")
Session = sa.orm.sessionmaker(engine)
Base = sa.orm.declarative_base()


class Dummy(Base):
    __tablename__ = "dummies"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String, nullable=False, unique=True)


Base.metadata.create_all(engine)


@pytest.fixture()
def session():
    try:
        session = Session()
        yield session
    finally:
        session.rollback()


def test_inherits_column_properties():
    name = "my_name"
    mapped_column_name = "my_name_mapped"
    required = False
    predicate_res = False
    default = "hello"

    column = Column(
        name,
        Str(),
        required=required,
        predicate=lambda _: predicate_res,
        mapped_column_name=mapped_column_name,
        default="hello",
    )

    mapped_column = MappedColumnKey(Dummy, Dummy.name, Dummy.id, "id", column)

    assert mapped_column.name is name
    assert mapped_column.result_key_name is mapped_column_name
    assert mapped_column.required is required
    assert mapped_column.default == default
    assert mapped_column.has_default
    assert mapped_column.predicate_result({}) is predicate_res


def test_accepts_unique_value(session):
    column = Column("name", Str(), required=True, unique=True)
    mapped_column = MappedColumnKey(Dummy, Dummy.name, Dummy.id, "id", column)

    assert mapped_column.validate("ok", False, {}, session) == "ok"


def test_rejects_non_unique_value_of_new_mapped_instance(session):
    session.add(Dummy(name="old"))
    session.flush()

    column = Column("name", Str(), required=True, unique=True)
    mapped_column = MappedColumnKey(Dummy, Dummy.name, Dummy.id, "id", column)

    with assert_errors([Error("already_exists")]):
        mapped_column.validate("old", False, {}, session)


def test_accepts_non_unique_value_of_old_mapped_instance(session):
    dummy = Dummy(name="old")
    session.add(dummy)
    session.flush()

    column = Column("name", Str(), required=True, unique=True)
    mapped_column = MappedColumnKey(Dummy, Dummy.name, Dummy.id, "id", column)

    assert mapped_column.validate("old", False, {}, session, dummy) == "old"
