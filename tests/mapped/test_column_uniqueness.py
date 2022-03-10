import pytest
from goodboy.errors import Error
from sqlalchemy import Column, Date, Integer, String, column, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from goodboy_sqlalchemy.mapped import Mapped
from tests.conftest import assert_dict_value_errors

# Use in-memory SQLite
engine = create_engine("sqlite://")
Session = sessionmaker(engine)
Base = declarative_base()


class Dummy(Base):
    __tablename__ = "dummies"

    id = Column(Integer, primary_key=True)

    name = Column(String, nullable=False)
    code = Column(String, nullable=False, unique=True)


Base.metadata.create_all(engine)


@pytest.fixture()
def session():
    try:
        session = Session()
        yield session
    finally:
        session.rollback()


mapped = Mapped(Dummy, column_names=["name", "code"])


def test_accepts_new_value(session):
    session.add(Dummy(name="Dummy 1", code="dummy_1"))
    session.add(Dummy(name="Dummy 2", code="dummy_2"))

    good_value = {"name": "Dummy 3", "code": "dummy_3"}

    assert mapped(good_value, context={"session": session}) == good_value


def test_rejects_existent_value(session):
    session.add(Dummy(name="Dummy 1", code="dummy_1"))
    session.add(Dummy(name="Dummy 2", code="dummy_2"))

    bad_value = {"name": "Dummy 2", "code": "dummy_2"}

    with assert_dict_value_errors({"code": [Error("already_exists")]}):
        mapped(bad_value, context={"session": session})
