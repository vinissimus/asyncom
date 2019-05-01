
from asyncom import OMBase
from sqlalchemy.ext.declarative import declarative_base

import pytest
import sqlalchemy as sa

Base = declarative_base(cls=OMBase)


def my_default():
    return "kk"


count = 0


def my_counter():
    global count
    count = count + 1
    return count


class DefaultValues(Base):
    __tablename__ = "default"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(20), default="asdf")
    name2 = sa.Column(sa.String(20), default=my_default)
    name4 = sa.Column(sa.Integer, default=my_counter, onupdate=my_counter)


@pytest.fixture
async def data(async_db):
    url = str(async_db.url)
    engine = sa.create_engine(url)
    Base.metadata.create_all(engine)


@pytest.mark.asyncio
async def test_ensure_default_values_on_insert(async_db, data):
    item = DefaultValues()
    await async_db.add(item)
    assert item.name == "asdf"
    assert item.name2 == "kk"
    assert item.name4 == 1
    result = await async_db.query(DefaultValues).get(item.id)
    assert result.name == "asdf"
    assert result.name2 == "kk"
    global count
    delattr(item, "name4")
    await async_db.update(item)
    assert item.name4 == 2
