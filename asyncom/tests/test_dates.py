import pytest
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as sa
from asyncom import OMBase
import datetime

Base = declarative_base(cls=OMBase)

pytestmark = pytest.mark.asyncio


class DatesModel(Base):
    __tablename__ = "test_dates"
    pk = sa.Column(sa.Integer, primary_key=True)
    field = sa.Column(sa.TIMESTAMP(True))


@pytest.fixture
async def data(async_db):
    url = str(async_db.url)
    engine = sa.create_engine(url)
    Base.metadata.create_all(engine)


async def test_withdates(async_db, data):
    obj = DatesModel()
    obj.field = datetime.datetime.now()
    await async_db.add(obj)
    assert await async_db.query(DatesModel).count() == 1
    obj.field = datetime.datetime.now()
    await async_db.update(obj)
