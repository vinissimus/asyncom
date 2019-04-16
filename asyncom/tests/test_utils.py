

import pytest
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as sa
from asyncom import OMBase
from asyncom.utils import import_data

Base = declarative_base(cls=OMBase)

pytestmark = pytest.mark.asyncio


class Data(Base):
    __tablename__ = 'data_importer'

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(100), index=True)


@pytest.fixture
async def data(async_db):
    url = str(async_db.url)
    engine = sa.create_engine(url)
    Base.metadata.create_all(engine)

data_yaml = """
---
- model: data_importer
  data:
    - id: 1
      name: hola
    - id: 2
      name: proba
    - id: 3
      name: proba3
"""


async def test_data_is_imported(async_db, data):
    await import_data(async_db, data_yaml, Base)
    res = await async_db.query(Data).get(1)
    assert res.name == "hola"
    total = await async_db.query(Data).count()
    assert total == 3


async def test_data_loader_from_file(async_db, data):
    await import_data(async_db, 'data_load.yaml', Base)
    res = await async_db.query(Data).get(1)
    assert res.name == "hola"
