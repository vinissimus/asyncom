import pytest
import asyncpg

@pytest.fixture(scope='session')
def pgsql():
    import pytest_docker_fixtures
    host, port = pytest_docker_fixtures.pg_image.run()
    yield host, port
    pytest_docker_fixtures.pg_image.stop()


@pytest.fixture(scope='session')
def db(pgsql):
    from asyncom import OMDatabase
    host, port = pgsql
    url = f'postgresql://postgres@{host}:{port}/guillotina'
    dbins = OMDatabase(url=url)
    yield dbins


@pytest.fixture
async def async_db(db):
    await db.connect()
    trans = db.transaction()
    await trans.start()
    yield db
    await trans.rollback()
    await db.disconnect()
