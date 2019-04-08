import pytest


@pytest.fixture(scope='session')
def pgsql():
    import pytest_docker_fixtures
    host, port = pytest_docker_fixtures.pg_image.run()
    yield host, port
    pytest_docker_fixtures.pg_image.stop()


@pytest.fixture(scope='session')
def db(pgsql):
    from asyncom import OMDatabase
    from databases import DatabaseURL
    host, port = pgsql
    url = f'postgresql://postgres@{host}:{port}/guillotina'
    dbins = OMDatabase(DatabaseURL(url))
    yield dbins


@pytest.fixture
async def async_db(db):
    await db.connect()
    transaction = await db.transaction()
    yield db
    await transaction.rollback()
    await db.disconnect()
