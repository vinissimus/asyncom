from pytest_docker_fixtures import images

import pytest

images.configure(
    "postgresql", "postgres", "11.1"
)


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
    dbins = OMDatabase(DatabaseURL(url), force_rollback=True)
    yield dbins


@pytest.fixture
async def async_db(db):
    await db.connect()
    yield db
    await db.disconnect()
