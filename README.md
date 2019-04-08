
# AsyncOM

Async OM it's a super basic Object mapper based almost all on sqlalchemy
ORM layer.

We use the declarative extension for building the classes, and also,
factor instances of them on querys.

There is still no support for relations. At the moment it's
tied to the [encode/databases](https://github.com/encode/databases) dependency.



## Usage

```python

from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as sa

Base = declarative_base()


class OrmTest(Base):
    __tablename__ = 'orm_test'

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(100), index=True)
    value = sa.Column(sa.Text)


# Instead of usign Database from databases, you can use:

db = OMDatabase()
res = await db.query(OrmTest).filter(
    OrmTest.name.like('xx')).all()

# Look at tests
```


