## Changelog

0.3.1
-----

- Be able to provide mapper_factory to `OMQuery`
  [vangheem]

0.3.0
---
- Fix bug with om.delete an inherited objects

0.2.3
---
- Fix boolean expr on adding prods.
  Allowing to add values with 0

0.2.1
---
- Small fix when checking none on json fields arrays

0.1.9
----
- Added Column default and onupdate on update and add
  (only for scalar values and callables)
- Added small proxy prop on OMDatabaes, to raw asyncpg connection

0.1.8
---
- Yaml data importer

0.1.7
----
- Support adding basic inherited models

0.1.6
---
- query should be an async iterator

0.1.5
---
- Load inherited models.

0.1.4
----
- query(Object).get should return None if not found

0.1.3
----
- Fixes and improvements. Lay out basic API

0.1.0 (2019-04-07)
------------------

* First release on PyPI.
