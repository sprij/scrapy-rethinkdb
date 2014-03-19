"""
Pipeline to insert documents to a RethinkDB table.

The minimal configuration requires defining the pipeline in the item pipelines
and a table name.

    ITEM_PIPELINES = [
      'scrapy_rethinkdb.RethinkDBPipeline',
    ]

    RETHINKDB_TABLE = 'items'

The connection will be created with the defaults of RethinkDB's python
driver (see http://www.rethinkdb.com/api/python/connect/) unless is
overwritten as in the following example to use the database called scrapydb:

    RETHINKDB_CONNECTION = {
        'db': 'scrapydb'
    }

The insertion will be done with the default options of RethinkDB's python
driver (see http://www.rethinkdb.com/api/python/insert/) unless is
overwritten as in the following example to insert or update the item:

    RETHINKDB_INSERT_OPTIONS = {
        'upsert': False
    }

The document inserted by default is the dictionary with values from the item.
This can be changed by extending the pipeline and overriding the method
get_document.

If any additional behaviour is needed before/after the insertion, such as
changing the item, dropping it, etc, this can be done by extending the pipeline
and overriding the method(s) before_insert/after_insert.
"""

from .pipeline import RethinkDBPipeline
