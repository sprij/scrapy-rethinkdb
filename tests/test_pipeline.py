import unittest
from mock import Mock, MagicMock, patch
from itertools import combinations_with_replacement

from scrapy_rethinkdb.pipeline import RethinkDBPipeline, NotConfigured, Item


class RethinkDBPipelineTest(unittest.TestCase):

    def setUp(self):

        # default pipeline under test
        self.driver = Mock()
        self.table_name = Mock()
        self.insert_options = MagicMock()
        self.pipeline = RethinkDBPipeline(
            self.driver, self.table_name, self.insert_options
        )

        # patch for driver
        self.driver_patcher = patch('scrapy_rethinkdb.pipeline.'
                                    'RethinkDBDriver')
        # patcher for the pipeline constructor
        self.pipeline_cls_patcher = patch('scrapy_rethinkdb.pipeline.'
                                          'RethinkDBPipeline.__init__')

        # returns iterator for all possible combinations for 3 arguments
        # which values can be either a Mock or None
        self.init_mocks_iter = lambda: \
            combinations_with_replacement((None, Mock()), 3)

        # returns settings dictionary
        self.get_pipeline_settings = \
            lambda conn_sett, table_name, insert_options: \
            {'RETHINKDB_TABLE': table_name,
             'RETHINKDB_CONNECTION': conn_sett,
             'RETHINKDB_INSERT_OPTIONS': insert_options}

    def test_init_not_configured(self):
        # asserts constructor will raise NotConfigured id any of the arguments
        # is None

        comb_iter = self.init_mocks_iter()
        for driver, table_name, insert_options in comb_iter:
            if not driver or not table_name or not insert_options:
                self.assertRaises(NotConfigured, RethinkDBPipeline,
                                  driver, table_name, insert_options)

    def test_init_empty_table_name(self):
        # asserts constructor will raise NotConfigured if table_name is empty
        self.assertRaises(NotConfigured, RethinkDBPipeline, Mock(), '', Mock())

    def test_init_configured(self):
        # asserts that the default pipeline under test tried to get the table

        self.assertEqual(self.pipeline.table,
                         self.driver.get_table.return_value)
        self.driver.get_table.assert_called_once_with(self.table_name)

    def test_from_crawler_configured(self):
        # asserts that from_crawler will return a pipeline instance if the
        # constructor returns None, as expected

        crawler = Mock()

        comb_iter = self.init_mocks_iter()
        for conn_sett, table_name, insert_options in comb_iter:
            crawler.settings = self.get_pipeline_settings(
                conn_sett, table_name, insert_options
            )

            with self.pipeline_cls_patcher as pipeline_cls, \
                    self.driver_patcher as driver_klass:
                pipeline_cls.return_value = None

                pipeline = RethinkDBPipeline.from_crawler(crawler)
                self.assertIsInstance(pipeline, RethinkDBPipeline)

                driver_klass.assert_called_once_with(conn_sett)
                pipeline_cls.assert_called_once_with(
                    driver_klass.return_value, table_name, insert_options
                )

    def test_from_crawler_not_configured(self):
        # asserts that from_crawler will raise NotConfigured the
        # constructor raises NotConfigured exception
        crawler = Mock()

        for conn_sett, table_name, insert_options in self.init_mocks_iter():
            crawler.settings = self.get_pipeline_settings(
                conn_sett, table_name, insert_options
            )

            with self.pipeline_cls_patcher as pipeline_cls, \
                    self.driver_patcher as driver_klass:
                pipeline_cls.side_effect = NotConfigured

                self.assertRaises(NotConfigured,
                                  RethinkDBPipeline.from_crawler,
                                  crawler)

                driver_klass.assert_called_once_with(conn_sett)
                pipeline_cls.assert_called_once_with(
                    driver_klass.return_value, table_name, insert_options
                )

    def test_process_item_not_an_item(self):
        # asserts that a non-item is just returned

        spider = Mock()
        # mocking before_insert to check that it won't be called in this test
        self.pipeline.before_insert = Mock()
        self.pipeline.process_item(Mock(), spider)
        self.assertTrue(spider.log.msg.called)
        self.assertFalse(self.pipeline.before_insert.called)

    def test_process_item_success(self):
        # asserts that a item is processed

        item = Mock(spec=Item)
        item._values = {}

        # mocking extension points
        self.pipeline.before_insert = Mock()
        self.pipeline.after_insert = Mock()

        self.pipeline.process_item(item, Mock())

        self.pipeline.before_insert.assert_called_once_with(
            item
        )
        self.pipeline.table.insert.assert_called_once_with(
            item._values
        )
        self.pipeline.driver.execute.assert_called_once_with(
            self.pipeline.table.insert.return_value
        )
        self.pipeline.after_insert.assert_called_once_with(
            item, self.pipeline.driver.execute.return_value
        )
