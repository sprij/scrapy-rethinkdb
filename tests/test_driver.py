import unittest
from mock import Mock, patch, call

from scrapy_rethinkdb.driver import RethinkDBDriver, RqlQuery, TableNotFound


class RethinkDBDriverTest(unittest.TestCase):

    def setUp(self):
        # mocks
        self.stmt_mock = Mock(spec=RqlQuery)
        self.table_query_mock = Mock(spec=RqlQuery)

        # global patch for the official driver
        self.r_patcher = patch('scrapy_rethinkdb.driver.r')
        self.r = self.r_patcher.start()

        # default driver under test and connection call expected
        self.driver = RethinkDBDriver({'param1': 1, 'param2': 2})
        self.connection_call = call(param1=1, param2=2)

        #table name
        self.table_name = 'table'

    def tearDown(self):
        # stop global patchers
        self.r_patcher.stop()

    def test_init_invalid_settings_type(self):
        self.assertRaises(ValueError, RethinkDBDriver, None)
        self.assertRaises(ValueError, RethinkDBDriver, Mock())

    def test_connection_first_connection(self):
        connection = self.driver.connection
        self.assertEqual(self.r.connect.return_value, connection)
        self.assertEqual([self.connection_call], self.r.connect.call_args_list)

    def test_connection_second_connection(self):
        connection1 = self.driver.connection
        connection2 = self.driver.connection
        self.assertEqual(self.r.connect.return_value, connection1)
        self.assertEqual(self.r.connect.return_value, connection2)
        self.assertEqual([self.connection_call], self.r.connect.call_args_list)

    def test_connection_exception(self):
        self.r.connect.side_effect = Exception
        self.assertRaises(Exception, lambda: self.driver.connection)

    def test_execute_invalid_stmt(self):
        self.assertRaises(ValueError, self.driver.execute, Mock())

    def test_execute_exception(self):
        self.stmt_mock.run.side_effect = Exception
        self.assertRaises(Exception, self.driver.execute, self.stmt_mock)
        self.stmt_mock.run.assert_called_once_with(self.r.connect.return_value)

    def test_execute_success(self):
        result = self.driver.execute(self.stmt_mock)
        self.stmt_mock.run.assert_called_once_with(self.r.connect.return_value)
        self.assertEqual(self.stmt_mock.run.return_value, result)

    def test_table_exists_empty_database(self):
        self.r.table_list.return_value = self.stmt_mock
        self.r.table_list.return_value.run.return_value = []
        self.assertFalse(self.driver.table_exists(self.table_name))
        self.r.table_list.assert_called_once_with()
        self.stmt_mock.run.assert_called_once_with(self.r.connect.return_value)

    def test_table_exists_no_table_in_database(self):
        self.r.table_list.return_value = self.stmt_mock
        self.r.table_list.return_value.run.return_value = [
            'not_' + self.table_name,
            'really_not_' + self.table_name
        ]
        self.assertFalse(self.driver.table_exists(self.table_name))
        self.r.table_list.assert_called_once_with()
        self.stmt_mock.run.assert_called_once_with(self.r.connect.return_value)

    def test_table_exists_only_table_in_database(self):
        self.r.table_list.return_value = self.stmt_mock
        self.r.table_list.return_value.run.return_value = [self.table_name]
        self.assertTrue(self.driver.table_exists(self.table_name))
        self.r.table_list.assert_called_once_with()
        self.stmt_mock.run.assert_called_once_with(self.r.connect.return_value)

    def test_table_exists_table_in_database(self):
        self.r.table_list.return_value = self.stmt_mock
        self.r.table_list.return_value.run.return_value = [
            'not_' + self.table_name,
            'really_not_' + self.table_name,
            self.table_name
        ]
        self.assertTrue(self.driver.table_exists(self.table_name))
        self.r.table_list.assert_called_once_with()
        self.stmt_mock.run.assert_called_once_with(self.r.connect.return_value)

    def test_get_table_empty_database(self):
        self.r.table_list.return_value = self.stmt_mock
        self.r.table_list.return_value.run.return_value = []

        self.assertRaises(TableNotFound,
                          self.driver.get_table, self.table_name)

        self.r.table_list.assert_called_once_with()
        self.stmt_mock.run.assert_called_once_with(self.r.connect.return_value)
        self.assertFalse(self.r.table.called)

    def test_get_table_no_table_in_database(self):

        self.r.table_list.return_value = self.stmt_mock
        self.r.table_list.return_value.run.return_value = [
            'not_' + self.table_name,
            'really_not_' + self.table_name
        ]
        self.assertRaises(TableNotFound,
                          self.driver.get_table, self.table_name)
        self.r.table_list.assert_called_once_with()
        self.stmt_mock.run.assert_called_once_with(self.r.connect.return_value)
        self.assertFalse(self.r.table.called)

    def test_get_table_only_table_in_database(self):
        self.r.table_list.return_value = self.stmt_mock
        self.r.table_list.return_value.run.return_value = [self.table_name]
        self.r.table.return_value = self.table_query_mock

        table = self.driver.get_table(self.table_name)
        self.assertEqual(self.r.table.return_value, table)

        self.r.table_list.assert_called_once_with()
        self.r.table.assert_called_once_with(self.table_name)
        self.stmt_mock.run.assert_called_once_with(self.r.connect.return_value)

    def test_get_table_table_in_database(self):
        self.r.table_list.return_value = self.stmt_mock
        self.r.table_list.return_value.run.return_value = [
            'not_' + self.table_name,
            'really_not_' + self.table_name,
            self.table_name
        ]
        self.r.table.return_value = self.table_query_mock

        table = self.driver.get_table(self.table_name)
        self.assertEqual(self.r.table.return_value, table)

        self.r.table_list.assert_called_once_with()
        self.r.table.assert_called_once_with(self.table_name)
        self.stmt_mock.run.assert_called_once_with(self.r.connect.return_value)
