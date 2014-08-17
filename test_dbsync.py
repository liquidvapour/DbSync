import unittest
import unittest.mock as mock

import os.path

from dbsync import *

class TestDbUpdater(unittest.TestCase):
    def setUp(self):
        self.db = mock.Mock()
        self.sp = mock.Mock()
        self.sut = DbUpdater(self.db, self.sp)

    def test_when_schema_folder_exists_should_tell_db_to_apply_schema(self):
        self.sp.schema_folder_exists.return_value = True
        self.sp.get_all_version_folders.return_value = []

        self.sut.bring_to_verion('0.1')
        self.db.apply_schema_to_db.assert_called_with()  


    def test_when_schema_folder_does_not_exist_should_do_nothing(self):
        self.sp.schema_folder_exists.return_value = False

        self.sut.bring_to_verion('0.1')


    def test_when_schema_folder_exists_should_tell_db_to_run_all_scripts_in_each_folder(self):
        self.sp.schema_folder_exists.return_value = True
        self.sp.get_all_version_folders.return_value = [('first', '0.1'), ('second', '0.2')]

        self.sut.bring_to_verion(None)
        self.db.run_all_scripts_in.assert_has_calls([mock.call('first', '0.1'), mock.call('second', '0.2')])


    def test_when_schema_folder_exists_and_told_to_brind_to_version_should_tell_db_to_run_all_scripts_in_each_folder_with_lower_or_equal_version(self):
        versionFolders = [('zero', '0.0'), ('first', '0.1')]

        self.sp.schema_folder_exists.return_value = True
        self.sp.get_all_version_folders.return_value = versionFolders + [('second', '0.2')]

        self.sut.bring_to_verion('0.1')
        self.assertEqual(self.db.run_all_scripts_in.call_args_list, [mock.call(*x) for x in versionFolders])


    def test_when_schema_folder_exists_and_told_to_brind_to_version_None_should_tell_db_to_run_all_scripts_in_each_folder(self):
        versionFolders = [('zero', '0.0'), ('first', '0.1'), ('first b', '0.1')]

        self.sp.schema_folder_exists.return_value = True
        self.sp.get_all_version_folders.return_value = versionFolders

        self.sut.bring_to_verion(None)
        self.assertEqual(self.db.run_all_scripts_in.call_args_list, [mock.call(*x) for x in versionFolders])

class TestDb(unittest.TestCase):
    def test_get_executed_scripts_converts_data_table_to_dictionary(self):
        sqlRunner = mock.MagicMock()
        sut = Db('test', sqlRunner)
        sqlRunner.get_all_data_for.return_value = [('0.1', 'one zero.sql'), ('0.1', 'one one.sql'), ('0.2', 'two zero.sql')]

        result = sut.get_executed_scripts()

        self.assertEqual(result, {'0.1': ['one zero.sql', 'one one.sql'], '0.2': ['two zero.sql']})


    def test_get_executed_scripts_converts_data_table_to_dictionary_keeping_version_numbers_normalised(self):
        sqlRunner = mock.MagicMock()
        sut = Db('test', sqlRunner)
        sqlRunner.get_all_data_for.return_value = [('0.1.0', 'one zero.sql'), ('0.1', 'one one.sql'), ('0.2.0', 'two zero.sql')]

        result = sut.get_executed_scripts()

        self.assertEqual(result, {'0.1': ['one zero.sql', 'one one.sql'], '0.2': ['two zero.sql']})


    def test_schema_exists_in_db_is_case_insensetive_with_results_from_get_all_data_for(self):
        sqlRunner = mock.MagicMock()
        sut = Db('test', sqlRunner)
        sqlRunner.get_all_data_for.return_value = [('UsEr1',), ('uSeR2',), ('TeSt',)]

        result = sut._schema_exists_in_db()

        self.assertTrue(result)

    @mock.patch('dbsync.Db.get_all_files_in', return_value=['one.sql', 'two.sql'])
    def test_when_db_told_to_apply_base_line_script_will_call_get_all_scripts_for_baseline_folder(self, getAllFilesIn):
        sqlRunner = mock.MagicMock()
        sut = Db('test', sqlRunner)
        result = sut.apply_base_line_scripts()
        getAllFilesIn.assert_called_with(os.path.join('.', 'test', 'baseline'))


    @mock.patch('os.path.isfile', return_value=True)
    @mock.patch('os.listdir', return_value=['one.sql', 'two.sql', '_three.sql'])
    def test_when_db_told_to_get_all_files_in_folder_will_not_return_files_starting_with_underscore(self, listdir, isfile):
        sqlRunner = mock.MagicMock()
        sut = Db('test', sqlRunner)
        result = sut.get_all_files_in(os.path.join('.', 'root'))
        self.assertNotIn(os.path.join('.', 'root', '_three.sql'), result)
        self.assertIn(os.path.join('.', 'root', 'one.sql'), result)
        self.assertIn(os.path.join('.', 'root', 'two.sql'), result)

        
if __name__ == "__main__":
    logger = logging.basicConfig(level=logging.WARN)

    unittest.main()