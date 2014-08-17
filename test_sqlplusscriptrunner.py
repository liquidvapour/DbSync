import unittest
import unittest.mock as mock
import sqlplusscriptrunner


class Test_ScriptFailedException_test(unittest.TestCase):
    def test_when_instanciated_should_inherit_from_exception(self):
        sut = sqlplusscriptrunner.ScriptFailedException('c:\script\path.sql')

        self.assertIsInstance(sut, Exception)

    def test_will_set_script_path_from_first_argument(self):
        testPath = 'c:\script\path.sql'
        sut = sqlplusscriptrunner.ScriptFailedException(testPath)
        self.assertEqual(sut.script_path, testPath)

    @mock.patch('sqlplusscriptrunner.execute_sql_script')
    @mock.patch('sqlplusscriptrunner.start_sqlplus')
    def test_when_told_to_run_sql_script_and_script_failes_should_throw_ScriptFailedException(self, startSqlPlus, executeSqlScript):
        
        sqlplusProcess = startSqlPlus.return_value
        sqlplusProcess.wait.return_value = 1
        self.assertRaises(sqlplusscriptrunner.ScriptFailedException, sqlplusscriptrunner.run_sql_script, 'cnn', 'c:\test\path.sql', 'foo')

if __name__ == '__main__':
    unittest.main()
