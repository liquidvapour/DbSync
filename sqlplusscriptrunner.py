from subprocess import  Popen, PIPE
from distutils.version import StrictVersion
import os.path
import cx_Oracle
import logging


class ScriptFailedException(Exception):
    def __init__(self, scriptPath):
        self.script_path = scriptPath

class OracleSqlRunner(object):
    log = logging.getLogger('sqlplusscriptrunner.OracleSqlRunner')
    def __init__(self, username, password, host):
        self.__username = username
        self.__password = password
        self.__host = host
        self.__connectionString = '{0}/{1}@{2}'.format(username, password, host)
    
    def run_sql_script(self, filename, schema = None):
        return run_sql_script(self.__connectionString, filename, schema)

    def run_sql_command(self, sql, schema = None, args = {}):
        
        with cx_Oracle.connect(self.__username, self.__password, self.__host) as cnn:
            if schema:
                cnn.current_schema = schema
            cursor = cnn.cursor()            
            if isinstance(sql, str):
                OracleSqlRunner.log.debug('Running command on schema {0}: {1}'.format(schema, sql))
                cursor.execute(sql, args)
            else:
                for cmd in sql: 
                    OracleSqlRunner.log.debug('Running command on schema {0}: {1}'.format(schema, cmd))
                    cursor.execute(cmd)

            cursor.close()


    def get_all_data_for(self, sqlScript, schema = None):
        with cx_Oracle.connect(self.__username, self.__password, self.__host) as cnn:
            if schema: cnn.current_schema = schema
            cursor = cnn.cursor()
            
            result = cursor.execute(sqlScript).fetchall()
            cursor.close()
        return result

    def drop_schema(self, schema):
        self.run_sql_command('drop user {0} cascade'.format(schema))



log = logging.getLogger('sqlplusscriptrunner')

def tell_sqlplus_to_exit_on_first_error_with_errorcode(stdin):
    stdin.write('WHENEVER SQLERROR EXIT 1;\n')


def set_current_schema_to(stdin, schema):
    log.info('setting current schema to: "{0}".'.format(schema))
    stdin.write('ALTER SESSION SET CURRENT_SCHEMA = {0};\n'.format(schema))


def execute_sql_script(stdin, filename):
    if not os.path.exists(filename):
        raise FileNotFoundError(filename)
    log.info('executing file: "{0}".'.format(filename))
    stdin.write('@"{0}"'.format(filename))


def drop_schema(connstr, schema):
    log.info('droping schema: "{0}".'.format(schema))
    if run_sql_command(connstr, 'dro user {0} cascade;'.format(schema)):
        log.info('"{0}" droped'.format(schema))
        return True

    return False


def run_sql_script(connstr, filename, schema = None):
    sqlplus = start_sqlplus(connstr)

    tell_sqlplus_to_exit_on_first_error_with_errorcode(sqlplus.stdin)

    if schema:
        set_current_schema_to(sqlplus.stdin, schema)
        
    execute_sql_script(sqlplus.stdin, filename)
    output = sqlplus.communicate()[0]
    exitcode = sqlplus.wait()
    
    if exitcode > 0:
        log.error('script failed with exit code: "{0}"'.format(exitcode))
        log.info(output)
        raise ScriptFailedException(filename)
        
    return True


def start_sqlplus(connstr):
    command = ['sqlplus', '-S', connstr]
    return Popen(command, stdin=PIPE, stdout = PIPE, stderr=PIPE, universal_newlines = True)


def run_sql_command(connstr, script, schema = None):
    sqlplus = start_sqlplus(connstr)

    tell_sqlplus_to_exit_on_first_error_with_errorcode(sqlplus.stdin)

    if schema:
        set_current_schema_to(sqlplus.stdin, schema)

    log.info('executing sql: {0}'.format(script))

    output = sqlplus.communicate('WHENEVER SQLERROR EXIT 1;\nselect from dual;\n')[0]
    exitcode = sqlplus.wait()
    
    
    if exitcode > 0:
        log.info(output)
        return False
        
    return True
