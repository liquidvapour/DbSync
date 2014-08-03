from subprocess import  Popen, PIPE
import cx_Oracle

class Runner(object):
    def __init__(self, username, password, host):
        self.__username = username
        self.__password = password
        self.__host = host
        self.__connectionString = '{0}/{1}@{2}'.format(username, password, host)
    
    def run_sql_script(self, filename, schema = None):
        return run_sql_script(self.__connectionString, filename, schema)

    def get_all_data_for(self, sqlScript):
        with cx_Oracle.connect(self.__username, self.__password, self.__host) as cnn:
            cursor = cnn.cursor()
            result = cursor.execute(sqlScript).fetchall()
            cursor.close()
        return result



def tell_sqlplus_to_exit_on_first_error_with_errorcode(stdin):
    stdin.write('WHENEVER SQLERROR EXIT 1;\n')

def set_current_schema_to(stdin, schema):
    print('setting current schema to: "{0}".'.format(schema))
    stdin.write('ALTER SESSION SET CURRENT_SCHEMA = {0};\n'.format(schema))

def execute_sql_script(stdin, filename):
    print('executing file: "{0}".'.format(filename))
    stdin.write('@"{0}"'.format(filename))

def drop_schema(connstr, schema):
    print('droping schema: "{0}".'.format(schema))
    if run_sql_command(connstr, 'drop user {0} cascade;'.format(schema)):
        print('"{0}" droped'.format(schema))
        return True

    return False


def run_sql_script(connstr, filename, schema = None):
    command = ['sqlplus', '-S', connstr]
    sqlplus = Popen(command, stdin=PIPE, stdout = PIPE, stderr=PIPE, universal_newlines = True)

    tell_sqlplus_to_exit_on_first_error_with_errorcode(sqlplus.stdin)

    if schema:
        set_current_schema_to(sqlplus.stdin, schema)
        
    execute_sql_script(sqlplus.stdin, filename)
    output = sqlplus.communicate()[0]
    exitcode = sqlplus.wait()
    
    if exitcode > 0:
        print(output)
        return False
        
    return True
    

def run_sql_command(connstr, script):
    command = ['sqlplus', '-S', connstr]
    print(command)
    sqlplus = Popen(command, stdin=PIPE, stdout = PIPE, stderr=PIPE, universal_newlines = True)
    sqlplus.stdin.write(script)
    return sqlplus.communicate()
