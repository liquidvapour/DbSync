import sys
import os
import getopt
import cx_Oracle
from distutils.version import StrictVersion

import sqlplusscriptrunner as runner

# directory structure
# <root folder> SchemaName
#       - updates
#           - 1.1
#               - run files in alphabetic order
#
# each schema has a table to hold scrips run
# id, version, script_name
# when moving up to a particular version will run all scripts that have not already been applied until all scripts for 
# the desired version are applied.

GET_ALL_SCHEMAS = """
    select username 
    from dba_users
"""

def does_schema_exist_in_db(schema):
    with cx_Oracle.connect(username, password, server) as cnn:
        cursor = cnn.cursor()
        schemas = [s[0].casefold() for s in cursor.execute(GET_ALL_SCHEMAS).fetchall()]
        cursor.close()
    return schema in schemas
    
def raise_schema_to_version(schema, version):
    pass

    
def get_all_folders_in(path):
    return [p for p in os.listdir(path) if os.path.isdir(os.path.join(path, p))]
    
def read_file(filename):
    with open(filename, 'r') as f:
        result = f.read()
    f.close()
    return result

def create_schema(cnn, schema):
    print('Running schema creation scripts for schema: "{0}".'.format(schema))
    output, error = runner.run_sql_script('{0}/{1}@{2}'.format(username, password, server), os.path.join('.', schema, 'create.user.sql'))
    print(output)
    print('error: "{0}".'.format(error))
    print('schema ({0}) created.'.format(schema))
    
def process_schema(schema):
    if not schema in get_all_folders_in('.'):
        print('Cannot find schema folder for schema: "{0}". Please provide a folder named the same as the schema with all the appropriate scripts'.format(schema))
        return
        
    if not does_schema_exist_in_db(schema):
        create_schema(schema)
    else:
        print('schema "{0}" already exists.'.format(schema))
    
username = 'system'
password = 'password1234'
server = 'localhost:1521/XE'    

class ArgumentsReader:
    SYNC = "sync"
    DROP = "drop"
    COMMANDS = (SYNC, DROP)
    COMMAND_HELP = """
---------------
db syncher help
---------------
syntax: dbsyns.py -s <schema> [command]

arguments
---------
command 
    sync (default): syncronises the schema with source control
    drop: drops the schema.
"""

    def __init__(self, argv):
        print('argv: {0}'.format(argv))
        schema = ''
        try:
            opts, args = getopt.getopt(argv, 'hs:', 'schema=')
        except getopt.GetoptError:
            self.print_help_and_exit()
            
        print('opts: {0}'.format(opts))
        print('args: {0}'.format(args))
        for opt, arg in opts:
            if opt == '-h':
                self.print_help_and_exit()
            elif opt in ('-s', '-schema'):
                schema = arg
        
        if len(args) > 0:
            self.__command = args[0].casefold()
        else:
            print('no command provided so defaulting to "sync".')
            self.__command = "sync"
            
        if not self.__command in ArgumentsReader.COMMANDS: 
            print('command:"{0}" not one of the valid commands: {1}'.format(self.__command, ArgumentsReader.COMMANDS))
            self.print_help_and_exit()
            
        if not schema:
            print('no schema provided')
            self.print_help_and_exit()
            
        self.__schema = schema
        
    def get_command(self):
        return self.__command
    
    def get_schema(self):
        return self.__schema
            
    def print_help_and_exit(self):    
        print(ArgumentsReader.COMMAND_HELP)
        sys.exit()
        
    def process(self, actions):
        if self.get_command() in actions:
            actions[self.get_command()](self)
        else:
            print('no action provided for command: "{0}".'.format(self.get_command()))
        


def sync_db(argReader):
    process_schema(argReader.get_schema())

def drop_schema(argReader):
    schema = argReader.get_schema()
    print('droping schema: "{0}".'.format(schema))
    output, error = runner.run_sql_command('{0}/{1}@{2}'.format(username, password, server), 'drop user {0} cascade;'.format(schema))
    print(output)
        
def main(argv):
    
    argReader = ArgumentsReader(argv)
    
    actions = {
        ArgumentsReader.SYNC: sync_db,
        ArgumentsReader.DROP: drop_schema
    }
    
    argReader.process(actions)
    
    
    
def query_roles(connection):    
    cursor = connection.cursor()
    cursor.execute('select id, name from role')
    for item in cursor:
        print('{0}, name: {1}'.format(item[0], item[1]))
    cursor.close()
    
if __name__ == '__main__':
    main(sys.argv[1:])
    