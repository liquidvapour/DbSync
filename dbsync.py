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

COMMAND_HELP = """
---------------
db syncher help
---------------
syntax: dbsyns.py [command] -s <schema>

arguments
---------
command 
    sync (default): syncronises the schema with source control
    drop: drops the schema.
"""

COMMANDS = ("sync", "drop")

def does_schema_exist_in_db(cnn, schema):
    cursor = cnn.cursor()
    schemas = [s[0].casefold() for s in cursor.execute(GET_ALL_SCHEMAS).fetchall()]
    cursor.close()
    return schema in schemas
    
def raise_schema_to_version(schema, version):
    pass

    
def get_all_folders_in(path):
    return [p for p in os.listdir(path) if os.path.isdir(os.path.join(path, p))]
    
def print_help_and_exit():    
    print(COMMAND_HELP)
    sys.exit()

def read_file(filename):
    with open(filename, 'r') as f:
        result = f.read()
    f.close()
    return result

def create_schema(cnn, schema):
    print('Running schema creation scripts for schema: "{0}".'.format(schema))
#    cursor = cnn.cursor()
#    createSchemaDdl = read_file()
#    print('ddl: {0}'.format(createSchemaDdl))
#    cursor.prepare(createSchemaDdl)
#    cursor.execute(None)
    output, error = runner.run_sql_script('{0}/{1}@{2}'.format(username, password, server), os.path.join('.', schema, 'create.user.sql'))
    print(output)
    print('error: "{0}".')
    print('schema ({0}) created.'.format(schema))
    
def process_schema(cnn, schema):
    if not schema in get_all_folders_in('.'):
        print('Cannot find schema folder for schema: "{0}".'.format(schema))
        return
        
    if not does_schema_exist_in_db(cnn, schema):
        create_schema(cnn, schema)
    else:
        print('schema "{0}" already exists.'.format(schema))
    
username = 'system'
password = 'password1234'
server = 'localhost:1521/XE'    
    
def main(argv):
    schema = ''
    try:
        opts, args = getopt.getopt(argv, 'hs:', 'schema=')
    except getopt.GetoptError:
        print_help_and_exit()
    for opt, arg in opts:
        if opt == '-h':
            print_help_and_exit()
        elif opt in ('-s', '-schema'):
            schema = arg
    
    if len(args) > 0:
        command = args[0].casefold()
    else:
        print('no command provided so defaulting to "sync".')
        command = "sync"
        
    if not command in COMMANDS: 
        print('command:"{0}" not one of the valid commands: {1}'.format(command, COMMANDS))
        print_help_and_exit()
        
    if not schema:
        print('no schema provided')
        print_help_and_exit()
    
    try:
        with cx_Oracle.connect(username, password, server) as db:
            process_schema(db, schema)
    except Exception as ex:
        print("Fatal error: {0}".format(ex))
        
    
    
def query_roles(connection):    
    cursor = connection.cursor()
    cursor.execute('select id, name from role')
    for item in cursor:
        print('{0}, name: {1}'.format(item[0], item[1]))
    cursor.close()
    
if __name__ == '__main__':
    main(sys.argv[1:])
    