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

GET_ALL_USERS = """
    select username 
    from dba_users
"""

    
def get_all_folders_in(path):
    return [p for p in os.listdir(path) if os.path.isdir(os.path.join(path, p))]


def get_all_files_in(path):
    return sorted([p for p in os.listdir(path) if os.path.isfile(os.path.join(path, p))])
    
        
username = 'system'
password = 'password1234'
server = 'localhost:1521/XE'    

class ArgumentsReader(object):
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
    



class DbSyncher(object):
    def __init__(self, username, password, host, schema, sqlRunner):
        self.__schema = schema
        self.__sqlRunner = sqlRunner

    def go(self):
        if self.schema_folder_exists():
            self.apply_schema_to_db()        

        for folder in self.get_all_version_folders():
            self.run_all_scripts_in(folder)

    def get_all_version_folders(self):
        root = os.path.join('.', self.__schema, 'versions')
        folders = get_all_folders_in(root)
        sortedFolders = sorted(folders, key=lambda a:StrictVersion(a))
        print('version folders: "{0}"'.format(sortedFolders))
        return [os.path.join(root, f) for f in sortedFolders]

    def schema_folder_exists(self):
        if not self.__schema in get_all_folders_in('.'):
            print('Cannot find schema folder for schema: "{0}". Please provide a folder named the same as the schema with all the appropriate scripts'.format(self.__schema))
            return False

        return True

    def schema_exists_in_db(self):
        userData = self.__sqlRunner.get_all_data_for(GET_ALL_USERS)
        schemas = [s[0].casefold() for s in userData]
        return self.__schema in schemas

    def apply_schema_to_db(self):
        if not self.schema_exists_in_db():
            if self.create_schema():
                self.apply_base_line_scripts()
        else:
            print('schema "{0}" already exists.'.format(self.__schema))

    def create_schema(self):
        print('Running schema creation scripts for schema: "{0}".'.format(self.__schema))
        scriptSucceeded = self.__sqlRunner.run_sql_script(os.path.join('.', self.__schema, 'create.user.sql'))
        if scriptSucceeded:
            print('schema ({0}) created.'.format(self.__schema))

        return scriptSucceeded

    def apply_base_line_scripts(self):
        print("applying baseline scripts")
        root = os.path.join('.', self.__schema, 'baseline')
        self.run_all_scripts_in(root)

    def run_all_scripts_in(self, root):    
        for f in get_all_files_in(root):
            scriptPath = os.path.join(root, f)
            if not self.run_script(scriptPath):
                print('failure running script: "{0}". stopping run!'.format(scriptPath))
                break        

    def run_script(self, filename):
        return self.__sqlRunner.run_sql_script(filename, self.__schema)


def sync_db(argReader):
    DbSyncher(
        username,
        password, 
        server, 
        argReader.get_schema(), 
        runner.Runner(username, password, server)).go()


def drop_schema(argReader):
    runner.drop_schema('{0}/{1}@{2}'.format(username, password, server), argReader.get_schema())
            

def main(argv):
    
    argReader = ArgumentsReader(argv)
    
    actions = {
        ArgumentsReader.SYNC: sync_db,
        ArgumentsReader.DROP: drop_schema
    }
    
    argReader.process(actions)

    
if __name__ == '__main__':
    main(sys.argv[1:])

        