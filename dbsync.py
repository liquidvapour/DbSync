import sys
import os
import getopt
import datetime
import cx_Oracle
import logging

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

CREATE_TRACKING_TABLE_SQL = """
    create table version_tracking (
        id         number                                  not null,
        version    varchar2(15)                            not null,
        script     varchar2(256)                           not null,
        applied_on timestamp     default current_timestamp not null,
        constraint version_tracking_pk primary key (id) enable validate)
"""

CREATE_TRACKING_TABLE_SEQ = """
    create sequence version_tracking_id_seq
    start with 1
    increment by 1
    minvalue 1
    nocache 
    nocycle 
    noorder
"""

INSERT_SCRIPT_INFO = """
    insert into version_tracking (id, version, script)
    values (version_tracking_id_seq.nextval, :version, :script)
"""    
        
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
syntax: dbsyns.py -s <schema> -v <version> [command]

arguments
---------
-s | -schema
    The schema you wish to apply.
-v | -version 
    The target version to bring database up to.
-l | -loglevel
    The required log level (DEBUG, INFO, WARN, ERROR)

    default: Bring database up to latest version.

command 
    sync (default): syncronises the schema with source control
    drop: drops the schema.

"""

    def __init__(self, argv):
        print('argv: {0}'.format(argv))
        schema = ''
        targetVersion = None
        logLevel = 'INFO'
        try:
            opts, args = getopt.getopt(argv, 'hs:v:l:', 'schema=version=loglevel=')
        except getopt.GetoptError:
            self.print_help_and_exit()
            
        print('opts: {0}'.format(opts))
        print('args: {0}'.format(args))
        for opt, arg in opts:
            if opt == '-h':
                self.print_help_and_exit()
            elif opt in ('-s', '-schema'):
                schema = arg
            elif opt in ('-v', '-version'):
                targetVersion = arg
            elif opt in ('-l', '-loglevel'):
                logLevel = arg
        
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
        self.__targetVersion = StrictVersion(targetVersion) if targetVersion else None
        self.__logLevel = getattr(logging, logLevel.upper())
        
        
    def get_command(self):
        return self.__command
    

    def get_schema(self):
        return self.__schema


    def get_target_version(self):
        return self.__targetVersion
            

    @property
    def log_level(self):
        return self.__logLevel


    def print_help_and_exit(self):    
        print(ArgumentsReader.COMMAND_HELP)
        sys.exit()
        

    def process(self, actions, *args):
        if self.get_command() in actions:
            actions[self.get_command()](self, *args)
        else:
            print('no action provided for command: "{0}".'.format(self.get_command()))

            
#------------------------------------------------------------------------------
# SourceOperations
#------------------------------------------------------------------------------
# Provides the source for the db
# methods:
# get_all_version_folders
# schema_folder_exists
#------------------------------------------------------------------------------
class SourceOperations(object):
    log = logging.getLogger('dbsync.SourceOperations')

    def __init__(self, schema):
        self.__schema = schema
        
    def get_all_version_folders(self):
        root = self.get_path_to_versions_folder()
        folders = self.get_all_folders_in(root)
        sortedFolders = sorted(folders, key = lambda a: StrictVersion(a))
        SourceOperations.log.info('version folders: "{0}"'.format(sortedFolders))
        return [(os.path.join(root, f), StrictVersion(f)) for f in sortedFolders]


    def get_path_to_versions_folder(self):
        return os.path.join('.', self.__schema, 'versions')

    def schema_folder_exists(self):
        if not self.__schema in self.get_all_folders_in('.'):
            SourceOperations.log.info('Cannot find schema folder for schema: "{0}". Please provide a folder named the same as the schema with all the appropriate scripts'.format(self.__schema))
            return False

        return True
        
    def get_all_folders_in(self, path):
        return [p for p in os.listdir(path) if not p.startswith('_') and os.path.isdir(os.path.join(path, p))]


class Db(object):
    log = logging.getLogger('dbsync.Db')

    def __init__(self, schema, sqlRunner):
        self.__schema = schema
        self.__sqlRunner = sqlRunner
        
        self.__allRunScripsByVersion = self.get_executed_scripts() if self._schema_exists_in_db() else {}

    def get_executed_scripts(self):
        """Returns a dictionary where:
            key = version and value = a list of all run scripts"""

        scriptData = self.__sqlRunner.get_all_data_for('select version, script from version_tracking', self.__schema)
        return self.__table_to_dict(scriptData)


    def make_sure_tacking_table_exists(self):
        self.__sqlRunner.run_sql_command((CREATE_TRACKING_TABLE_SQL, CREATE_TRACKING_TABLE_SEQ), self.__schema)


    def apply_schema_to_db(self):
        if not self._schema_exists_in_db():
            if self.create_schema():
                self.apply_base_line_scripts()

            self.make_sure_tacking_table_exists()
        else:
            Db.log.info('schema "{0}" already exists.'.format(self.__schema))


    def create_schema(self):
        Db.log.info('Running schema creation scripts for schema: "{0}".'.format(self.__schema))
        scriptSucceeded = self.__sqlRunner.run_sql_script(os.path.join('.', self.__schema, 'create.user.sql'))
        if scriptSucceeded:
            Db.log.info('schema ({0}) created.'.format(self.__schema))

        return scriptSucceeded


    def apply_base_line_scripts(self):
        Db.log.info("applying baseline scripts")
        root = os.path.join('.', self.__schema, 'baseline')
        # rp 2014-08-07: What "version" is the baseline. Is this important? If so then what is the convection for this.
        self.run_all_scripts_in(root)


    def get_all_files_in(self, path):
        Db.log.debug('get_all_files_in: {0}'.format(path))
        return sorted([os.path.join(path, p) for p in os.listdir(path) if not p.startswith('_') and os.path.isfile(os.path.join(path, p))])


    def run_all_scripts_in(self, root, version = None):    
        for scriptPath in self.get_all_files_in(root):
            Db.log.debug('considering "{0}"'.format(scriptPath))
            if version:
                if str(version) in self.__allRunScripsByVersion:
                    if not scriptPath in self.__allRunScripsByVersion[str(version)]:
                        if not self.apply_script(scriptPath, version): break
                    else:
                        Db.log.info('"{0}" - [{1}] aleady applied.'.format(scriptPath, version))
                else:
                    if not self.apply_script(scriptPath, version): break
            else:
                if not self.run_script(scriptPath):
                    Db.log.info('script: {0} failed!'.format(scriptPath))


    def apply_script(self, scriptPath, version):
        scriptDidRun = self.run_script(scriptPath)
        if scriptDidRun:
            self.record_script_as_run(scriptPath, version)
            return True
        else:
            Db.log.info('failure running script: "{0}". stopping run!'.format(scriptPath))
            return False


    def record_script_as_run(self, scriptPath, version):
        self.__sqlRunner.run_sql_command(INSERT_SCRIPT_INFO, self.__schema, {"version": str(version), "script": scriptPath})


    def run_script(self, filename):
        return self.__sqlRunner.run_sql_script(filename, self.__schema)


    def _schema_exists_in_db(self):
        userData = self.__sqlRunner.get_all_data_for(GET_ALL_USERS)
        schemas = [s[0].casefold() for s in userData]
        return self.__schema in schemas


    def __table_to_dict(self, table):
        result = {}

        for i in table:
            version = str(StrictVersion(i[0]))
            if not version in result:
                result[version] = []
            result[version].append(i[1])

        return result



log = logging.getLogger(__name__)

class DbUpdater(object):
    log = logging.getLogger('dbsync.DbUpdater')

    def __init__(self, db, sourceProvider):
        self.__db = db
        self.__sourceProvider = sourceProvider


    def bring_to_verion(self, targetVersion):
        
        if self.__sourceProvider.schema_folder_exists():
            self.__db.apply_schema_to_db()        

            for folder, version in self.__sourceProvider.get_all_version_folders():
                DbUpdater.log.debug('considering folder: "{0}" for version: "{1}".'.format(folder, version))
                if targetVersion:
                    if targetVersion >= version:
                        self.__db.run_all_scripts_in(folder, version)
                    else:
                        DbUpdater.log.debug('folder version "{0}" is greater than target version "{1}" so skipping.'.format(version, targetVersion))
                else:
                    DbUpdater.log.debug('No version provided to going to always apply scripts in folder.')
                    self.__db.run_all_scripts_in(folder, version)



def sync_db(argReader, sqlRunner):
    db = Db(
        argReader.get_schema(), 
        sqlRunner)
    
    updater = DbUpdater(db, SourceOperations(argReader.get_schema()))
    updater.bring_to_verion(argReader.get_target_version())


def drop_schema(argReader, sqlRunner):
    sqlRunner.drop_schema(argReader.get_schema())
            

def main(argv):
    log.debug('currnet path: {0}'.format(os.path.realpath('.')))

    argReader = ArgumentsReader(argv)
    
    logger = logging.basicConfig(level=argReader.log_level)
    
    actions = {
        ArgumentsReader.SYNC: sync_db,
        ArgumentsReader.DROP: drop_schema
    }
    
    argReader.process(actions, runner.OracleSqlRunner(username, password, server))

    
if __name__ == '__main__':
    main(sys.argv[1:])
