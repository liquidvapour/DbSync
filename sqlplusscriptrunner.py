from subprocess import  Popen, PIPE

def run_sql_script(connstr, filename, schema = None):
    command = ['sqlplus', '-S', connstr]
    print(command)
    sqlplus = Popen(command, stdin=PIPE, stdout = PIPE, stderr=PIPE, universal_newlines = True)
    
    if schema:
        print('setting current schema to: "{0}".'.format(schema))
        sqlplus.stdin.write('ALTER SESSION SET CURRENT_SCHEMA = {0};\n'.format(schema))
        
    print('executing file: "{0}".'.format(filename))
    sqlplus.stdin.write('@"{0}"'.format(filename))
    return sqlplus.communicate()
    
def run_sql_command(connstr, script):
    command = ['sqlplus', '-S', connstr]
    print(command)
    sqlplus = Popen(command, stdin=PIPE, stdout = PIPE, stderr=PIPE, universal_newlines = True)
    sqlplus.stdin.write(script)
    return sqlplus.communicate()
