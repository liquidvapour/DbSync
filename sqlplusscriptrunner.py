from subprocess import  Popen, PIPE

def run_sql_script(connstr, filename):
    command = ['sqlplus', '-S', connstr]
    print(command)
    sqlplus = Popen(command, stdin=PIPE, stdout = PIPE, stderr=PIPE, universal_newlines = True)
    sqlplus.stdin.write('@"{0}"'.format(filename))
    return sqlplus.communicate()
    
