create user foo
identified by foo
temporary tablespace temp
profile default;

GRANT CONNECT TO foo;
GRANT RESOURCE TO foo;
ALTER USER foo DEFAULT ROLE ALL;
