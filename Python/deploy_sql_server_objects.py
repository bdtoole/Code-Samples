# To Do: Add GIT integration to pull the latest from master
# To Do: Convert CREATE or ALTER specification to CREATE OR ALTER statement (optional, depending on source control standard)
# Other enhancements as necessary based un new use cases
# Note that this script started to be scalable out for multiple objects but was initially only written for views

import pyodbc
server = <server>
database = <database>
Authentication='ActiveDirectoryInteractive'
driver = '{ODBC Driver 17 for SQL Server}'
local_repo = <repo location>

objects = ['object','list']
schemas = ['schema','list']
create_or_alter = 'ALTER'
test = False

for object in objects:
    for schema in schemas:
        object_name = schema + '.' + object
        file_path = local_repo + '\\views\\' + object + '.sql'

        # Read in the file, stripping out \nGO and everything before the create_or_alter statement, and substituting 
        # the appropriate schema in place of "schema"
        sql_file = ''.join(open(file_path).read().replace('schema',schema).partition(create_or_alter)[1:]).replace('\nGO','')
        sql_file = '\n' + sql_file + '\n'

        # Execute sql, rolling back if in Test mode and committing if not
        with pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';AUTHENTICATION='+Authentication) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_file)
                if test:
                    conn.rollback()
                    print('Rolled back transaction for ' + object_name)
                else:
                    conn.commit()
                    print('Committed transaction for ' + object_name)   
