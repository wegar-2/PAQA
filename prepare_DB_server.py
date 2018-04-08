import sqlalchemy as sa
import logging


# 1. create connection string
root_pass = input("Type in the root password. ")
con_string = "mysql://root:" + root_pass + "@localhost/"

# 2. create connection engine
mysqldb_engine = sa.create_engine(con_string)

# 3. create schema, user and grant privileges
temp_connection = mysqldb_engine.connect()
temp_connection.execute("CREATE DATABASE IF NOT EXISTS PAQA_DB;")
temp_connection.execute("CREATE USER IF NOT EXISTS 'paqa_user'@'localhost' as identified by 'pass';")
temp_connection.execute("GRANT ALL ON PAQA_DB.* TO 'PAQA_USER'@'LOCALHOST';")
temp_connection.execute("FLUSH PRIVILEGES; ")
temp_connection.close()
del mysqldb_engine

# 4.





