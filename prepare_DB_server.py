import sqlalchemy as sa
import logging

# 0. getting a logger
main_logger = logging.getLogger(name="main_logger")


def prepare_DB_server():

    # 1. create connection string
    main_logger.info("Requesting user password to create target user and schema. ")
    root_pass = input("Type in the root password. ")
    con_string = "mysql://root:" + root_pass + "@localhost/"

    # 2. create connection engine
    mysqldb_engine = sa.create_engine(con_string)

    # 3. create schema, user and grant privileges
    try:
        temp_connection = mysqldb_engine.connect()
        temp_connection.execute("CREATE DATABASE IF NOT EXISTS PAQA_DB;")
        temp_connection.execute("CREATE USER IF NOT EXISTS 'PAQA_USER'@'localhost' identified by 'pass';")
        temp_connection.execute("GRANT ALL ON PAQA_DB.* TO 'PAQA_USER'@'localhost';")
        temp_connection.execute("FLUSH PRIVILEGES; ")
        temp_connection.close()
    except Exception as exc:
        main_logger.error(msg="Error occurred when creating the schema and user to work with. ")
        main_logger.error(msg=exc)
    else:
        del mysqldb_engine

    # 4. create & test connection engine as PAQA_USER to PAQA_DB
    try:
        mysqldb_engine = sa.create_engine("mysql+mysqldb://PAQA_USER:pass@localhost/PAQA_DB")
    except Exception as exc:
        main_logger.error(msg="Error occurred when createing a test connection engine...")
        main_logger.error(msg=exc)
    else:
        try:
            connection1 = mysqldb_engine.connect()
        except Exception as exc:
            main_logger.error("An error occurred when testing DB connectivity to the PAQA_DB schema...")
            main_logger.error(msg=exc)
        else:
            connection1.close()
            main_logger.info(msg="Test connection to DB schema PAQA_DB created and closed successfully!")
            del connection1, mysqldb_engine





