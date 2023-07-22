import os
import mysql.connector
from dotenv import load_dotenv
from ashfaquecodes.ashfaquecodes import get_execution_start_time, get_execution_end_time


load_dotenv()
execution_start_time = get_execution_start_time()


def log(discrepancy_count, table_name, primary_key_table, fk_name, referenced_table_name, invalid_ids):
    with open(f'{os.getcwd()}/discrepancies.log', 'a+') as _log:
        _log.write(
f"""
====================================================================================================
Found {discrepancy_count} discrepancies
TABLE: {table_name}
TABLE {table_name} PRIMARY KEY: {primary_key_table}
TABLE {table_name} FOREIGN KEY: {fk_name}
FOREIGN KEY {fk_name} REFERENCED TABLE: {referenced_table_name}
TABLE {table_name} ID: {invalid_ids}
====================================================================================================
""")


''' # ! Remove it later (1/2)
# def get_primary_key(cursor, table_name):
#     # Query the primary key column name for the given table
#     query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = '{table_name}' AND CONSTRAINT_NAME = 'PRIMARY'"
#     cursor.execute(query)
#     primary_key = cursor.fetchone()
#     if primary_key:
#         return primary_key[0]
#     return None
'''


def get_primary_key(cursor, table):
    # Build and execute the query to get the primary key
    query = f"SHOW KEYS FROM {table} WHERE Key_name = 'PRIMARY'"
    cursor.execute(query)
    result = cursor.fetchone()
    # print(result)
    if result:
        return result[4]  # The primary key name is at index 4 in the result
    else:
        return None


def check_foreign_keys(connection, batch_size=1000):
    cursor = connection.cursor(prepared=True)

    # Get a list of all tables in the database
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]

    # Fetch all foreign keys in a single query
    cursor.execute("SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_SCHEMA = %s", (db_config["database"],))
    foreign_keys = cursor.fetchall()

    # Create a dictionary to store the foreign keys for each table
    table_foreign_keys = {}
    for table_name, column_name, referenced_table_name in foreign_keys:
        if table_name not in table_foreign_keys:
            table_foreign_keys[table_name] = []
        table_foreign_keys[table_name].append((column_name, referenced_table_name))

    print('Total Tables Count: %d' % len(tables))
    # Check foreign keys for data discrepancies
    for index, table in enumerate(tables):
        print('Processing Table Number: ', index)
        if table in table_foreign_keys:    # Only taking tables having foreign keys.
            for column_name, referenced_table_name in table_foreign_keys[table]:
                ''' # DEPRECATED
                ## This block of code only shows the count of discrepancies.
                # Build the query to find invalid references
                query = f"SELECT COUNT(*) FROM {table} WHERE {column_name} IS NOT NULL AND {column_name} NOT IN (SELECT id FROM {referenced_table_name})"    # ? Skips null values in foreign keys.
                cursor.execute(query)
                count = cursor.fetchone()[0]

                # Check for discrepancies and save results
                if count > 0:
                    print(f"Found {count} discrepancies in the foreign key '{column_name}' of table '{table}' with reference table named '{referenced_table_name}'.")
                '''
                # Get the primary key column names for the current table and referenced table
                primary_key_table = get_primary_key(cursor, table)
                primary_key_referenced = get_primary_key(cursor, referenced_table_name)
                # print('-------', primary_key_table, table)
                # print('-------', primary_key_referenced, referenced_table_name)

                if primary_key_table and primary_key_referenced:
                    '''
                    This block of code shows the id if the `table` if there is discrepancy in the foreign key.
                    '''
                    # Build the query to find invalid references (excluding NULL values)
                    query = f"SELECT `{primary_key_table}` FROM {table} WHERE {column_name} IS NOT NULL AND {column_name} NOT IN (SELECT `{primary_key_referenced}` FROM {referenced_table_name})"    # Using `` as some keys might be keywords.
                    cursor.execute(query)

                    ''' # ! Remove it later (2/2)
                    # invalid_references = set()
                    # while True:
                    #     rows = cursor.fetchmany(batch_size)
                    #     if not rows:
                    #         break

                    #     # Process the rows with invalid references
                    #     for row in rows:
                    #         row_id = row[0]
                    #         invalid_references.add(row_id)
                    # if invalid_references:
                    #     print(f"Found {len(invalid_references)} discrepancies in the foreign key '{column_name}' of table '{table}' with reference table named '{referenced_table_name}' with '{table}' id: {list(invalid_references)}")
                    '''

                    invalid_rows = cursor.fetchall()  # Fetch all rows at once
                    # Check for discrepancies and print results
                    if invalid_rows:
                        invalid_ids = [row[0] for row in invalid_rows]
                        # ? print(f"Found {len(invalid_ids)} discrepancies in the foreign key '{column_name}' of table '{table}' with reference table named '{referenced_table_name}' with '{table}' id: {tuple(invalid_ids)}")
                        log(
                            discrepancy_count=len(invalid_ids),
                            table_name=table,
                            primary_key_table=primary_key_table,
                            fk_name=column_name,
                            referenced_table_name=referenced_table_name,
                            invalid_ids=tuple(invalid_ids)
                        )

                else:
                    print(f"Warning: Unable to determine the primary key for table '{table}' or referenced table '{referenced_table_name}'. Skipping foreign key check.")

    cursor.close()


if __name__ == "__main__":
    db_config = {
        "host": str(os.getenv('DB_HOST')),
        "user": str(os.getenv('DB_USER')),
        "password": str(os.getenv('DB_PASSWORD')),
        "database": str(os.getenv('DB_NAME')),
    }

    try:
        connection = mysql.connector.connect(**db_config)
        print('Database IP: %s, Database Name: %s' % (db_config['host'], db_config['database']))
        check_foreign_keys(connection)
    except mysql.connector.Error as e:
        print(f"Error: {e}")
    finally:
        total_execution_time_str = get_execution_end_time(execution_start_time, print_time = True)
        connection.close()
