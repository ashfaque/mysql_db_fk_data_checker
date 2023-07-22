import os
import mysql.connector
from dotenv import load_dotenv
from ashfaquecodes.ashfaquecodes import get_execution_start_time, get_execution_end_time


load_dotenv()
execution_start_time = get_execution_start_time()


def check_foreign_keys(connection):
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

    # Check foreign keys for data discrepancies
    for table in tables:
        if table in table_foreign_keys:    # Only taking tables having foreign keys.
            for column_name, referenced_table_name in table_foreign_keys[table]:
                # Build the query to find invalid references
                query = f"SELECT COUNT(*) FROM {table} WHERE {column_name} IS NOT NULL AND {column_name} NOT IN (SELECT id FROM {referenced_table_name})"    # ? Skips null values in foreign keys.
                cursor.execute(query)
                count = cursor.fetchone()[0]

                # Check for discrepancies and save results
                if count > 0:
                    print(f"Found {count} discrepancies in the foreign key '{column_name}' of table '{table}' with reference table named '{referenced_table_name}'.")

    cursor.close()


if __name__ == "__main__":
    db_config = {
        "host": str(os.getenv('DB_HOST')),
        "user": str(os.getenv('DB_USER')),
        "password": str(os.getenv('DB_PASSWORD')),
        "database": str(os.getenv('DB_NAME')),
    }
    print('Database IP: %s, Database Name: %s' % (db_config['host'], db_config['database']))

    try:
        connection = mysql.connector.connect(**db_config)
        check_foreign_keys(connection)
    except mysql.connector.Error as e:
        print(f"Error: {e}")
    finally:
        total_execution_time_str = get_execution_end_time(execution_start_time, print_time = True)
        connection.close()
