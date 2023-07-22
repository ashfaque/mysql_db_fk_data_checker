# pip install mysql-connector-python

import mysql.connector

def check_foreign_keys(connection):
    cursor = connection.cursor()

    # Get a list of all tables in the database
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]

    # Iterate through each table to find foreign keys
    for table in tables:
        cursor.execute(f"SHOW CREATE TABLE {table}")
        create_table_statement = cursor.fetchone()[1]

        # Extract foreign keys from the CREATE TABLE statement
        foreign_keys = []
        lines = create_table_statement.split("\n")
        for line in lines:
            if "FOREIGN KEY" in line:
                foreign_keys.append(line)

        # Check foreign keys for data discrepancies
        for foreign_key in foreign_keys:
            # Extract column and table names from the foreign key
            column_name = foreign_key.split("`")[1]
            reference_table = foreign_key.split("REFERENCES ")[1].split("(")[0].strip("`")

            # Build the query to find invalid references
            query = f"SELECT COUNT(*) FROM {table} WHERE {column_name} NOT IN (SELECT id FROM {reference_table})"
            cursor.execute(query)
            count = cursor.fetchone()[0]

            # Check for discrepancies and print results
            if count > 0:
                print(f"Found {count} discrepancies in the foreign key '{column_name}' of table '{table}'.")
                print(f"Invalid references in table '{reference_table}'.")

    cursor.close()

if __name__ == "__main__":
    # Replace with your database connection details
    db_config = {
        "host": "your_host",
        "user": "your_user",
        "password": "your_password",
        "database": "your_database",
    }

    try:
        connection = mysql.connector.connect(**db_config)
        check_foreign_keys(connection)
    except mysql.connector.Error as e:
        print(f"Error: {e}")
    finally:
        connection.close()
