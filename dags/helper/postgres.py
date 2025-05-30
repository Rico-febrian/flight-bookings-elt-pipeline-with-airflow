import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Base directory where all DAG-related files are stored
BASE_PATH = "/opt/airflow/dags"

class Execute:
    
    @staticmethod
    def _query(connection_id, query_path):
        """
        Executes a SQL query file on a PostgreSQL database.
        
        Args:
            connection_id (str): Airflow connection ID for the PostgreSQL database.
            query_path (str): Relative path to the SQL file located inside BASE_PATH.
        
        Raises:
            Exception: If the query execution or database connection fails.
        """
        # Create a PostgresHook using the given Airflow connection ID
        hook = PostgresHook(postgres_conn_id=connection_id)

        # Establish raw psycopg2 connection and cursor from the hook
        connection = hook.get_conn()
        cursor = connection.cursor()

        # Read the SQL query from the specified file
        with open(f'{BASE_PATH}/{query_path}', 'r') as file:
            query = file.read()

        # Execute the query
        cursor.execute(query)

        # Cleanup: close cursor and commit changes to the DB
        cursor.close()
        connection.commit()
        connection.close()

    @staticmethod
    def _get_dataframe(connection_id, query_path):
        """
        Executes a SQL SELECT query from file and returns the result as a pandas DataFrame.
        
        Args:
            connection_id (str): Airflow connection ID for the PostgreSQL database.
            query_path (str): Relative path to the SQL file located inside BASE_PATH.
        
        Returns:
            pd.DataFrame: Query results as a DataFrame, with column names included.
        
        Raises:
            Exception: If reading the file, executing the query, or creating the DataFrame fails.
        """
        # Create a PostgresHook using the given Airflow connection ID
        pg_hook = PostgresHook(postgres_conn_id=connection_id)

        # Establish raw psycopg2 connection and cursor
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        # Read SQL SELECT query from file
        with open(f'{BASE_PATH}/{query_path}', 'r') as file:
            query = file.read()

        # Execute the query and fetch all results
        cursor.execute(query)
        result = cursor.fetchall()

        # Extract column names from the query result
        column_list = [desc[0] for desc in cursor.description]

        # Convert the result into a pandas DataFrame
        df = pd.DataFrame(result, columns=column_list)

        # Cleanup: close cursor, commit (even though SELECT doesn't need it), and close connection
        cursor.close()
        connection.commit()
        connection.close()

        return df
