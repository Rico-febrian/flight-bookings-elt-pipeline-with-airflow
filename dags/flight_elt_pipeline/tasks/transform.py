from helper.postgres import Execute
from helper.logger import logger
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logger(logger_name="transform-job")

class Transform:
    @staticmethod
    def transform_to_dwh(connection_id, query_path, table_name, schema):
        """
        Transform and load data from the staging database to the data warehouse table.

        This method truncates the target table in the data warehouse to ensure a full load,
        then executes the transformation SQL query to populate the table with fresh data.

        Args:
            connection_id (str): Airflow connection ID for the PostgreSQL data warehouse database.
            query_path (str): File path to the SQL transformation query to be executed.
            table_name (str): Target table name in the data warehouse to load data into.
            schema (str): Schema name in the data warehouse containing the target table.

        Returns:
            None: This function performs data transformation and load but returns no value.

        Raises:
            Exception: If any database operation or SQL execution fails.
        """
        try:
            logger.info("Initializing connection to data warehouse...")
            hook = PostgresHook(postgres_conn_id=connection_id)
            engine = hook.get_sqlalchemy_engine()

            # Prepare SQL command to truncate the target table to ensure a clean load
            truncate_query = f"TRUNCATE TABLE {schema}.{table_name} CASCADE;"
            logger.info(f'Truncating table {schema}.{table_name}...')
            
            # Execute truncation within a transactional block for atomicity
            with engine.connect() as connection:
                with connection.begin():
                    connection.execute(truncate_query)

            logger.info(f'Truncation completed. Running transformation query from {query_path}...')
            
            # Execute the transformation query (which should handle data insert/update)
            df = Execute._query(
                connection_id=connection_id,
                query_path=query_path
            )

            logger.info(f'Data successfully transformed and loaded into {schema}.{table_name}')

        except Exception as e:
            logger.error(f"Error during transformation and load data from staging to final schema in DWH: {e}", exc_info=True)
            raise

