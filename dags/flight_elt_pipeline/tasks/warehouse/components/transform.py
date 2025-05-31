import os
from helper.postgres import Execute
from helper.logger import logger
from airflow.exceptions import AirflowException, AirflowSkipException

logger = logger(logger_name="transform-job")

class Transform:
    @staticmethod
    def transform_to_dwh(table_name, query_path):
        """
        Transform and load data from the staging database to the data warehouse table.

        Args:
            connection_id (str): Airflow connection ID for the PostgreSQL data warehouse database.
            query_path (str): File path to the SQL transformation query to be executed.
            table_name (str): Target table name in the data warehouse to load data into.

        Returns:
            None: This function performs data transformation and load but returns no value.

        Raises:
            Exception: If any database operation or SQL execution fails.
        """
        try:
            logger.info(f"Starting transformation for table: {table_name}")
            
            # Execute the transformation query (which should handle data insert/update)
            df = Execute._query(
                connection_id="warehouse-conn",
                query_path=query_path
            )

            # if not os.path.exists(query_path):
            #     raise AirflowSkipException(f"Transformation query file not found for table {table_name}")

            logger.info(f'Data successfully transformed and loaded into {table_name} table')

        except Exception as e:
            logger.error(f"Error during transformation and load data from staging to final schema in DWH: {e}", exc_info=True)
            raise AirflowException(f"Transform task failed for {table_name} table") from e