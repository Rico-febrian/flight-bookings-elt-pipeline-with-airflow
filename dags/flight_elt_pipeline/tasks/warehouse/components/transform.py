from airflow.exceptions import AirflowException, AirflowSkipException
from helper.postgres import Execute
from helper.logger import logger

# Initialize logger for the transform task
logger = logger(logger_name="transform-job")

class Transform:
    @staticmethod
    def transform_to_dwh(table_name, query_path):
        """
        Transform and load data from the staging database to the data warehouse table.

        This function executes a transformation query (typically an INSERT or MERGE)
        that moves data from the staging area (stg schema) into the data warehouse (dwh/final schema),
        applying business logic defined in the SQL script.

        Args:
            table_name (str): 
                The name of the target table in the data warehouse (e.g., 'fact_bookings').
                Used for logging purposes to identify which table is being transformed.

            query_path (str): 
                Path to the SQL query file that contains the transformation logic. 
                The file should be readable and contain valid SQL compatible with PostgreSQL.

        Returns:
            None: 
                This function does not return a value. It executes SQL and logs the result.

        Raises:
            AirflowException: 
                If the transformation fails due to any SQL or connection-related errors.
        """
        try:
            logger.info(f"Starting transformation for table: {table_name}")
            
            # Execute the transformation query (which should handle data insert/update)
            df = Execute._query(
                connection_id="warehouse-conn", # Airflow's connection for warehouse DB
                query_path=query_path           # SQL file with transformation logic
            )

            logger.info(f'Data successfully transformed and loaded into {table_name} table')

        except Exception as e:
            # Catch and log any error, then re-raise as AirflowException for proper DAG error tracking
            logger.error(f"Error during transformation and load data from staging to final schema in DWH: {e}", exc_info=True)
            raise AirflowException(f"Transform task failed for {table_name} table") from e