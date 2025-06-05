from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException, AirflowSkipException
from helper.minio import MinioClient
from helper.logger import logger
from io import BytesIO
from datetime import timedelta
import pandas as pd

# Initialize logger for the extract task
logger = logger(logger_name="extract-job")

class Extract:
    @staticmethod
    def source_db(table_name, incremental, **kwargs):
        """
        Extract data from a PostgreSQL source table and upload it as a CSV file to MinIO.

        This method supports both full and incremental extraction. 
        For incremental extraction, it filters rows based on `created_at` and `updated_at` values.

        Args:
            table_name (str): 
                Name of the PostgreSQL table to extract data from. 
                This should be the raw table name without schema prefix.

            incremental (bool or str): 
                If True or a non-empty string, performs an incremental load, 
                filtering rows based on created_at or updated_at equal to the previous day (based on Airflow's execution date).
                If False or empty, performs a full load without filtering.

            **kwargs: 
                Airflow context parameters. Must include 'ds' (execution date) as a string in 'YYYY-MM-DD' format.
                Used to calculate the previous date for incremental filtering.

        Raises:
            AirflowSkipException: If no new data is found during incremental extraction.
            AirflowException: If any error occurs during the extraction or upload process.
        """
        
        try:
            logger.info('Extracting data from source database...')

            # Initialize PostgreSQL connection with Airflow's PostgresHook
            pg_hook = PostgresHook(postgres_conn_id='sources-conn')
            connection = pg_hook.get_conn()
            cursor = connection.cursor()

            # Define SQL query to fetch data from the target table
            query = f"SELECT * FROM bookings.{table_name} "
            
            # If incremental load is enabled (set to TRUE)
            if incremental:

                # Get yesterday's date based on Airflow execution date
                date = kwargs['ds']
                previous_date = (pd.to_datetime(date) - timedelta(days=1)).strftime('%Y-%m-%d')

                # Filter data based on created_at and updated_at columns
                query += (
                    f"WHERE DATE(created_at) = '{previous_date}' "
                    f"OR DATE(updated_at) = '{previous_date}';"
                )

                # Name the output file with the date suffix for traceability
                object_name = f"/temp/{table_name}-{previous_date}.csv"

            else:

                # For full load, use a simple filename without a date
                object_name = f"/temp/{table_name}.csv"

            # Execute query and fetch all results
            cursor.execute(query)
            result = cursor.fetchall()

            # Retrieve column names from the query result metadata
            column_list = [desc[0] for desc in cursor.description]

            # Convert the result to a DataFrame
            df = pd.DataFrame(result, columns=column_list)

            # Clean up DB connections
            cursor.close()
            connection.commit()
            connection.close()
            
            # If no data found in selected table when running the incremental load, skip the task
            if df.empty:
                raise AirflowSkipException(f"{table_name} doesn't have new data. Skipped...")

            # Initialize MinIO client via Airflow connection
            logger.info('Initializing MinIO client...')
            client = MinioClient._get()

            # Define MinIO bucket name to store extracted data
            bucket_name = "extracted-data"
            
            # Create the bucket in MinIO if it doesn't already exist
            if not client.bucket_exists(bucket_name):
                logger.info(f"Bucket '{bucket_name}' does not exist. Creating bucket...")
                client.make_bucket(bucket_name)
            else:
                logger.info(f"Bucket '{bucket_name}' exists.")
            
            # Convert DataFrame to CSV and store it in memory (no file writing to disk)
            logger.info('Converting DataFrame to CSV bytes...')
            csv_bytes = df.to_csv(index=False).encode('utf-8')  # Convert to CSV then encode to bytes
            csv_buffer = BytesIO(csv_bytes)  # Wrap it in a buffer to simulate a file object
            
            # Upload the CSV file to MinIO using the constructed path and buf
            logger.info(f'Uploading CSV to MinIO bucket...')
            client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,  # Object path in MinIO
                data=csv_buffer,
                length=len(csv_bytes),  # Required by MinIO to know how much data to expect
                content_type='application/csv'  # Metadata for file type
            )
            
            # Log the upload result
            logger.info(f'Data uploaded to selected MinIO bucket as {table_name}.csv')
        
        except AirflowSkipException as e:
            raise e
        except Exception as e:
            raise AirflowException(f"Error during extracting {table_name} table : {str(e)}")
