from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from airflow import AirflowException
from helper.minio import MinioClient
from helper.postgres import Execute
from helper.logger import logger
from io import BytesIO
from datetime import timedelta
import pandas as pd

logger = logger(logger_name="extract-job")

BASE_PATH = "/opt/airflow/dags"

class Extract:
    @staticmethod
    def source_db(table_name, incremental, **kwargs):
        """
        Extract data from a PostgreSQL source database and upload it as a CSV file to a specified MinIO bucket.

        Args:
            connection_id (str): Airflow connection ID for the source PostgreSQL database.
            query_path (str): Path to the SQL SELECT query file within the DAGs directory.
            bucket_name (str): The target bucket in MinIO where the file will be stored.
            table_name (str): The name to be used for the resulting CSV file.

        Returns:
            None: The function performs an upload but returns no value.

        Raises:
            Exception: If any step fails (e.g., database connection, file I/O, or MinIO upload).
        """
        
        try:
            # Extract data from the source database using a SQL query file
            logger.info('Extracting data from source database...')

            pg_hook = PostgresHook(postgres_conn_id='sources-conn')
            connection = pg_hook.get_conn()
            cursor = connection.cursor()

            query = f"SELECT * FROM bookings.{table_name};"
            if incremental:
                date = kwargs['ds']
                query += f"WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY';"

                object_name = f"/temp/{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}.csv"

            else:

                object_name = f"/temp/{table_name}.csv"

            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            connection.commit()
            connection.close()

            column_list = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(result, columns=column_list)

            if df.empty:
                raise AirflowSkipException(f"{table_name} doesn't have new data. Skipped...")

            # Initialize MinIO client via Airflow connection
            logger.info('Initializing MinIO client...')
            client = MinioClient._get()

            # Define MinIO bucket name
            bucket_name = "extracted-data"
            
            # Create the bucket in MinIO if it doesn't already exist
            if not client.bucket_exists(bucket_name):
                logger.info(f"Bucket '{bucket_name}' does not exist. Creating bucket...")
                client.make_bucket(bucket_name)
            else:
                logger.info(f"Bucket '{bucket_name}' exists.")
            
            # Convert DataFrame to CSV in memory (no physical file written)
            logger.info('Converting DataFrame to CSV bytes...')
            csv_bytes = df.to_csv(index=False).encode('utf-8')  # Convert to CSV then encode to bytes
            csv_buffer = BytesIO(csv_bytes)  # Wrap it in a buffer to simulate a file object
            
            # Upload the CSV to the MinIO bucket under a temporary path
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
