from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException, AirflowSkipException
from helper.minio import MinioClient
from helper.logger import logger
from sqlalchemy import create_engine
from io import BytesIO
from datetime import timedelta
from pangres import upsert
from minio.error import S3Error
import pandas as pd
import json

# Initialize logger for the load task
logger = logger(logger_name="load-job")

class Load:
    @staticmethod
    def load_to_staging(table_name, incremental, **kwargs):
        """
        Load CSV data from a MinIO bucket into a staging table in a PostgreSQL database.

        Args:
            table_name (str): 
                Name of the table to be loaded into the staging schema.
                This should match the original source table name.

            incremental (bool or str): 
                If True or a non-empty string, load from an incremental file (with date in the filename).
                Otherwise, load the full version of the file without date-based filtering.

            **kwargs: 
                Expected to contain Airflow context variables, specifically:
                    - 'ds' (str): Execution date in 'YYYY-MM-DD' format.
                    - 'table_pkey' (dict): Dictionary mapping table names to their primary key column(s).
        
        Raises:
            AirflowSkipException: If the file doesn't exist in MinIO (common in incremental mode when there's no new data).
            AirflowException: If any failure occurs during MinIO interaction, file parsing, or DB loading.
        """

        try:
            
            # Get yesterday's date based on Airflow execution date
            date = kwargs.get('ds')
            previous_date = (pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")

            # Get list of primary key from Airflow's variables
            table_pkey = kwargs.get('table_pkey')

            # Determine MinIO object path based on whether we're doing incremental or full load
            object_name = f'/temp/{table_name}-{previous_date}.csv' if incremental else f'/temp/{table_name}.csv'
            
            # Initialize MinIO client and fetch the CSV object from the specified bucket
            logger.info('Connecting to MinIO...')
            client = MinioClient._get()

            bucket_name = "extracted-data"
            logger.info(f"Fetching object from MinIO: {bucket_name}{object_name}")
            
            obj = client.get_object(
                bucket_name=bucket_name,
                object_name=object_name
            )

        except S3Error as e:
            # If file not found, skip the task instead of failing the whole pipeline
            if e.code == "NoSuchKey":
                raise AirflowSkipException(f"File {object_name} not found in bucket. Skipped...")
            else:
                raise AirflowException(f"MinIO error: {e}")
            
        except Exception as e:
            raise AirflowException(f"Unexpected error while accessing MinIO: {e}")
            
        try:

            # Read the CSV content from the MinIO object as bytes then convert to a pandas DataFrame
            logger.info('Reading CSV content into DataFrame...')
            df = pd.read_csv(BytesIO(obj.read()))
            
            # Close the MinIO object stream to free resources
            obj.close()
            logger.info('CSV loaded successfully!')

            # Convert specific columns to JSON string format for JSONB compatibility in Postgres
            jsonb_columns = ['model', 'airport_name', 'city', 'contact_data']
            logger.info(f'Converting selected columns to JSON strings (if exists)')

            for column_name in jsonb_columns:
                if column_name in df.columns:
                    # Convert each row's dictionary/list to a JSON-formatted string
                    df[column_name] = df[column_name].apply(json.dumps)

            logger.info('JSON columns converted')
            
            # Set table primary key
            df = df.set_index(table_pkey[table_name])

            # Create a SQLAlchemy engine from Airflow PostgresHook to connect to the staging DB
            logger.info('Connecting to PostgreSQL via SQLAlchemy engine...')
            
            engine = create_engine(PostgresHook(postgres_conn_id='warehouse-conn').get_uri())

            try:
                # Define number of rows per upsert batch
                BATCH_SIZE = 10000 

                # Process and load data in batches
                for start in range(0, len(df), BATCH_SIZE):
                    end = start + BATCH_SIZE
                    batch = df.iloc[start:end]
                    
                    try:
                        # Perform upsert: insert if new, update if exists
                        upsert(
                            con=engine,
                            df=batch,
                            table_name=table_name,
                            schema='stg',
                            if_row_exists='update'
                        )
                        logger.info(f"✅ Batch {start}-{end} loaded succesfully") 

                    except Exception as batch_error:
                        raise AirflowException(f"Failed upsert batch {start}-{end}: {batch_error}") 
            
            finally:
                engine.dispose()

            logger.info(f'Uploading DataFrame to {table_name} table...')

        except AirflowSkipException as e:
            raise e
        
        except Exception as e:
            raise AirflowException(f"Error during loading {table_name} table : {str(e)}")
            