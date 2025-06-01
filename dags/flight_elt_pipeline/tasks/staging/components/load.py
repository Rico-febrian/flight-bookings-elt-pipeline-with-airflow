from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException, AirflowSkipException
from helper.minio import MinioClient
from helper.logger import logger
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from io import BytesIO
from datetime import timedelta
from pangres import upsert
from minio.error import S3Error
import pandas as pd
import json

logger = logger(logger_name="load-job")

class Load:
    @staticmethod
    def load_to_staging(table_name, incremental, **kwargs):
        """
        Load CSV data from a MinIO bucket into a staging table in a PostgreSQL database.

        Args:
            connection_id (str): Airflow connection ID for the PostgreSQL staging database.
            bucket_name (str): Name of the MinIO bucket containing the CSV file.
            table_name (str): Name of the table (and CSV file) to load data into.
            schema (str): Database schema name where the staging table exists.

        Returns:
            None: This function uploads data to the staging table but does not return a value.

        Raises:
            Exception: If any operation fails during the loading process (e.g., connection errors, data upload failures).
        """
        
        try:

            date = kwargs.get('ds')
            previous_date = (pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")
            table_pkey = kwargs.get('table_pkey')

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
            if e.code == "NoSuchKey":
                raise AirflowSkipException(f"File {object_name} not found in bucket. Skipped...")
            else:
                raise AirflowException(f"MinIO error: {e}")
            
        except Exception as e:
            raise AirflowException(f"Unexpected error: {e}")
            
        try:

            # Read the CSV content from the MinIO object as bytes, convert to a pandas DataFrame
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
                # Define batch size
                BATCH_SIZE = 10000

                # Loop and send data for each batch
                for start in range(0, len(df), BATCH_SIZE):
                    end = start + BATCH_SIZE
                    batch = df.iloc[start:end]
                    
                    try:
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

            # Upload DataFrame contents into the staging table, appending data without DataFrame index
            logger.info(f'Uploading DataFrame to {table_name} table...')

        except AirflowSkipException as e:
            raise e
        
        except Exception as e:
            raise AirflowException(f"Error during loading {table_name} table : {str(e)}")
            