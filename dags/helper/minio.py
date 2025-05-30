from airflow.hooks.base import BaseHook
from minio import Minio

class MinioClient:
    @staticmethod
    def _get():
        """
        Creates a connection to the MinIO object storage service using
        credentials stored in Airflow's connection settings.

        Returns:
            Minio: A MinIO client object ready to perform operations like upload/download.

        Raises:
            Exception: If the connection to MinIO fails, an error is raised with the failure reason.
        """
        try:
            # Retrieve MinIO connection configuration from Airflow by connection ID 'minio'
            minio = BaseHook.get_connection('minio-conn')

            # Initialize and return a MinIO client using credentials from the Airflow connection.
            # Details such as endpoint URL are pulled from the 'extra' field (a JSON string).
            client = Minio(
                endpoint=minio.extra_dejson['endpoint_url'],  # MinIO server address (e.g., "localhost:9000")
                access_key=minio.login,                       # Access key (username) from Airflow connection
                secret_key=minio.password,                    # Secret key (password) from Airflow connection
                secure=False                                  # Use HTTP (not HTTPS); change to True if using SSL
            )

            return client

        except Exception as e:
            # Raise a new exception with error message if MinIO connection fails
            raise Exception(f'Cannot connect to MinIO due to the following error: {e}')
