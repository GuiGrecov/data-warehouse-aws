import boto3
from botocore.exceptions import NoCredentialsError
import os
import sys
from dotenv import load_dotenv

load_dotenv()

class S3Client:
    def __init__(self):
        self._envs = {
            "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),  # Corrigido
            "region_name": os.getenv("AWS_REGION_NAME", "us-east-2"),
            "s3_bucket": os.getenv("S3_BUCKET_NAME"),
            "datalake": os.getenv("DELTA_LAKE_S3_PATH")
        }

        for var in self._envs:
            if self._envs[var] is None:
                print(f"Error: Environment variable {var} is not set.")
                sys.exit(1)

        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=self._envs["aws_access_key_id"],
            aws_secret_access_key=self._envs["aws_secret_access_key"],
            region_name=self._envs["region_name"]
        )

    def upload_file(self, data: bytes, s3_key: str) -> None:
        try:
            self.s3.put_object(Bucket=self._envs["s3_bucket"], Key=s3_key, Body=data)
            print(f"File uploaded successfully to {self._envs['s3_bucket']}/{s3_key}")
        except NoCredentialsError:
            print("Credentials not available.")

    def download_file(self, s3_key: str):
        try:
            file = self.s3.get_object(Bucket=self._envs["s3_bucket"], Key=s3_key)
            return file
        except NoCredentialsError:
            print("Credentials not available.")
            return None
        except FileNotFoundError:
            print(f"File {s3_key} not found in bucket {self._envs['s3_bucket']}.")
            return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def list_files(self, prefix: str = "") -> list:
        try:
            response = self.s3.list_objects_v2(Bucket=self._envs["s3_bucket"], Prefix=prefix)
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            else:
                print("No files found.")
                return []
        except NoCredentialsError:
            print("Credentials not available.")
            return []
        except Exception as e:
            print(f"An error occurred: {e}")
            return []
