"""
S3 Storage Service - AWS S3 integration for file storage and management.
Provides upload, download, delete, and presigned URL generation capabilities.
"""

import logging
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from typing import Optional, Dict, Any, BinaryIO, List
from datetime import datetime, timedelta
import os

from ...config import settings

logger = logging.getLogger(__name__)


class S3Service:
    """Service for AWS S3 operations"""
    
    def __init__(self):
        """Initialize S3 client with credentials from environment/config"""
        try:
            # Initialize S3 client
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=getattr(settings, 'AWS_ACCESS_KEY_ID', None),
                aws_secret_access_key=getattr(settings, 'AWS_SECRET_ACCESS_KEY', None),
                region_name=getattr(settings, 'AWS_REGION', 'us-east-1')
            )
            
            # Test connection
            self.s3_client.list_buckets()
            
        except NoCredentialsError:
            logger.warning("AWS credentials not found. S3 operations will fail.")
            self.s3_client = None
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {str(e)}")
            self.s3_client = None
    
    def upload_file(
        self,
        file_obj: BinaryIO,
        file_key: str,
        bucket: str,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Upload a file to S3.
        
        Args:
            file_obj: File-like object to upload
            file_key: S3 key (path) for the file
            bucket: S3 bucket name
            content_type: MIME type of the file
            metadata: Additional metadata to store with the file
            
        Returns:
            True if upload successful, False otherwise
        """
        if not self.s3_client:
            logger.error("S3 client not initialized")
            return False
        
        try:
            extra_args = {}
            
            if content_type:
                extra_args['ContentType'] = content_type
            
            if metadata:
                extra_args['Metadata'] = metadata
            
            self.s3_client.upload_fileobj(
                file_obj,
                bucket,
                file_key,
                ExtraArgs=extra_args
            )
            
            logger.info(f"Successfully uploaded file to s3://{bucket}/{file_key}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to upload file to S3: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during S3 upload: {str(e)}")
            return False
    
    def download_file(self, bucket: str, file_key: str, local_path: str) -> bool:
        """
        Download a file from S3.
        
        Args:
            bucket: S3 bucket name
            file_key: S3 key (path) of the file
            local_path: Local path to save the file
            
        Returns:
            True if download successful, False otherwise
        """
        if not self.s3_client:
            logger.error("S3 client not initialized")
            return False
        
        try:
            self.s3_client.download_file(bucket, file_key, local_path)
            logger.info(f"Successfully downloaded s3://{bucket}/{file_key} to {local_path}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to download file from S3: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during S3 download: {str(e)}")
            return False
    
    def delete_file(self, bucket: str, file_key: str) -> bool:
        """
        Delete a file from S3.
        
        Args:
            bucket: S3 bucket name
            file_key: S3 key (path) of the file to delete
            
        Returns:
            True if deletion successful, False otherwise
        """
        if not self.s3_client:
            logger.error("S3 client not initialized")
            return False
        
        try:
            self.s3_client.delete_object(Bucket=bucket, Key=file_key)
            logger.info(f"Successfully deleted s3://{bucket}/{file_key}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete file from S3: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during S3 deletion: {str(e)}")
            return False
    
    def get_presigned_url(
        self,
        bucket: str,
        file_key: str,
        expiration: int = 3600,
        http_method: str = 'GET'
    ) -> Optional[str]:
        """
        Generate a presigned URL for S3 object access.
        
        Args:
            bucket: S3 bucket name
            file_key: S3 key (path) of the file
            expiration: URL expiration time in seconds (default: 1 hour)
            http_method: HTTP method (GET, POST, PUT, DELETE)
            
        Returns:
            Presigned URL string or None if failed
        """
        if not self.s3_client:
            logger.error("S3 client not initialized")
            return None
        
        try:
            method_mapping = {
                'GET': 'get_object',
                'POST': 'post_object',
                'PUT': 'put_object',
                'DELETE': 'delete_object'
            }
            
            client_method = method_mapping.get(http_method.upper(), 'get_object')
            
            presigned_url = self.s3_client.generate_presigned_url(
                client_method,
                Params={'Bucket': bucket, 'Key': file_key},
                ExpiresIn=expiration
            )
            
            logger.info(f"Generated presigned URL for s3://{bucket}/{file_key}")
            return presigned_url
            
        except ClientError as e:
            logger.error(f"Failed to generate presigned URL: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during presigned URL generation: {str(e)}")
            return None
    
    def file_exists(self, bucket: str, file_key: str) -> bool:
        """
        Check if a file exists in S3.
        
        Args:
            bucket: S3 bucket name
            file_key: S3 key (path) of the file
            
        Returns:
            True if file exists, False otherwise
        """
        if not self.s3_client:
            logger.error("S3 client not initialized")
            return False
        
        try:
            self.s3_client.head_object(Bucket=bucket, Key=file_key)
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                logger.error(f"Failed to check file existence: {str(e)}")
                return False
        except Exception as e:
            logger.error(f"Unexpected error during file existence check: {str(e)}")
            return False
    
    def get_file_metadata(self, bucket: str, file_key: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for an S3 file.
        
        Args:
            bucket: S3 bucket name
            file_key: S3 key (path) of the file
            
        Returns:
            Dictionary containing file metadata or None if failed
        """
        if not self.s3_client:
            logger.error("S3 client not initialized")
            return None
        
        try:
            response = self.s3_client.head_object(Bucket=bucket, Key=file_key)
            
            metadata = {
                'size': response.get('ContentLength'),
                'last_modified': response.get('LastModified'),
                'content_type': response.get('ContentType'),
                'etag': response.get('ETag'),
                'metadata': response.get('Metadata', {}),
                'storage_class': response.get('StorageClass')
            }
            
            return metadata
            
        except ClientError as e:
            logger.error(f"Failed to get file metadata: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during metadata retrieval: {str(e)}")
            return None
    
    def list_files(
        self,
        bucket: str,
        prefix: str = "",
        max_keys: int = 1000
    ) -> Optional[List[Dict[str, Any]]]:
        """
        List files in an S3 bucket with optional prefix filter.
        
        Args:
            bucket: S3 bucket name
            prefix: Prefix to filter files (optional)
            max_keys: Maximum number of files to return
            
        Returns:
            List of file information dictionaries or None if failed
        """
        if not self.s3_client:
            logger.error("S3 client not initialized")
            return None
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            files = []
            for obj in response.get('Contents', []):
                file_info = {
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'],
                    'etag': obj['ETag'],
                    'storage_class': obj.get('StorageClass', 'STANDARD')
                }
                files.append(file_info)
            
            return files
            
        except ClientError as e:
            logger.error(f"Failed to list files: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during file listing: {str(e)}")
            return None