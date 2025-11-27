from pydantic_settings import BaseSettings
from typing import List, Union
from pydantic import field_validator
import os

class Settings(BaseSettings):
    # Database
    database_url: str = "sqlite:///./dms_database.db"
    
    # Security - MUST be set in .env file
    secret_key: str = ""  # Required: Set SECRET_KEY in .env
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    
    # OpenAI API - MUST be set in .env file for chat functionality
    openai_api_key: str = ""  # Required: Set OPENAI_API_KEY in .env
    
    # AWS Textract - MUST be set in .env file
    aws_access_key_id: str = ""  # Required: Set AWS_ACCESS_KEY_ID in .env
    aws_secret_access_key: str = ""  # Required: Set AWS_SECRET_ACCESS_KEY in .env
    aws_region: str = "us-east-1"  # Optional: Set AWS_REGION in .env (defaults to us-east-1)
    aws_s3_bucket: str = ""  # Required for S3-based processing: Set AWS_S3_BUCKET in .env
    
    # File Upload
    upload_dir: str = "./uploads"
    max_file_size: int = 10485760  # 10MB
    
    # CORS - can be comma-separated string or list
    allowed_origins: Union[str, List[str]] = "http://localhost:3000,http://127.0.0.1:3000"
    
    @field_validator('allowed_origins', mode='before')
    @classmethod
    def parse_allowed_origins(cls, v):
        if isinstance(v, str):
            # Split comma-separated string and strip whitespace
            return [origin.strip() for origin in v.split(',') if origin.strip()]
        return v
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        # Explicitly read from environment variables (for Railway, Render, etc.)
        env_file_encoding = 'utf-8'

settings = Settings()

# Debug: Print AWS configuration status (without exposing secrets)
print("🔧 AWS Configuration Check:")
print(f"   AWS_ACCESS_KEY_ID set: {bool(settings.aws_access_key_id)}")
print(f"   AWS_SECRET_ACCESS_KEY set: {bool(settings.aws_secret_access_key)}")
print(f"   AWS_REGION: {settings.aws_region}")
print(f"   AWS_S3_BUCKET set: {bool(settings.aws_s3_bucket)}")
if settings.aws_s3_bucket:
    print(f"   AWS_S3_BUCKET value: {settings.aws_s3_bucket}")
else:
    print("   ⚠️  AWS_S3_BUCKET is empty or not set!")

# Create upload directory if it doesn't exist
os.makedirs(settings.upload_dir, exist_ok=True)
