from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import PostgresDsn, computed_field
from typing import Optional

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_ignore_empty=True,
        extra="ignore"
    )

    PROJECT_NAME: str = "Job Orchestrator"
    
    # Database
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "password"
    POSTGRES_SERVER: str = "localhost"
    POSTGRES_PORT: int = 5434
    POSTGRES_DB: str = "job_orchestrator"
    
    @computed_field
    @property
    def SQLALCHEMY_DATABASE_URI(self) -> str:
        return str(PostgresDsn.build(
            scheme="postgresql+asyncpg",
            username=self.POSTGRES_USER,
            password=self.POSTGRES_PASSWORD,
            host=self.POSTGRES_SERVER,
            port=self.POSTGRES_PORT,
            path=self.POSTGRES_DB,
        ))

    # Job Configuration
    DEFAULT_RETENTION_DAYS: int = 30
    DEFAULT_LEASE_TIMEOUT_SECONDS: int = 60
    
    # Redis (Optional mostly for rate limiting if we get to it)
    REDIS_URL: str = "redis://localhost:6379/0"

settings = Settings()
