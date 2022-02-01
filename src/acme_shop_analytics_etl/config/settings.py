"""
Application Settings

Centralized configuration using dataclasses and environment variables.
"""
import os
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Optional


@dataclass
class DatabaseSettings:
    """Database connection settings."""
    
    url: str = field(
        default_factory=lambda: os.getenv(
            "DATABASE_URL",
            "postgresql://analytics:password@localhost:5432/acme_analytics",
        )
    )
    source_url: str = field(
        default_factory=lambda: os.getenv(
            "SOURCE_DATABASE_URL",
            "postgresql://readonly:password@localhost:5432/acme_production",
        )
    )
    pool_size: int = field(
        default_factory=lambda: int(os.getenv("DATABASE_POOL_SIZE", "5"))
    )
    max_overflow: int = field(
        default_factory=lambda: int(os.getenv("DATABASE_MAX_OVERFLOW", "10"))
    )


@dataclass
class ETLSettings:
    """ETL job configuration."""
    
    batch_size: int = field(
        default_factory=lambda: int(os.getenv("ETL_BATCH_SIZE", "1000"))
    )
    max_retries: int = field(
        default_factory=lambda: int(os.getenv("ETL_MAX_RETRIES", "3"))
    )
    retry_delay_seconds: int = field(
        default_factory=lambda: int(os.getenv("ETL_RETRY_DELAY_SECONDS", "60"))
    )


@dataclass
class LoggingSettings:
    """Logging configuration."""
    
    level: str = field(
        default_factory=lambda: os.getenv("LOG_LEVEL", "INFO")
    )
    format: str = field(
        default_factory=lambda: os.getenv("LOG_FORMAT", "structured")
    )


@dataclass
class PIISettings:
    """PII handling configuration."""
    
    encryption_key: Optional[str] = field(
        default_factory=lambda: os.getenv("PII_ENCRYPTION_KEY")
    )
    tokenization_salt: Optional[str] = field(
        default_factory=lambda: os.getenv("PII_TOKENIZATION_SALT")
    )


@dataclass
class HeaderSettings:
    """HTTP header configuration for correlation tracking."""
    
    # TODO(TEAM-API): Migrate from X-Legacy-User-Id to X-User-Id
    correlation_header: str = field(
        default_factory=lambda: os.getenv("CORRELATION_HEADER", "X-Acme-Request-ID")
    )
    legacy_user_id_header: str = field(
        default_factory=lambda: os.getenv("LEGACY_USER_ID_HEADER", "X-Legacy-User-Id")
    )
    user_id_header: str = field(
        default_factory=lambda: os.getenv("USER_ID_HEADER", "X-User-Id")
    )


@dataclass
class Settings:
    """
    Main application settings.
    
    Aggregates all configuration categories into a single settings object.
    """
    
    database: DatabaseSettings = field(default_factory=DatabaseSettings)
    etl: ETLSettings = field(default_factory=ETLSettings)
    logging: LoggingSettings = field(default_factory=LoggingSettings)
    pii: PIISettings = field(default_factory=PIISettings)
    headers: HeaderSettings = field(default_factory=HeaderSettings)
    
    # Environment indicator
    environment: str = field(
        default_factory=lambda: os.getenv("ENVIRONMENT", "development")
    )
    
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment.lower() == "production"
    
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.environment.lower() == "development"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Get the application settings singleton.
    
    Uses lru_cache to ensure settings are only loaded once.
    
    Returns:
        Settings: The application settings instance.
    """
    return Settings()
