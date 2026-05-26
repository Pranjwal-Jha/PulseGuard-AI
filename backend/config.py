"""Configuration management for RAG of Fire backend."""

from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # App
    app_name: str = "RAG of Fire"
    app_version: str = "0.1.0"
    debug: bool = False
    
    # Database
    database_url: str = "postgresql://user:password@localhost/rag_of_fire"
    
    # Vector DB
    vector_db_path: str = "./data/chroma_db"
    embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    
    # LLM
    use_real_llm: bool = False
    llm_provider: str = "openai"  # openai, anthropic
    openai_api_key: str = ""
    anthropic_api_key: str = ""
    llm_model: str = "gpt-4-turbo"
    
    # Notifications
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_username: str = ""
    smtp_password: str = ""
    slack_webhook_url: str = ""
    
    # Server
    host: str = "127.0.0.1"
    port: int = 8000
    
    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
