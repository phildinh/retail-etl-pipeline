# etl/utils/config.py

import os
from pathlib import Path
from pydantic_settings import BaseSettings
from pydantic import Field


# ─────────────────────────────────────────────
# WHY THIS FUNCTION EXISTS:
# We need to find the project root folder
# regardless of where Python is run from.
# Path(__file__) = this file's location
# .parent = etl/utils/ folder
# .parent.parent = etl/ folder  
# .parent.parent.parent = retail_etl/ (project root) ✓
# ─────────────────────────────────────────────
def get_project_root() -> Path:
    return Path(__file__).parent.parent.parent


def get_env_file() -> Path:
    """
    Read the ENV variable from OS to decide which .env file to load.
    Defaults to 'dev' if ENV is not set — safe for local development.
    
    WHY DEFAULT TO 'dev'?
    If a developer forgets to set ENV, we want them to hit the dev
    database, NOT production. Fail safe, not fail dangerous.
    """
    env = os.environ.get("ENV", "dev").lower()

    valid_envs = ["dev", "test", "prod"]
    if env not in valid_envs:
        raise ValueError(
            f"ENV='{env}' is not valid. Must be one of: {valid_envs}"
        )

    env_file = get_project_root() / f".env.{env}"

    if not env_file.exists():
        raise FileNotFoundError(
            f"Environment file not found: {env_file}\n"
            f"Make sure .env.{env} exists in the project root."
        )

    return env_file


class Settings(BaseSettings):
    """
    WHY PYDANTIC SETTINGS?
    - Reads from .env file automatically
    - Validates types (DB_PORT becomes int, not string)
    - Raises clear errors at startup if anything is missing
    - One single source of truth for all config values
    
    WHY FIELD(...) WITH alias?
    Our .env files use DB_SERVER but we want to call it
    db_host in Python code (more standard naming).
    The alias maps the .env key → Python attribute name.
    """

    # Database settings
    env: str = Field(default="dev", alias="ENV")
    db_host: str = Field(..., alias="DB_SERVER")
    db_port: int = Field(default=5432, alias="DB_PORT")
    db_name: str = Field(..., alias="DB_NAME")
    db_user: str = Field(..., alias="DB_USER")
    db_password: str = Field(..., alias="DB_PASSWORD")

    # API settings
    api_base_url: str = Field(..., alias="API_BASE_URL")

    model_config = {
        # Tell pydantic-settings to read from the correct .env file
        "env_file": str(get_env_file()),
        "env_file_encoding": "utf-8",
        # WHY populate_by_name=True?
        # Allows us to use either the alias (DB_SERVER)
        # OR the Python name (db_host) when creating Settings()
        "populate_by_name": True,
    }

    @property
    def db_url(self) -> str:
        """
        Build the SQLAlchemy connection string from individual parts.
        
        WHY A PROPERTY INSTEAD OF STORING THE FULL URL?
        Security. If you store the full URL in .env, the password
        is visible in one long string. Storing parts separately
        makes it easier to mask the password in logs.
        
        Format: postgresql+psycopg2://user:password@host:port/dbname
        The '+psycopg2' tells SQLAlchemy which driver to use.
        """
        return (
            f"postgresql+psycopg2://"
            f"{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}"
            f"/{self.db_name}"
        )

    @property
    def db_url_safe(self) -> str:
        """
        Same URL but with password masked — safe to print in logs.
        NEVER log the real db_url. Always log this one.
        """
        return (
            f"postgresql+psycopg2://"
            f"{self.db_user}:***"
            f"@{self.db_host}:{self.db_port}"
            f"/{self.db_name}"
        )


# ─────────────────────────────────────────────
# WHY MODULE-LEVEL SINGLETON?
# We create ONE Settings() instance here.
# Every other file does: from etl.utils.config import settings
# They all share the same object — loaded once, reused everywhere.
# This avoids reading the .env file 20 times across 20 files.
# ─────────────────────────────────────────────
settings = Settings()

# Initialise logger as soon as config is loaded
# WHY HERE? config.py is always the first import in every file.
# Initialising logger here means it's ready before anything else runs.
from etl.utils.logger import setup_logger
setup_logger(env=settings.env)