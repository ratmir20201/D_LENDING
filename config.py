from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv()


class VerticaSettings(BaseSettings):
    """Настройки базы данных."""

    user: str = "user"
    password: str = "password"
    host: str = "localhost"
    port: int = 5433
    database: str = ""

    @property
    def conn_info(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "autocommit": True,
            "tlsmode": "disable",
        }

    class Config:
        env_prefix = "VERTICA__"


settings = VerticaSettings()
