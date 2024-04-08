import os
import logging
import logging.handlers
import ecs_logging
from loguru import logger
from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Settings class for application settings and secrets management.

    Official documentation on pydantic settings management.

    - https://pydantic-docs.helpmanual.io/usage/settings/.
    """

    APP_NAME: str = "FutureData"

    # Logger
    LOG_VERBOSITY: str = "INFO"
    LOG_ROTATION_SIZE: str = "100MB"
    LOG_RETENTION: str = "30 days"
    LOG_FILE_NAME: str = "./logs/intella_{time:D-M-YY}.log"
    LOG_FORMAT: str = "{time:HH:mm:ss!UTC}\t|\t{file}:{module}:{line}\t|\t{message}"
    ECS_LOG_PATH: str = f"./logs/elastic-{os.getpid()}.log"
    PROFILE: bool = False

    # AWS Credentials
    AWS_REGION: str = "eu-central-1"
    AWS_ACCESS_KEY_ID: str = ""
    AWS_SECRET_ACCESS_KEY: str = ""
    AWS_SESSION_TOKEN: str = ""  # not necessary

    def _setup_logger(self) -> bool:
        # logger.remove() to remove default logging to StdErr
        logger.add(
            self.LOG_FILE_NAME,
            rotation=self.LOG_ROTATION_SIZE,
            retention=self.LOG_RETENTION,
            colorize=True,
            format=self.LOG_FORMAT,
            level=self.LOG_VERBOSITY,
            serialize=False,
            catch=True,
            backtrace=False,
            diagnose=False,
            encoding="utf8",
        )

        # Proxy loguru logs also to logging logger.
        # The ecs logging formats all logs from the python logging system for elastic.
        # It could be configured to read logs directly from loguru, but in that case it
        # would miss all parts of the system that log directly to the python logging
        # system.
        class PropagateHandler(logging.Handler):
            def emit(self, record):
                logging.getLogger(record.name).handle(record)

        logger.add(PropagateHandler(), format="{message}")

        # Add ECS rotating files sink
        pylogger = logging.getLogger()
        ecs_handler = logging.handlers.RotatingFileHandler(
            self.ECS_LOG_PATH,
            maxBytes=100_000_000,
            backupCount=2,
            encoding="utf8",
        )
        ecs_handler.setFormatter(
            ecs_logging.StdlibFormatter(
                # Extra configuration you want to have in the logging output
                extra={}
            )
        )
        ecs_handler.setLevel(logging.INFO)
        pylogger.addHandler(ecs_handler)

        return True


@lru_cache()
def get_settings() -> Settings:
    """Generate and get the settings."""
    try:
        return Settings()
    except Exception as message:
        logger.error(f"Error: impossible to get the settings: {message}")
        return None


settings = get_settings()
