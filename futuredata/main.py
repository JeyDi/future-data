from loguru import logger

from futuredata.src.config import settings

if __name__ == "__main__":
    logger.info(f"Welcome to: {settings.APP_NAME}")
