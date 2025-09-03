import logging

from src.utils.logger import setup_logger


def test_setup_logger_creates_logger():
    logger = setup_logger("test_logger")
    assert isinstance(logger, logging.Logger)
    assert logger.name == "test_logger"
