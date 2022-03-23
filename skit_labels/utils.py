"""
Module provides access to logger config, session token and package version.
"""
import os
import sys

import toml
from loguru import logger

LOG_LEVELS = ["CRITICAL", "ERROR", "WARNING", "SUCCESS", "INFO", "DEBUG", "TRACE"]


def get_version():
    project_toml = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "pyproject.toml")
    )
    with open(project_toml, "r") as handle:
        project_metadata = toml.load(handle)
    return project_metadata["tool"]["poetry"]["version"]


def configure_logger(level: int) -> None:
    """
    Configure the logger.
    """
    size = len(LOG_LEVELS)
    if level >= size:
        level = size - 1
    log_level = LOG_LEVELS[level]

    config = {
        "handlers": [
            {
                "sink": sys.stdout,
                "format": """
    -------------------------------------------------------
    <level>{level}</level>
    -------
    TIME: <green>{time}</green>
    FILE: {name}:L{line} <blue>{function}(...)</blue>
    <level>{message}</level>
    -------------------------------------------------------
    """,
                "colorize": True,
                "level": log_level,
            },
            {
                "sink": "file.log",
                "rotation": "500MB",
                "retention": "10 days",
                "format": "{time} {level} -\n{message}\n--------------------\n",
                "level": log_level,
            },
        ],
    }
    logger.configure(**config)
    logger.enable(__name__)


def read_session() -> str | None:
    """
    Read the session from the environment.
    """
    home = os.path.expanduser("~")
    try:
        with open(os.path.join(home, ".skit", "token"), "r") as handle:
            return handle.read().strip()
    except FileNotFoundError:
        return None
