"""
Module provides access to logger config, session token and package version.
"""
import os
import sys

import toml
from typing import Optional
from loguru import logger
from datetime import datetime


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


def read_session() -> Optional[str]:
    """
    Read the session from the environment.
    """
    home = os.path.expanduser("~")
    try:
        with open(os.path.join(home, ".skit", "token"), "r") as handle:
            return handle.read().strip()
    except FileNotFoundError:
        return None


def to_datetime(d: str) -> datetime:
    """
    Convert an arbitrary date string to ISO format.

    :param d: A date string.
    :type d: str
    :raises ValueError: If the date string can't be parsed.
    :return: The date string in ISO format.
    :rtype: datetime
    """
    if not isinstance(d, str):
        raise TypeError(f"Expected a string, got {type(d)}")

    time_fns = [
        datetime.fromisoformat,
        lambda date_string: datetime.strptime(
            date_string, "%Y-%m-%d %H:%M:%S.%f %z %Z"
        ),
        lambda date_string: datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%SZ"),
        lambda date_string: datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f%z"),
    ]

    for time_fn in time_fns:
        try:
            return time_fn(d)
        except ValueError:
            continue
    raise ValueError(f"Could not parse date string: {d}")
