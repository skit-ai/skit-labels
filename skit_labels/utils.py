"""
Module provides access to logger config, session token and package version.
"""
import os
import sys

import toml
from typing import Optional
from loguru import logger
from datetime import datetime
import pandas as pd
from typing import Union
from skit_labels import constants as const

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
    return None


def add_data_label(input_file: str, data_label: Optional[str] = None) -> str:
    df = pd.read_csv(input_file)
    data_label = data_label or None
    df = df.assign(data_label=data_label)
    df.to_csv(input_file, index=False)
    return input_file


def validate_headers(input_file, tagging_type):
    expected_columns_mapping  = const.EXPECTED_COLUMNS_MAPPING
    expected_headers = expected_columns_mapping.get(tagging_type)
    
    df = pd.read_csv(input_file)
    
    column_headers = df.columns.to_list()
    column_headers = [header.lower() for header in column_headers]
    column_headers = sorted(column_headers)
    expected_headers = sorted(expected_headers)
    
    logger.info(f"column_headers: {column_headers}")
    logger.info(f"expected_headers: {expected_headers}")
    
    is_match = column_headers == expected_headers
    logger.info(f"Is match: {is_match}")
    
    if not is_match:
        missing_headers = set(expected_headers).difference(set(column_headers))
        additional_headers = set(column_headers).difference(set(expected_headers))
        if missing_headers:
            return missing_headers
        elif additional_headers:
            df.drop(additional_headers, axis=1, inplace=True)
            df.to_csv(input_file, index=False)
            is_match = True
            logger.info(f"Following additional headers have been removed from the csv: {additional_headers}")
    return []


def validate_input_data(tagging_type, input_file):
    is_valid = True
    error = ''
    if tagging_type == const.CONVERSATION_TAGGING:
        missing_headers = validate_headers(input_file, tagging_type)
        if missing_headers:
            error = f'Headers in the input file does not match the expected fields. Missing fields = {missing_headers}'
            is_valid = False
        
    return is_valid, error