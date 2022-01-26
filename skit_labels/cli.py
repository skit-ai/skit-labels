"""
Command line interface for interacting with a tog datasets.
"""
import argparse
import asyncio
import os
import sys
from ast import arg
from datetime import datetime

import pytz

from skit_labels import __version__, commands
from skit_labels import constants as const
from skit_labels import utils


def is_timezone(value: str) -> str:
    if value not in pytz.all_timezones:
        raise argparse.ArgumentTypeError(
            f"Unknown timezone {value}. Lookup `pytz.all_timezones`."
        )
    return value


def is_numeric(value: str) -> str:
    if not isinstance(value, str):
        raise argparse.ArgumentTypeError(f"{value} is not a string.")
    if not value.isdigit():
        raise argparse.ArgumentTypeError(f"{value} is not a numeric value.")
    return value


def date_type(value: str):
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date {value}, expected YYYY-MM-DD.")


def create_job_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument(
        "-j",
        "--job-id",
        type=is_numeric,
        required=True,
        help="Id of the tog dataset that we want to download.",
    )
    parser.add_argument(
        "--start-date",
        type=date_type,
        help="Filter items added to the dataset after this date. (inclusive)",
    )
    parser.add_argument(
        "--end-date",
        type=date_type,
        help="Filter items added to the dataset before this date. (exclusive)",
    )
    parser.add_argument(
        "--db", type=str, help="Database name.", default=os.environ.get(const.TOGDB_DB)
    )
    parser.add_argument(
        "--host",
        type=str,
        help="Database host.",
        default=os.environ.get(const.TOGDB_HOST),
    )
    parser.add_argument(
        "--port",
        type=int,
        help="Database port.",
        default=os.environ.get(const.TOGDB_PORT),
    )
    parser.add_argument(
        "--user",
        type=str,
        help="Database user.",
        default=os.environ.get(const.TOGDB_USER),
    )
    parser.add_argument(
        "--password",
        type=str,
        help="Database password.",
        default=os.environ.get(const.TOGDB_PASSWORD),
    )
    return parser


def add_job_args(fn):
    def wrapper(*args, **kwargs):
        parser = fn(*args, **kwargs)
        return create_job_args(parser)

    return wrapper


@add_job_args
def build_dataset_from_tog_command(
    parser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    parser.add_argument(
        "-o",
        "--output-format",
        type=str,
        help="Store dataset in supported formats.",
        choices=[".csv", ".sqlite"],
        default=".csv",
    )
    parser.add_argument(
        "-tz",
        "--timezone",
        type=is_timezone,
        help="Timezone to parse datetime values. Like 'America/Los_Angeles', 'Asia/Kolkata' etc.",
        default=pytz.UTC,
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of items to download in a batch.",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="If provided, download all data instead of including untagged datapoints.",
    )
    parser.add_argument(
        "-tt",
        "--task-type",
        type=str,
        default=const.TASK_TYPE__CONVERSATION,
        help="Task type for deserialization.",
        choices=const.TASK_TYPES,
    )
    return parser


def build_dataset_from_dvc_command(
    parser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    parser.add_argument(
        "--repo", type=str, required=True, help="DVC enabled git repository."
    )
    parser.add_argument("--path", type=str, required=True, help="Path to the dataset.")
    parser.add_argument(
        "--remote",
        type=str,
        help="Remote. Required only if the repo "
        "hasn't set a default remote. This is usually a bucket name.",
    )
    return parser


def build_download_command(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    data_source_parsers = parser.add_subparsers(dest="data_source")
    build_dataset_from_tog_command(
        data_source_parsers.add_parser(
            const.SOURCE__DB,
            help="Download a dataset of a given id from the database.",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
    )
    build_dataset_from_dvc_command(
        data_source_parsers.add_parser(
            const.SOURCE__DVC,
            help="Download a dataset from a dvc enabled repo.",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
    )
    return parser


def upload_dataset_to_tog_command(
    parser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    parser.add_argument(
        "--url",
        type=str,
        help="URL of the dataset server. Optionally set the DATASET_SERVER_URL environment variable.",
        default=os.environ.get(const.DATASET_SERVER_URL),
    )
    parser.add_argument(
        "--token",
        type=str,
        help="The organization authentication token.",
        default=utils.read_session(),
    )
    parser.add_argument(
        "-i",
        "--input",
        type=str,
        help="The raw data to be uploaded.",
    )
    parser.add_argument(
        "-j",
        "--job-id",
        type=is_numeric,
        required=True,
        help="Id of the tog dataset that we want to download.",
    )
    return parser


def build_upload_command(parser: argparse.ArgumentParser) -> None:
    data_source_parsers = parser.add_subparsers(dest="data_source")
    upload_dataset_to_tog_command(
        data_source_parsers.add_parser(
            const.SOURCE__DB,
            help="Upload a dataset to the database. Creates a new dataset if dataset id not provided",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
    )


def build_cli():
    parser = argparse.ArgumentParser(
        description=f"skit-labels {__version__}. Command line interface for interacting with labelled datasets.",
    )
    parser.add_argument(
        "-v", action="count", help="Increase verbosity.", dest="verbosity", default=0
    )
    command_parsers = parser.add_subparsers(dest="command")
    build_download_command(
        command_parsers.add_parser(
            const.DOWNLOAD,
            help="Download a dataset. of a given id from the database.",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
    )
    build_upload_command(
        command_parsers.add_parser(
            const.UPLOAD,
            help="Upload a dataset.",
        )
    )
    create_job_args(
        command_parsers.add_parser(
            const.DESCRIBE, help="Describe a dataset for a given tog dataset id."
        )
    )
    create_job_args(
        command_parsers.add_parser(
            const.STATS, help="Get tagged/untagged points for a given tog dataset id."
        )
    )
    return parser


def upload_dataset(input_file, url, token, job_id):
    errors, df_size = asyncio.run(
        commands.upload_dataset_to_db(
            input_file,
            url,
            token,
            job_id,
        )
    )
    return errors, df_size


def cmd_to_str(args: argparse.Namespace) -> str:
    utils.configure_logger(args.verbosity)
    if args.command == const.DOWNLOAD and args.data_source == const.SOURCE__DB:
        return commands.download_dataset_from_db(
            args.job_id,
            args.task_type,
            args.timezone,
            full=args.full,
            batch_size=args.batch_size,
            output_format=args.output_format,
            start_date=args.start_date,
            end_date=args.end_date,
            db=args.db,
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
        )
    elif args.command == const.DOWNLOAD and args.data_source == const.SOURCE__DVC:
        return commands.download_dataset_from_dvc(args.repo, args.path, args.remote)
    elif args.command == const.UPLOAD and args.data_source == const.SOURCE__DB:
        if not args.token:
            raise ValueError(
                "Token is required for uploading to the database."
                "Use [skit-auth](https://github.com/skit-ai/skit-auth) to obtain the token."
            )

        if args.input is None:
            is_pipe = not os.isatty(sys.stdin.fileno())
            if is_pipe:
                args.input = sys.stdin.readline().strip()
            else:
                raise argparse.ArgumentTypeError(
                    "Expected to receive --input=<file> or its valued piped in."
                )

        errors, df_size = upload_dataset(args.input, args.url, args.token, args.job_id)

        if errors:
            return (
                f"Encountered {len(errors)} over {df_size}.\nSummary:\n{errors}."
            )
    elif args.command == const.DESCRIBE:
        return commands.describe_dataset(
            args.job_id,
            start_date=args.start_date,
            end_date=args.end_date,
            db=args.db,
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
        )
    elif args.command == const.STATS:
        return commands.stat_dataset(
            args.job_id,
            db=args.db,
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
        )


def main():
    parser = build_cli()
    args = parser.parse_args()
    message = cmd_to_str(args)
    if args.command == const.DOWNLOAD and args.data_source == const.SOURCE__DB:
        # Since the first element is the file, message[1] is the dataset type.
        print(message[0])
    elif args.command == const.STATS:
        print(commands.print_job_stats(message))
    else:
        print(message)
