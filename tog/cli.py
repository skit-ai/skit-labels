"""
Command line interface for interacting with a tog data server.
"""
import os
import pytz
import argparse

import toml
from tog import constants as const
from tog import commands


def is_timezone(value):
    if value not in pytz.all_timezones:
        raise argparse.ArgumentTypeError(
            f"Unknown timezone {value}. Lookup `pytz.all_timezones`."
        )


def build_dataset_from_tog_command(parser: argparse.ArgumentParser):
    parser.add_argument(
        "-j",
        "--job-id",
        type=int,
        help="Id of the tog dataset that we want to download.",
    )
    parser.add_argument(
        "-o",
        "--output-format",
        type=str,
        help="Store dataset in supported formats.",
        choices=[".csv", ".sqlite"],
    )
    parser.add_argument(
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
        "--task-type",
        type=str,
        default=const.TASK_TYPE__DICT,
        help="Task type for deserialization.",
        choices=const.TASK_TYPES,
    )


def build_dataset_from_dvc_command(parser: argparse.ArgumentParser) -> None:
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


def build_download_command(parser: argparse.ArgumentParser) -> None:
    data_source_parsers = parser.add_subparsers(dest="data_source")
    build_dataset_from_tog_command(
        data_source_parsers.add_parser(
            const.SOURCE__DB,
            help="Download a dataset from tog database.",
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


def build_describe_command(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--job-id", type=int, help="Id of the tog dataset that we want to describe."
    )


def build_stats_command(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--job-id",
        type=int,
        help="Check the state of the dataset i.e tagged, "
        "untagged and pending data points for a given job-id.",
    )


def get_version():
    project_toml = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'pyproject.toml'))
    with open(project_toml, 'r') as handle:
        project_metadata = toml.load(handle)
    return project_metadata["tool"]["poetry"]["version"]


def build_parser():
    parser = argparse.ArgumentParser(
        description=f"tog-cli {get_version()}. Command line interface for interacting with data server.",
    )
    command_parsers = parser.add_subparsers(dest="command")
    build_download_command(
        command_parsers.add_parser(
            const.DOWNLOAD,
            help="Download a dataset for a given tog id.",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
    )
    build_describe_command(
        command_parsers.add_parser(
            const.DESCRIBE, help="Describe a dataset for a given tog id."
        )
    )
    build_stats_command(
        command_parsers.add_parser(
            const.STATS, help="Get tagged/untagged points for a given tog id."
        )
    )
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    if args.command == const.DOWNLOAD and args.data_source == const.SOURCE__DB:
        df_path = commands.download_dataset_from_db(
            args.job_id,
            args.task_type,
            args.timezone,
            args.full,
            args.batch_size,
            args.output_format,
        )
        print(f"Saved dataframe to {df_path}.")
    if args.command == const.DOWNLOAD and args.data_source == const.SOURCE__DVC:
        df_path = commands.download_dataset_from_dvc(
            args.repo, args.path, args.remote
        )
        print(f"Saved dataframe to {df_path}.")
    elif args.command == const.DESCRIBE:
        commands.describe_dataset(args.job_id)
    elif args.command == const.STATS:
        commands.stat_dataset(args.job_id)
