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


def build_download_command(parser: argparse.ArgumentParser) -> None:
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


def build_parser():
    with open("pyproject.toml") as handle:
        project_metadata = toml.load(handle)

    version = project_metadata["tool"]["poetry"]["version"]
    parser = argparse.ArgumentParser(
        description=f"tog-cli {version}. Command line interface for interacting with data server.",
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
    if args.command == const.DOWNLOAD:
        sdb, sdb_path = commands.download_dataset(
            args.job_id,
            args.task_type,
            args.timezone,
            args.full,
            args.batch_size,
        )
        if args.output_format == const.OUTPUT_FORMAT__CSV:
            df_path = commands.sdb2df(sdb, args.job_id)
            print(f"Saved dataframe to {df_path}.")
            os.remove(sdb_path)
        else:
            print(f"Saved sqlite database to {sdb_path}.")
    elif args.command == const.DESCRIBE:
        commands.describe_dataset(args.job_id)
    elif args.command == const.STATS:
        commands.stat_dataset(args.job_id)
