"""
Command line interface for interacting with a tog data server.

Usage:
  tog download --job-id=<job-id> --output-sqlite=<output-sqlite>
    [--batch-size=<batch-size>] [--all] [--task-type=<task-type>]
  tog describe --job-id=<job-id>
  tog stats --job-id=<job-id>
  tog list
  tog --version
  tog (-h|--help)

Options:
  --job-id=<job-id>                 Id of the tog job that we want to download
  --output-sqlite=<output-sqlite>   Output sqlite file path
  --batch-size=<batch-size>         Number of items to download in a batch via server sided cursor [default: 500]
  --all                             If provided, download all data instead of only tagged ones.
  --task-type=<task-type>           Task type for deserialization [default: conversation]
"""

import os

import attr
from docopt import docopt
from tqdm import tqdm

from tog import __version__
from tog.db import Database, Job, SqliteDatabase


def batch_gen(source, n=100):
    """
    Batched generator
    """

    batch = []

    for it in source:
        batch.append(it)
        if len(batch) % n == 0:
            yield batch
            batch = []

    if batch:
        yield batch


def main():
    args = docopt(__doc__, version=__version__)

    if args["download"]:
        job = Job(int(args["--job-id"]), task_type=args["--task-type"])

        output_filepath = args["--output-sqlite"]
        if os.path.exists(output_filepath):
            raise RuntimeError(f"File already exists {output_filepath}")

        sdb = SqliteDatabase(output_filepath)
        bar = tqdm(total=job.total(untagged=args["--all"]))

        for items in batch_gen(job.get(untagged=args["--all"]), n=int(args["--batch-size"])):
            rows = [(task.id, attr.asdict(task), tag, task.is_gold, tagged_time) for task, tag, tagged_time in items]
            sdb.insert_rows(rows)
            bar.update(n=len(items))

    elif args["describe"]:
        job = Job(int(args["--job-id"]))

        print(f"Job {job.id}: {job.name} [language: {job.lang}]\n{job.description}")

    elif args["stats"]:
        job = Job(int(args["--job-id"]))

        n_total = job.total(untagged=True)
        n_tagged = job.total()

        print(f"Total items {n_total}. Tagged {n_tagged}. Untagged {n_total - n_tagged}.")

    elif args["list"]:
        db = Database()

        jobs = db.list_jobs()
        print(f"Total {len(jobs)} active jobs found\n")

        for i, job in enumerate(jobs):
            print(f"{i + 1}. Job {job['id']}: {job['name']} [language: {job['language']}]")
