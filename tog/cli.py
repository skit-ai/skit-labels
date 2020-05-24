"""
Command line interface for interacting with a tog data server.

Usage:
  tog download --job-id=<job-id> --output-sqlite=<output-sqlite>
    [--batch-size=<batch-size>] [--all] [--task-type=<task-type>]
  tog describe --job-id=<job-id>
  tog --version
  tog (-h|--help)

Options:
  --job-id=<job-id>                 Id of the tog job that we want to download
  --output-sqlite=<output-sqlite>   Output sqlite file path
  --batch-size=<batch-size>         Number of items to download in a batch via server sided cursor [default: 500]
  --all                             If provided, download all data instead of only tagged ones.
  --task-type=<task-type>           Task type for deserialization [default: conversation]
"""

import json
import sqlite3

import attr
from docopt import docopt
from tqdm import tqdm

from tog import __version__
from tog.db import Job


def create_data_table(conn):
    """
    Create a table for storing data in.
    """

    c = conn.cursor()
    c.execute("CREATE TABLE data (data_id INTEGER NOT NULL, data TEXT NOT NULL, tag TEXT NOT NULL, is_gold BOOLEAN NOT NULL, tagged_time TEXT)")
    conn.commit()


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

        conn = sqlite3.connect(args["--output-sqlite"])
        create_data_table(conn)
        bar = tqdm(total=job.total(untagged=args["--all"]))

        c = conn.cursor()
        for items in batch_gen(job.get(untagged=args["--all"]), n=int(args["--batch-size"])):
            rows = [(task.id, json.dumps(attr.asdict(task)), json.dumps(tag), task.is_gold, tagged_time) for task, tag, tagged_time in items]
            c.executemany("INSERT INTO data (data_id, data, tag, is_gold, tagged_time) VALUES (?, ?, ?, ?, ?)", rows)
            bar.update(n=len(items))

        conn.commit()
    elif args["describe"]:
        job = Job(int(args["--job-id"]))

        print(f"Job {job.id}: {job.name} [language: {job.lang}]\n{job.description}")
