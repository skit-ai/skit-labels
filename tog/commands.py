import pytz
import tempfile
from typing import Optional

import attr
import pandas as pd
from tqdm import tqdm

from tog.db import Job, SqliteDatabase
from tog import constants as const


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


def download_dataset(
    job_id: int,
    task_type: str,
    timezone: pytz.BaseTzInfo = pytz.UTC,
    full: bool = False,
    batch_size: int = 500,
):
    job = Job(job_id, task_type=task_type, tz=timezone)

    _, temp_filepath = tempfile.mkstemp(suffix=const.OUTPUT_FORMAT__SQLITE)
    sdb = SqliteDatabase(temp_filepath)
    bar = tqdm(total=job.total(untagged=full))

    describe_dataset(job_id)
    stat_dataset(job_id)

    for items in batch_gen(job.get(untagged=full), n=int(batch_size)):
        rows = []
        for task, tag, tagged_time in items:
            # For raw dictionary type tasks, we don't use attr classes.
            if isinstance(task, dict):
                task_dict = task
            else:
                task_dict = attr.asdict(task)

            # TODO: is_gold might not be working for dict type tasks as of now
            rows.append((task.id, task_dict, tag, task.is_gold, tagged_time))

        sdb.insert_rows(rows)
        bar.update(n=len(items))
    return sdb, temp_filepath


def sdb2df(sdb: SqliteDatabase, job_id: int) -> str:
    _, output_file = tempfile.mkstemp(prefix=f"job-{job_id}-", suffix=const.OUTPUT_FORMAT__CSV)
    df = pd.read_sql_query("SELECT * FROM data", sdb.conn)
    df.to_csv(output_file, index=False)
    return output_file


def describe_dataset(job_id: Optional[int] = None, job: Optional[Job] = None):
    job_ = job or Job(job_id)
    print(f"Job {job_.id}: {job_.name} [language: {job_.lang}]\n{job_.description}")

def stat_dataset(job_id: Optional[int] = None, job: Optional[Job] = None):
    job_ = job or Job(job_id)
    n_total = job_.total(untagged=True)
    n_tagged = job_.total()
    print(
        f"Total items {n_total}. Tagged {n_tagged}. Untagged {n_total - n_tagged}."
    )
