import asyncio
import json
import os
import tempfile
from typing import Iterable, List, Optional

import aiohttp
import attr
import dvc.api
import jsonschema
import numpy as np
import pandas as pd
import pytz
from loguru import logger
from tqdm import tqdm

from skit_labels import constants as const
from skit_labels.db import Job, SqliteDatabase


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
    job_id: str,
    task_type: str,
    timezone: pytz.BaseTzInfo = pytz.UTC,
    full: bool = False,
    batch_size: int = 500,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    job = Job(int(job_id), task_type=task_type, tz=timezone)
    describe_dataset(job_id)
    stat_dataset(job_id)

    _, temp_filepath = tempfile.mkstemp(suffix=const.OUTPUT_FORMAT__SQLITE)
    sdb = SqliteDatabase(temp_filepath)
    bar = tqdm(total=job.total(untagged=full))

    for items in batch_gen(
        job.get(untagged=full, start_date=start_date, end_date=end_date),
        n=int(batch_size),
    ):
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


def sdb2df(sdb: SqliteDatabase, job_id: str) -> str:
    _, output_file = tempfile.mkstemp(
        prefix=f"job-{job_id}-", suffix=const.OUTPUT_FORMAT__CSV
    )
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
    print(f"Total items {n_total}. Tagged {n_tagged}. Untagged {n_total - n_tagged}.")


def download_dataset_from_dvc(repo: str, path: str, remote: Optional[str] = None):
    file_name = os.path.split(path)[-1]
    _, output_file = tempfile.mkstemp(suffix=file_name)
    with dvc.api.open(path, repo=repo, remote=remote) as f:
        df = pd.read_csv(f)
        df.to_csv(output_file, index=False)
    return output_file


def download_dataset_from_db(
    job_id: str,
    task_type: str,
    timezone: pytz.BaseTzInfo = pytz.UTC,
    full: bool = False,
    batch_size: int = 500,
    output_format: str = const.OUTPUT_FORMAT__CSV,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> str:
    sdb, sdb_path = download_dataset(
        job_id,
        task_type,
        timezone,
        full,
        batch_size,
        start_date=start_date,
        end_date=end_date,
    )
    if output_format == const.OUTPUT_FORMAT__CSV:
        df_path = sdb2df(sdb, job_id)
        os.remove(sdb_path)
        return df_path
    else:
        return sdb_path


def build_dataset(
    job_id: str, data_frame: pd.DataFrame, source: Optional[str] = const.DEFAULT_SOURCE
) -> List[dict]:
    """
    Build a dataset from the dataframe.

    :param job_id: The dataset id where data should be uploaded.
    :type job_id: str
    :param data_frame: The dataframe to upload.
    :type data_frame: pd.DataFrame
    :param source: The source of the data_frame.
    :type source: Optional[str]
    :return: The job-id where the data was uploaded.
    :rtype: int
    """
    dataset = []
    logger.debug(f"Pushing {len(data_frame)} items to {job_id=}")
    data_frame.fillna(np.nan, inplace=True)
    data_frame.replace([np.nan], [None], inplace=True)
    for _, row in tqdm(
        data_frame.iterrows(),
        total=len(data_frame),
        desc="Building a dataset for uploading safely.",
    ):
        dedupe_id = "_".join([row[const.CONVERSATION_UUID], row[const.CALL_UUID]])

        if const.RAW in data_frame.columns:
            data = json.loads(row[const.RAW])
        else:
            data = row.to_dict()

        alternatives = (
            data[const.UTTERANCES]
            if const.UTTERANCES in data
            else data[const.ALTERNATIVES]
        )
        if not alternatives:
            alternatives = []

        if isinstance(alternatives, str):
            alternatives = json.loads(alternatives)

        data_point = {
            const.PRIORITY: 1,
            const.DATA_SOURCE: source,
            const.DATA_ID: dedupe_id,
            const.DATA: {
                **data,
                const.CALL_UUID: str(row[const.CALL_UUID]),
                const.CONVERSATION_UUID: str(row[const.CONVERSATION_UUID]),
                const.ALTERNATIVES: alternatives,
            },
            const.IS_GOLD: False,
        }
        jsonschema.validate(data_point[const.DATA], const.UPLOAD_DATASET_SCHEMA)
        dataset.append(data_point)
    return dataset


async def upload_dataset(
    session: aiohttp.ClientSession, job_id: str, dataset: List[dict]
):
    path = f"/tog/tasks/?job_id={job_id}"
    async with session.post(path, json=dataset) as response:
        upload_response = await response.json()
        return (upload_response, response.status)


async def upload_dataset_batches(
    dataset_chunks: Iterable[List[dict]], url: str, token: str, job_id: str
) -> str:
    """
    Post the dataset to the server.

    :param dataset_chunks: The dataset to post.
    :type dataset_chunks: Iterable[dict]
    :param token: The token to use for authentication.
    :type token: str
    :param job_id: The job id where the dataset should be uploaded.
    :type job_id: str
    :return: The job id where the dataset was uploaded.
    :rtype: int
    """
    headers = {"Authorization": f"Bearer {token}"}
    async with aiohttp.ClientSession(url, headers=headers) as session:
        requests = [
            upload_dataset(session, job_id, dataset) for dataset in dataset_chunks
        ]
        return await asyncio.gather(*requests)


async def upload_dataset_to_db(
    input_file: str, url: str, token: str, job_id: str
) -> str:
    """
    Uploads a dataset to the database.

    :param input_file: Path to the input file.
    :type input_file: str
    :param url: The url to the dataset server.
    :type url: str
    :param token: The token for uploading to the target organization.
    :type token: str
    :param job_id: The dataset id where data should be uploaded, defaults to None
    :type job_id: Optional[str], optional
    :return: The job-id where the data was uploaded.
    :rtype: str
    """
    _, extension = os.path.splitext(input_file)
    if extension != ".csv":
        raise ValueError("Expected file extension to be a csv.")
    data_frame = pd.read_csv(input_file)
    dataset = build_dataset(job_id, data_frame, token)
    batched_dataset = batch_gen(dataset)
    responses = await upload_dataset_batches(batched_dataset, url, token, job_id)
    for message, status_code in responses:
        if status_code != 200:
            logger.error(message)

    return job_id
