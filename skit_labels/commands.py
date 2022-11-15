import ast
import uuid
import asyncio
import ast
import json
import os
import tempfile
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
import aiofiles

import aiohttp
import attr
import dvc.api
import jsonschema
import numpy as np
import pandas as pd
import pytz
from loguru import logger
from requests import JSONDecodeError
from tqdm import tqdm

from skit_labels import constants as const
from skit_labels.db import Database, Job, LabelstudioJob, SqliteDatabase


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
    db: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[Union[int, str]] = None,
) -> Tuple[SqliteDatabase, str, str]:
    database = Database(db=db, user=user, password=password, host=host, port=port)
    JOB_CREATOR = LabelstudioJob if db == const.LABELSTUIO_DB else Job
    job = JOB_CREATOR(
        int(job_id),
        task_type=task_type,
        tz=timezone,
        start_date=start_date,
        end_date=end_date,
        database=database,
        db=db,
        user=user,
        password=password,
        host=host,
        port=port
    )
    if db != const.LABELSTUIO_DB:
        describe_dataset(job_id, job=job)
        stat_dataset(job_id, job=job)

    _, temp_filepath = tempfile.mkstemp(suffix=const.OUTPUT_FORMAT__SQLITE)
    sdb = SqliteDatabase(temp_filepath)
    bar = tqdm(total=job.total(untagged=full))
    data_ids = job.get_ids(untagged=full, start_date=start_date, end_date=end_date)
    
    for start_index in range(0, len(data_ids), batch_size):
        items = job.get(data_ids=data_ids[start_index:start_index+batch_size], untagged=full, start_date=start_date, end_date=end_date)
        rows = []
        for task, tag, tagged_time in items:
            # For raw dictionary type tasks, we don't use attr classes.
            task_dict = task if isinstance(task, dict) else attr.asdict(task)

            # TODO: is_gold might not be working for dict type tasks as of now
            rows.append(
                (
                    task.id,
                    task_dict,
                    tag,
                    task.is_gold,
                    tagged_time,
                    job_id
                )
            )

        sdb.insert_rows(rows)
        bar.update(n=len(items))
    return sdb, temp_filepath, job.type()


def parse_json(data: str) -> Dict[str, Any]:
    try:
        data = json.loads(data)
        data = data if isinstance(data, dict) else json.loads(data)
        return data
    except JSONDecodeError:
        return {}


def unpack(df: pd.DataFrame) -> pd.DataFrame:
    df.data = df.data.apply(parse_json)
    df_dict = df.to_dict(orient="records")
    df = pd.json_normalize(df_dict)
    columns = {col: col.replace("data.", "") for col in df.columns if col.startswith("data.") and "data_id" not in col}
    df.rename(columns=columns, inplace=True)
    return df


def sdb2df(sdb: SqliteDatabase, job_id: str) -> str:
    _, output_file = tempfile.mkstemp(
        prefix=f"job-{job_id}-", suffix=const.OUTPUT_FORMAT__CSV
    )
    df = pd.read_sql_query("SELECT * FROM data", sdb.conn)
    df = unpack(df)
    df.to_csv(output_file, index=False)
    return output_file


def describe_dataset(
    job_id: Optional[int] = None,
    job: Optional[Job] = None,
    db: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[Union[int, str]] = None,
) -> str:
    return job or Job(
        int(job_id),
        db=db,
        user=user,
        password=password,
        host=host,
        port=port,
    )


def stat_dataset(
    job_id: Optional[int] = None,
    job: Optional[Job] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    db: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[Union[int, str]] = None,
) -> dict:
    job_ = job or Job(
        int(job_id),
        start_date=start_date,
        end_date=end_date,
        db=db,
        user=user,
        password=password,
        host=host,
        port=port,
    )
    n_total = job_.total(untagged=True)
    n_tagged = job_.total()
    return {
        const.TOTAL: n_total,
        const.TAGGED: n_tagged,
        const.UNTAGGED: n_total - n_tagged,
    }


def print_job_stats(job_stats: dict) -> str:
    return json.dumps(job_stats, indent=2)


def download_dataset_from_dvc(
    repo: str, path: str, remote: Optional[str] = None
) -> str:
    file_name = os.path.split(path)[-1]
    _, output_file = tempfile.mkstemp(suffix=file_name)
    with dvc.api.open(path, repo=repo, remote=remote) as f:
        df = pd.read_csv(f)
        df.to_csv(output_file, index=False)
    return output_file


def processLabelstudioColumns(df_path: str):
    df = pd.read_csv(df_path)
    df[const.DATA_ID] = df[const.CONVERSATION_UUID].values
    df[const.ALTERNATIVES] = df[const.UTTERANCES].values if const.UTTERANCES in df else df[const.ALTERNATIVES]
    try:
        #e.g - "[{""id"": ""ID1lrD5_AT"", ""type"": ""choices"", ""value"": {""choices"": [""_confirm_""]}, ""origin"": ""manual"", ""to_name"": ""audio"", ""from_name"": ""tag""}]"
        df["tag"] = df["tag"].apply(lambda val: json.dumps(json.loads(val)[0]["value"]))
        df = df[df["tag"].apply(lambda val: "choices" in json.loads(val))]
        df["tag"] = df["tag"].apply(lambda val: json.loads(val)["choices"][0])
    except json.JSONDecodeError:
        logger.warning("please check tag column, it's unparseable to get a single value out")
        
    df.to_csv(df_path, index=False)

async def download_dataset_from_labelstudio(
    url: str,
    token: str,
    project_id: Union[int, str]
) -> Tuple[str, str]:
    """
    Download dataset from labelstudio
    """
    _, output_file = tempfile.mkstemp(suffix=const.OUTPUT_FORMAT__CSV)
    headers = {
        "Authorization": f"token {token}",
    }
    async with aiohttp.ClientSession(url, headers=headers) as session:
        async with session.get(url=f"/api/projects/{project_id}/export?exportType=CSV") as response:
            if response.status != 200:
                error_message = await response.text()
                raise Exception(f"Error downloading dataset: {error_message} {response.status} ")
            async with aiofiles.open(output_file, mode='wb') as f:
                await f.write(await response.read())
    
    processLabelstudioColumns(df_path=output_file)
    return output_file, "csv"


def download_dataset_from_db(
    job_id: str,
    task_type: str,
    timezone: pytz.BaseTzInfo = pytz.UTC,
    full: bool = False,
    batch_size: int = 500,
    output_format: str = const.OUTPUT_FORMAT__CSV,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    db: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[Union[int, str]] = None,
) -> Tuple[str, str]:
    sdb, sdb_path, dataset_type = download_dataset(
        job_id,
        task_type,
        timezone,
        full,
        batch_size,
        start_date=start_date,
        end_date=end_date,
        db=db,
        user=user,
        password=password,
        host=host,
        port=port,
    )
    if output_format == const.OUTPUT_FORMAT__CSV:
        df_path = sdb2df(sdb, job_id)
        if db == const.LABELSTUIO_DB:
            processLabelstudioColumns(df_path)
        os.remove(sdb_path)
        return df_path, dataset_type
    else:
        return sdb_path, dataset_type


def extract_utterances_safely(conversation_uuid, utterances):
    if not isinstance(utterances, (list, str)):
        return []
    try:
        utterances = (
            json.loads(utterances) if isinstance(utterances, str) else utterances
        )
    except json.JSONDecodeError:
        utterances = ast.literal_eval(utterances) if isinstance(utterances, str) else []
    except Exception as e:
        logger.warning(
            f"{conversation_uuid} has invalid utterances: {utterances}, setting to []."
        )
        logger.warning(e)
        utterances = []
    return utterances


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

        conversation_uuid = row[const.CONVERSATION_UUID]
        dedupe_id = f"{conversation_uuid}_{uuid.uuid4().hex}"
        errors = []
        if const.RAW in data_frame.columns:
            data = json.loads(row[const.RAW])
        else:
            data = row.to_dict()
        utterance_columns = {const.UTTERANCES, const.ALTERNATIVES}

        if data_frame.columns.intersection(utterance_columns).empty:
            raise ValueError(f"Expected one of {const.UTTERANCES} or {const.ALTERNATIVES} "
            "columns in the dataframe. {data_frame.columns}")
        utterance_col = const.UTTERANCES if const.UTTERANCES in data_frame.columns else const.ALTERNATIVES

        data_point = {
            const.PRIORITY: 1,
            const.DATA_SOURCE: source,
            const.DATA_ID: dedupe_id,
            const.DATA: {
                **data,
                const.CALL_UUID: str(row[const.CALL_UUID]),
                const.CONVERSATION_UUID: str(row[const.CONVERSATION_UUID]),
                const.ALTERNATIVES: extract_utterances_safely(row[const.CONVERSATION_UUID], row[utterance_col]),
            },
            const.IS_GOLD: False,
        }
        try:
            jsonschema.validate(data_point[const.DATA], const.UPLOAD_DATASET_SCHEMA)
            dataset.append(data_point)
        except jsonschema.exceptions.ValidationError as e:
            errors.append(e)
            if len(errors) > len(data_frame) * 0.5:
                raise RuntimeError(f"Too many errors: {errors}")

    return dataset


async def upload_dataset(
    session: aiohttp.ClientSession, job_id: str, dataset: List[dict], retries: int = 3
):
    sleep_time = 5 #seconds
    while retries >= 0 :
        path = f"/tog/tasks/?job_id={job_id}"
        status_code = 0
        try:
            async with session.post(path, json=dataset) as response:
                status_code = response.status
                if str(response.status).startswith("2"):
                    upload_response = await response.json()
                    return (upload_response, status_code)
                else:
                    raise aiohttp.ClientOSError
                
        except (aiohttp.ClientOSError, aiohttp.ServerDisconnectedError, asyncio.TimeoutError) as e:
            retries -= 1
            print(f"failed to upload dataset: {e},\n..retrying in {sleep_time} seconds")
            await asyncio.sleep(sleep_time)
    
    raise Exception(f"Error uploading dataset: {status_code}")


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
    print("Uploading batches")
    async with aiohttp.ClientSession(url, headers=headers) as session:
        requests = [
            upload_dataset(session, job_id, dataset) for dataset in dataset_chunks
        ]
        return await asyncio.gather(*requests)


async def upload_file(input_file: str, project_id: str, session: aiohttp.ClientSession) -> str:
    with open(input_file, "rb") as f:
        return await session.post(f"/api/projects/{project_id}/import", data={"file": f})


async def upload_dataset_to_labelstudio(
    input_file: str,
    url: str,
    token: str,
    project_id: str
) -> Tuple[List[str], int]:
    """
    Upload the dataset to LabelStudio.

    :return: The job id where the dataset was uploaded.
    :rtype: int
    """
    headers = {"Authorization": f"token {token}"}
    async with aiohttp.ClientSession(url, headers=headers) as session:
        response = await upload_file(input_file, project_id, session)

        if response.status != 201:
            error_message = await response.text()
            raise RuntimeError(f"Failed to upload dataset to LabelStudio: {error_message}, {response.status}")
        else:
            response = await response.json()
            return [], response["task_count"]


async def upload_dataset_to_db(
    input_file: str, url: str, token: str, job_id: str
) -> Tuple[List[str], int]:
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
    dataset = build_dataset(job_id, data_frame)
    batched_datasets = batch_gen(dataset, 100)
    errors_final = []
    for batched_dataset in batch_gen(batched_datasets, 10):
        responses = await upload_dataset_batches(batched_dataset, url, token, job_id)
        errors = []

        for message, status_code in responses:
            if status_code not in [200, 201]:
                errors.append(message)
                logger.error(f"{status_code}: {message}")
        errors_final.extend(errors)
    return errors_final, len(data_frame)
