"""
Module for working with tog database
"""

import json
import os
import sqlite3
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union


import psycopg2
import pytz

from skit_labels.utils import to_datetime
from skit_labels import constants as const
from skit_labels.types import (
    AudioSegmentTask,
    CallTranscriptionTask,
    ConversationTask,
    DataGenerationTask,
    DictTask,
    SimulatedCallTask,
    Task,
)


def build_task(
    d: Dict, task_type: str, data_id: Optional[str] = None, tz=pytz.UTC
) -> Task:
    """
    Create a task from given data dictionary.
    """

    if task_type == "conversation":
        task = ConversationTask.from_dict(d)

        # Since the reftime from db is in UTC, we convert it to our timezone. This
        # is needed as saying 12 pm means different things in different timezones
        # and can't be translated without doing something stupid.
        task.reftime = to_datetime(task.reftime)
        task.reftime = task.reftime.astimezone(tz).isoformat()
    elif task_type == "simulated_call":
        task = SimulatedCallTask.from_dict(d)
    elif task_type == "audio_segment":
        task = AudioSegmentTask.from_dict(d)
    elif task_type == "dict":
        task = DictTask.from_dict(d, data_id)
    elif task_type == "call_transcription":
        task = CallTranscriptionTask.from_dict(d, data_id)
    elif task_type == "data_generation":
        task = DataGenerationTask.from_dict(d)
    else:
        raise TypeError(f"Invalid task type {task_type} provided.")
    return task


class SqliteDatabase:
    """
    Class mapping to a local sqlite database file which can keep only one job.
    """

    def __init__(self, filepath: str):
        self.filepath = filepath
        self._initialize()

    def _initialize(self):
        self.conn = sqlite3.connect(self.filepath)
        c = self.conn.cursor()
        c.execute(
            """CREATE TABLE IF NOT EXISTS data (
            data_id INTEGER NOT NULL,
            data TEXT NOT NULL,
            tag TEXT NOT NULL,
            is_gold BOOLEAN NOT NULL,
            tagged_time TEXT,
            job_id TEXT NOT NULL
        )"""
        )
        self.conn.commit()

    def insert_rows(self, rows: List):
        """
        Write rows in database. Each item of row is a tuple of following elements:
        - data_id : int
        - data: Dict
        - tag: Dict
        - is_gold: bool
        - tagged_time: Optional[str]
        - job_id: str
        """

        c = self.conn.cursor()
        c.executemany(
            "INSERT INTO data (data_id, data, tag, is_gold, tagged_time, job_id) VALUES (?, ?, ?, ?, ?, ?)",
            [(i, json.dumps(d), json.dumps(t), g, tt, ji) for i, d, t, g, tt, ji in rows],
        )
        self.conn.commit()


class Database:
    """
    Class holding connection with backend database.
    """

    def __init__(
        self,
        db: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[Union[int, str]] = None,
    ):
        self._initialize(db=db, user=user, password=password, host=host, port=port)

    def _initialize(
        self,
        db: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[Union[int, str]] = None,
    ):
        db = db or os.getenv(const.TOGDB_DB, "tog")
        user = user or os.getenv(const.TOGDB_USER)
        password = password or os.getenv(const.TOGDB_PASSWORD)
        host = host or os.getenv(const.TOGDB_HOST, "localhost")
        port = port or os.getenv(const.TOGDB_PORT, "5432")

        if password is None:
            raise ValueError(
                "Credentials for Tog database not set. Check for missing environment variables."
            )

        self.conn = psycopg2.connect(
            host=host, database=db, user=user, password=password, port=port
        )

    def list_jobs(self) -> List[Dict]:
        with self.conn.cursor() as cur:
            # NOTE: We are not picking out task_type field since that
            #       collides with our task type names. Ideally we need to
            #       settle on same nomenclature.
            cur.execute(
                "SELECT id, name, description, config, language FROM jobs_job WHERE is_active"
            )

            jobs: List[Dict] = []
            for row in cur.fetchall():
                jobs.append(
                    {
                        "id": row[0],
                        "name": row[1],
                        "description": row[2],
                        "config": row[3],
                        "language": row[4],
                    }
                )

            return jobs


class AbstractJob(ABC):
    """
    Abstract class representing connection to a tagging job.
    """

    @abstractmethod
    def total(self, untagged=False) -> int:
        ...

    @abstractmethod
    def get_by_data_id(self, id: int, cache=True):
        ...

    @abstractmethod
    def get(self, untagged=False, itersize=1000, only_gold=False):
        ...


class Job(AbstractJob):
    """
    A Tog job which specifies a kind of tagging data set and problem.
    """

    def __init__(
        self,
        id: int,
        task_type="conversation",
        database: Optional[Database] = None,
        tz=pytz.UTC,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        db: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[Union[str, int]] = None,
    ):
        self.id = id
        # TODO: Check task validity right here
        self.task_type = task_type
        self.db_name = db
        self.db = database or Database(
            db=db, user=user, password=password, host=host, port=port
        )
        self._fetch_details()

        # Cache for keeping rows of job indexed by data_ids
        self.cache = {}
        self.tz = tz
        self.start_date = start_date
        self.end_date = end_date
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def __repr__(self):
        f"Job {self.id}: {self.name} [language: {self.lang}]\n{self.description}"

    def _fetch_details(self):
        """
        Save metadata about the current job id
        """

        with self.db.conn.cursor() as cur:
            # NOTE: We are not picking out task_type field since that
            #       collides with our task type names. Ideally we need to
            #       settle on same nomenclature.
            cur.execute(
                f"SELECT name, description, config, language FROM jobs_job WHERE id = {self.id}"
            )
            try:
                self.name, self.description, self.config, self.lang = cur.fetchone()
            except TypeError:
                raise ValueError("Invalid job id")

    def total(self, untagged=False, start_date=None, end_date=None):
        """
        Return total number of items for this job. If `untagged` is True, consider
        untagged items in counting too.
        """
        start_date = start_date or self.start_date
        end_date = end_date or self.end_date

        with self.db.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT count(*) FROM jobs_task INNER JOIN jobs_data ON
                    jobs_data.id = jobs_task.data_id
                WHERE
                    jobs_task.job_id = {self.id}
                    {'' if untagged else 'AND jobs_task.tag IS NOT NULL'}
                    {f"AND jobs_data.created_at >= '{start_date}'" if isinstance(start_date, str) else ''}
                    {f"AND jobs_data.created_at < '{end_date}'" if isinstance(end_date, str) else ''}
                """
            )
            n = cur.fetchone()[0]
        return n

    def get_by_data_id(self, id: int, cache=True, start_date=None, end_date=None):
        """
        Return task and tag using the data id
        """

        if id in self.cache:
            return self.cache[id]

        start_date = start_date or self.start_date
        end_date = end_date or self.end_date

        with self.db.conn.cursor() as cur:
            cur.execute(
                f"""
            SELECT
                jobs_data.data,
                jobs_task.tag,
                jobs_task.is_gold,
                jobs_task.tagged_time,
                jobs_data.id
            FROM jobs_task INNER JOIN jobs_data ON
                jobs_data.id = jobs_task.data_id
            WHERE
                jobs_task.job_id = {self.id} AND jobs_data.data_id = {id}
                {f"AND jobs_data.created_at >= '{start_date}'" if isinstance(start_date, str) else ''}
                {f"AND jobs_data.created_at < '{end_date}'" if isinstance(end_date, str) else ''}            
            """
            )
            try:
                task_dict, tag_list, is_gold, tagged_time = cur.fetchone()
            except TypeError:
                raise RuntimeError("No item found for given data id")

            task = build_task(task_dict, self.task_type, tz=self.tz)
            task.is_gold = bool(is_gold)
            tag = json.loads(tag_list)

            if cache:
                self.cache[id] = (task, tag, tagged_time)
            return task, tag, tagged_time

    def type(self):
        """
        List all available job_ids
        """
        import time
        time.sleep(.5)
        self.db = Database(
            db=self.db_name, user=self.user, password=self.password, host=self.host, port=self.port
        )
        with self.db.conn.cursor() as cursor:
            cursor.execute('select "taskType" from jobs_job where id = %s', (self.id,))
            record = cursor.fetchone()
            if record:
                return record[0]
            raise ValueError(f"Invalid job id {self.id}")

    def get_ids(
        self,
        untagged=False,
        itersize=1000,
        only_gold=False,
        start_date=None,
        end_date=None,
    ):
        """
        Return (generator) tagged tasks and tags from the database. Itersize sets
        the iteration size for the server sided cursor.

        If `untagged` is True, also return untagged items. This might be useful
        for checking, say, production metrics. If `only_gold` is True, return
        only items which are marked as gold.
        """
        start_date = start_date or self.start_date
        end_date = end_date or self.end_date

        query = f"""
        SELECT
            jobs_data.id
        FROM jobs_task INNER JOIN jobs_data ON
            jobs_data.id = jobs_task.data_id
        WHERE
            jobs_task.job_id = {self.id}
            {'' if untagged else 'AND jobs_task.tag IS NOT NULL'}
            {"AND jobs_task.is_gold = true" if only_gold else ''}
            {f"AND jobs_data.created_at >= '{start_date}'" if start_date else ''}
            {f"AND jobs_data.created_at < '{end_date}'" if end_date else ''}
        """
        with self.db.conn.cursor() as cur:
            cur.itersize = itersize
            cur.execute(query)
            data_ids = [x[0] for x in cur.fetchall()]
        self.db.conn.close()
        return data_ids


    def get(
        self,
        data_ids = [],
        untagged=False,
        itersize=1000,
        only_gold=False,
        start_date=None,
        end_date=None,
    ):
        """
        Return (generator) tagged tasks and tags from the database. Itersize sets
        the iteration size for the server sided cursor.

        If `untagged` is True, also return untagged items. This might be useful
        for checking, say, production metrics. If `only_gold` is True, return
        only items which are marked as gold.
        """
        start_date = start_date or self.start_date
        end_date = end_date or self.end_date

        query = f"""
        SELECT
            jobs_data.data,
            jobs_task.tag,
            jobs_task.is_gold,
            jobs_task.tagged_time,
            jobs_data.id
        FROM jobs_task INNER JOIN jobs_data ON
            jobs_data.id = jobs_task.data_id
        WHERE
            jobs_task.data_id IN {tuple(data_ids)}
        """
        db = Database(self.db_name, self.user, self.password, host=self.host, port=self.port)
        items = []
        with db.conn.cursor() as cur:
            cur.execute(query)

            for row in cur:
                task_dict, tag, is_gold, tagged_time, data_id = row
                task = build_task(task_dict, self.task_type, data_id, tz=self.tz)
                task.is_gold = bool(is_gold)
                items.append((task, tag, tagged_time))
        db.conn.close()
        return items


class LabelstudioJob(AbstractJob):
    """
    A Labelstudio job which specifies a kind of tagging data set and problem.
    """

    def __init__(
        self,
        id: int,
        task_type: str = "conversation",
        database: Optional[Database] = None,
        tz=pytz.UTC,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        db: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[Union[str, int]] = None,
    ):
        self.id = id
        self.db_name = db
        self.task_type = task_type
        self.db = database or Database(
            db=db, user=user, password=password, host=host, port=port
        )

        # Cache for keeping rows of job indexed by data_ids
        self.cache = {}
        self.tz = tz
        self.start_date = start_date
        self.end_date = end_date
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def total(self, untagged=False, start_date=None, end_date=None):
        """
        Return total number of items for this job. If `untagged` is True, consider
        untagged items in counting too.
        """
        start_date = start_date or self.start_date
        end_date = end_date or self.end_date

        with self.db.conn.cursor() as cur:
            cur.execute(
                f"""
                WITH labelstudio_data as (
                    SELECT 
                        project.id as "job_id",
                        task_completion.created_at as "task_completion_created_at"
                    FROM project
                    INNER JOIN task on project.id=task.project_id
                    INNER JOIN task_completion on task_completion.task_id=task.id
                    {"" if untagged else "WHERE task_completion.result != '[]'"}
                )
                
                SELECT COUNT(*) from labelstudio_data
                WHERE
                    job_id={self.id}
                    {f"AND task_completion_created_at >= '{start_date}'" if isinstance(start_date, str) else ''}
                    {f"AND task_completion_created_at <= '{end_date}'" if isinstance(end_date, str) else ''}
                """
            )
            n = cur.fetchone()[0]
        return n

    def type(self):
        """
        type of job
        """
        return ""
        

    def get_by_data_id(self, id: int, cache=True, start_date=None, end_date=None):
        """
        Return task and tag using the data id
        """

        if id in self.cache:
            return self.cache[id]

        start_date = start_date or self.start_date
        end_date = end_date or self.end_date

        with self.db.conn.cursor() as cur:
            cur.execute(
                f"""
                WITH labelstudio_data as (
                    SELECT 
                        project.id as "job_id",
                        task.data->> 'conversation_uuid' as "data_id",
                        task_completion.created_at as "task_completion_created_at",
                        task.data,
                        task_completion.result as "tag"
                    FROM project
                    INNER JOIN task on project.id=task.project_id
                    INNER JOIN task_completion on task_completion.task_id=task.id
                    {"" if untagged else "WHERE task_completion.result != '[]'"}
                )
                
                SELECT data_id, data, job_id, task_completion_created_at, tag FROM labelstudio_data
                WHERE
                    job_id={self.id} AND labelstudio_data.data_id={id}
                    {f"AND task_completion_created_at >= '{start_date}'" if isinstance(start_date, str) else ''}
                    {f"AND task_completion_created_at <= '{end_date}'" if isinstance(end_date, str) else ''}
                """
            )
            try:
                data_id, task_dict, job_id, tagged_time, tag_list = cur.fetchone()
            except TypeError:
                raise RuntimeError("No item found for given data id")

            task = build_task(task_dict, task_type='conversation', tz=self.tz)
            tag = json.loads(tag_list)[0]["value"]["choices"][0]

            if cache:
                self.cache[id] = (task, tag, tagged_time)
            return task, tag, tagged_time

    def get_ids(
        self,
        untagged=False,
        itersize=1000,
        only_gold=False,
        start_date=None,
        end_date=None,
    ):
        """
        Return (generator) tagged tasks and tags from the database. Itersize sets
        the iteration size for the server sided cursor.

        If `untagged` is True, also return untagged items. This might be useful
        for checking, say, production metrics. If `only_gold` is True, return
        only items which are marked as gold.
        """
        start_date = start_date or self.start_date
        end_date = end_date or self.end_date

        query = f"""
            WITH labelstudio_data as (
                SELECT 
                    project.id as "job_id",
                    task.data,
                    task_completion.created_at as "task_completion_created_at"
                FROM project
                INNER JOIN task on project.id=task.project_id
                INNER JOIN task_completion on task_completion.task_id=task.id
                {"" if untagged else "WHERE task_completion.result != '[]'"}
            )
            
            SELECT data->>'conversation_uuid' as "data_id" FROM labelstudio_data
            WHERE
                job_id={self.id}
                {f"AND task_completion_created_at >= '{start_date}'" if isinstance(start_date, str) else ''}
                {f"AND task_completion_created_at <= '{end_date}'" if isinstance(end_date, str) else ''}
            """
        with self.db.conn.cursor() as cur:
            cur.itersize = itersize
            cur.execute(query)
            data_ids = [x[0] for x in cur.fetchall()]
        self.db.conn.close()
        return data_ids


    def get(
        self,
        data_ids = [],
        untagged=False,
        itersize=1000,
        only_gold=False,
        start_date=None,
        end_date=None,
    ):
        """
        Return (generator) tagged tasks and tags from the database. Itersize sets
        the iteration size for the server sided cursor.

        If `untagged` is True, also return untagged items. This might be useful
        for checking, say, production metrics.
        """
        start_date = start_date or self.start_date
        end_date = end_date or self.end_date

        query = f"""
            WITH labelstudio_data as (
                SELECT 
                    project.id as "job_id",
                    task.data->> 'conversation_uuid' as "data_id",
                    task_completion.created_at as "task_completion_created_at",
                    task.data,
                    task_completion.result as "tag"
                FROM project
                INNER JOIN task on project.id=task.project_id
                INNER JOIN task_completion on task_completion.task_id=task.id
                {"" if untagged else "WHERE task_completion.result != '[]'"}
            )
            
            SELECT data_id, data, job_id, task_completion_created_at, tag FROM labelstudio_data
            WHERE
                job_id={self.id} AND data_id in {tuple(data_ids)}
                {f"AND task_completion_created_at >= '{start_date}'" if isinstance(start_date, str) else ''}
                {f"AND task_completion_created_at <= '{end_date}'" if isinstance(end_date, str) else ''}
            """
        
        db = Database(self.db_name, self.user, self.password, host=self.host, port=self.port)
        items = []
        with db.conn.cursor() as cur:
            cur.execute(query)

            for row in cur:
                data_id, task_dict, job_id, tagged_time, tag_list = row
                task = build_task(task_dict, "conversation", data_id, tz=self.tz)
                task.is_gold = True
                items.append((task, tag_list, tagged_time))
        db.conn.close()
        return items


class JobLocal(AbstractJob):
    """
    A tog job relying on local sqlite database.
    """

    def __init__(self, filepath: str, task_type="conversation", tz=pytz.UTC):
        self.task_type = task_type
        self.conn = sqlite3.connect(filepath)
        self.cache = {}
        self.tz = tz

    def total(self, untagged=False):
        """
        Return total number of items for this job. If `untagged` is True, consider
        untagged items in counting too.
        """

        cur = self.conn.cursor()
        cur.execute(
            f"SELECT count(*) FROM data {'' if untagged else 'WHERE tag IS NOT NULL'}"
        )
        return cur.fetchone()[0]

    def get_by_data_id(self, id: int, cache=True, show_source=False):
        """
        Return task and tag using the data id
        NOTE: We are not using cache here.
        """

        on_source = ", source" if show_source else ""

        cur = self.conn.cursor()
        cur.execute(
            f"""SELECT
          data, tag, is_gold, tagged_time{on_source}
        FROM data
        WHERE data_id = '{id}'
        """
        )

        try:
            if show_source:
                task_dict, tag_list, is_gold, tagged_time, source = cur.fetchone()
            else:
                task_dict, tag_list, is_gold, tagged_time = cur.fetchone()
        except TypeError:
            raise RuntimeError("No item found for given data id")

        task = build_task(json.loads(task_dict), self.task_type, tz=self.tz)
        task.is_gold = bool(is_gold)
        tag = json.loads(tag_list)
        if show_source:
            return task, tag, tagged_time, source
        return task, tag, tagged_time

    def get(
        self,
        untagged=False,
        itersize=1000,
        only_gold=False,
        show_source=False,
        show_ids=False,
    ):
        """
        Return (generator) tagged tasks and tags from the database.

        If `untagged` is True, also return untagged items. This might be useful
        for checking, say, production metrics. If `only_gold` is True, return
        only items which are marked as gold.
        """
        cur = self.conn.cursor()
        on_source = ", source" if show_source else ""
        on_ids = "data_id," if show_ids else ""
        cur.execute(
            f"""SELECT
          {on_ids} data, tag, is_gold, tagged_time {on_source}
        FROM data
        {'' if untagged else 'WHERE tag IS NOT NULL'}
        {'AND is_gold = 1' if only_gold else ''}
        """
        )

        for row in cur:
            if show_source and show_ids:
                id_, task_dict, tag, is_gold, tagged_time, source = row
            elif show_source:
                task_dict, tag, is_gold, tagged_time, source = row
            elif show_ids:
                id_, task_dict, tag, is_gold, tagged_time = row
            else:
                task_dict, tag, is_gold, tagged_time = row
            task = build_task(json.loads(task_dict), self.task_type, tz=self.tz)
            task.is_gold = bool(is_gold)
            if show_source and show_ids:
                yield id_, task, json.loads(tag), tagged_time, source
            elif show_source:
                yield task, json.loads(tag), tagged_time, source
            elif show_ids:
                yield id_, task, json.loads(tag), tagged_time
            else:
                yield task, json.loads(tag), tagged_time
