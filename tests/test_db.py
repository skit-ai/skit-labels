import pytest

from tog.db import JobLocal, write_job_file


def test_sqlite_read_write(tmp_path):
    """
    Test if read after write keeps things the same.
    """

    rows = [
        (1, {"id": 1, "something": 2}, {}, False, None),
        (2, {"id": 2, "something": 1}, {}, False, None),
        (3, {"id": 3, "something": 3}, {}, True, None)
    ]

    sqlite_path = str(tmp_path / "job.sqlite")
    write_job_file(rows, sqlite_path)

    jb = JobLocal(sqlite_path, task_type="test_task")

    items = list(jb.get())

    # Assuming order is maintained
    for row, (d, t, tt) in zip(rows, items):
        assert row == (d.id, d.data, t, d.is_gold, tt)
