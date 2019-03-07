import os
from datetime import datetime
from unittest import TestCase

import freezegun
import pytest
from mod_ngarn.connection import get_connection
from mod_ngarn.utils import create_table
from mod_ngarn.worker import Job, JobRunner


def sync_dummy_job(text):
    return text


async def async_dummy_job(text):
    return text


def raise_dummy_job():
    raise KeyError()


@pytest.mark.asyncio
async def test_job_execute_builtin_success():
    await create_table()
    cnx = await get_connection()
    job = Job(cnx, 'modngarn_job', "job-1", "sum", 1, [[1, 2]], {})
    result = await job.execute()
    assert result == 3
    await cnx.close()


@pytest.mark.asyncio
async def test_job_execute_sync_fn_success():
    await create_table()
    cnx = await get_connection()
    job = Job(cnx, 'modngarn_job', "job-1", "tests.test_job.sync_dummy_job", 1, ["hello"], {})
    result = await job.execute()
    assert result == "hello"
    await cnx.close()


@pytest.mark.asyncio
async def test_job_execute_async_fn_success():
    await create_table()
    cnx = await get_connection()
    job = Job(cnx, 'modngarn_job', "job-1", "tests.test_job.async_dummy_job", 1, ["hello"], {})
    result = await job.execute()
    assert result == "hello"
    await cnx.close()


@pytest.mark.asyncio
async def test_job_success_record_to_db():
    await create_table()
    cnx = await get_connection()
    queue_table = os.getenv("DBTABLE", "modngarn_job")
    await cnx.execute(f'TRUNCATE TABLE "{queue_table}";')
    await cnx.execute(
        """
    INSERT INTO "{queue_table}" (id, fn_name, args) VALUES ('job-1', 'tests.test_job.async_dummy_job', '["hello"]')
    """.format(
            queue_table=queue_table
        )
    )
    job = Job(cnx, queue_table, "job-1", "tests.test_job.async_dummy_job", 0, ["hello"], {})
    result = await job.execute()
    assert result == "hello"
    job = await cnx.fetchrow(f'SELECT * FROM "{queue_table}" WHERE id=$1', "job-1")
    assert job["result"] == "hello"
    await cnx.execute(f'TRUNCATE TABLE "{queue_table}";')
    await cnx.close()


@pytest.mark.asyncio
async def test_job_failed_record_to_db():
    await create_table()
    cnx = await get_connection()
    queue_table = os.getenv("DBTABLE", "modngarn_job")
    await cnx.execute(f'TRUNCATE TABLE "{queue_table}";')
    await cnx.execute(
        """
    INSERT INTO "{queue_table}" (id, fn_name, args) VALUES ('job-2', 'tests.test_job.raise_dummy_job', '["hello"]')
    """.format(
            queue_table=queue_table
        )
    )
    job = Job(cnx, queue_table, "job-2", "tests.test_job.raise_dummy_job", 0)
    await job.execute()
    job_db = await cnx.fetchrow(f'SELECT * FROM "{queue_table}" WHERE id=$1', "job-2")
    assert job_db["result"] == None
    assert job_db["priority"] == 1
    assert job_db["reason"] == "KeyError()"

    job = Job(cnx, queue_table, "job-2", "tests.test_job.raise_dummy_job", 1)
    await job.execute()
    job_db = await cnx.fetchrow(f'SELECT * FROM "{queue_table}" WHERE id=$1', "job-2")
    assert job_db["result"] == None
    assert job_db["priority"] == 2
    assert job_db["reason"] == "KeyError()"
    await cnx.execute(f'TRUNCATE TABLE "{queue_table}";')
    await cnx.close()


@freezegun.freeze_time("2018-01-01T12:00:00+00:00")
@pytest.mark.asyncio
async def test_job_failed_exponential_delay_job_based_on_priority_and_max_priority_equal_30():
    await create_table()
    cnx = await get_connection()
    queue_table = os.getenv("DBTABLE", "modngarn_job")
    await cnx.execute(f'TRUNCATE TABLE "{queue_table}";')
    await cnx.execute(
        """
    INSERT INTO "{queue_table}" (id, fn_name, args) VALUES ('job-2', 'tests.test_job.raise_dummy_job', '["hello"]')
    """.format(
            queue_table=queue_table
        )
    )
    job = Job(cnx, queue_table, "job-2", "tests.test_job.raise_dummy_job", 0)
    # First failed, should delay 1 sec
    await job.execute()
    job_db = await cnx.fetchrow(f'SELECT * FROM "{queue_table}" WHERE id=$1', "job-2")
    assert job_db["priority"] == 1
    assert job_db["scheduled"].isoformat() == "2018-01-01T12:00:01+00:00"

    # Second failed, should delay 2 sec
    job.priority = job_db["priority"]
    await job.execute()
    job_db = await cnx.fetchrow(f'SELECT * FROM "{queue_table}" WHERE id=$1', "job-2")
    assert job_db["priority"] == 2
    assert job_db["scheduled"].isoformat() == "2018-01-01T12:00:02+00:00"

    # Third failed, should delay 4 sec
    job.priority = job_db["priority"]
    await job.execute()
    job_db = await cnx.fetchrow(f'SELECT * FROM "{queue_table}" WHERE id=$1', "job-2")
    assert job_db["priority"] == 3
    assert job_db["scheduled"].isoformat() == "2018-01-01T12:00:04+00:00"

    # 10th failed, should delay 1024 sec
    job.priority = 10
    await job.execute()
    job_db = await cnx.fetchrow(f'SELECT * FROM "{queue_table}" WHERE id=$1', "job-2")
    assert job_db["scheduled"].isoformat() == "2018-01-01T12:17:04+00:00"

    # 29th failed, priority should be 30
    job.priority = 29
    await job.execute()
    job_db = await cnx.fetchrow(f'SELECT * FROM "{queue_table}" WHERE id=$1', "job-2")
    assert job_db["priority"] == 30

    # 30th failed, priority should be 30
    job.priority = 30
    await job.execute()
    job_db = await cnx.fetchrow(f'SELECT * FROM "{queue_table}" WHERE id=$1', "job-2")
    assert job_db["priority"] == 30

    await cnx.execute(f'TRUNCATE TABLE "{queue_table}";')
    await cnx.close()


@pytest.mark.asyncio
async def test_job_runner_success_process():
    await create_table()
    cnx = await get_connection()
    queue_table = os.getenv("DBTABLE", "modngarn_job")
    await cnx.execute(f'TRUNCATE TABLE "{queue_table}";')
    await cnx.execute(
        """
    INSERT INTO "{queue_table}" (id, fn_name, args) VALUES ('job-1', 'tests.test_job.async_dummy_job', '["hello"]')
    """.format(
            queue_table=queue_table
        )
    )
    job_runner = JobRunner()
    await job_runner.run(queue_table)
    job = await cnx.fetchrow(f'SELECT * FROM "{queue_table}" WHERE id=$1', "job-1")
    assert job["result"] == "hello"
    await cnx.execute(f'TRUNCATE TABLE "{queue_table}";')
    await cnx.close()


@pytest.mark.asyncio
async def test_job_runner_can_define_limit():
    await create_table()
    cnx = await get_connection()
    await cnx.execute(f'TRUNCATE TABLE "modngarn_job";')
    await cnx.execute(
        """INSERT INTO "modngarn_job" (id, fn_name, args)
            SELECT 'job-' || s, 'tests.test_job.async_dummy_job', '["hello"]'
            FROM generate_series(0, 100) s;"""
    )
    job_runner = JobRunner()
    await job_runner.run('modngarn_job', limit=10)
    total_processed = await cnx.fetchval(f'SELECT COUNT(*) FROM "modngarn_job" WHERE executed IS NOT NULL')
    assert total_processed == 10
    await job_runner.run('modngarn_job', limit=10)
    total_processed = await cnx.fetchval(f'SELECT COUNT(*) FROM "modngarn_job" WHERE executed IS NOT NULL')
    assert total_processed == 20
    await cnx.execute(f'TRUNCATE TABLE "modngarn_job";')
    await cnx.close()
