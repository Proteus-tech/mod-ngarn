import pytest
from mod_ngarn.worker import Job, JobRunner
from unittest import TestCase
from mod_ngarn.connection import get_connection


def sync_dummy_job(text):
    return text


async def async_dummy_job(text):
    return text


def raise_dummy_job():
    raise KeyError()


@pytest.mark.asyncio
async def test_job_execute_builtin_success():
    cnx = await get_connection()
    job = Job(cnx, 'job-1', 'sum', 1, [[1, 2]], {})
    result = await job.execute()
    assert result == 3
    await cnx.close()


@pytest.mark.asyncio
async def test_job_execute_sync_fn_success():
    cnx = await get_connection()
    job = Job(cnx, 'job-1', 'tests.test_job.sync_dummy_job', 1, ['hello'], {})
    result = await job.execute()
    assert result == 'hello'
    await cnx.close()


@pytest.mark.asyncio
async def test_job_execute_async_fn_success():
    cnx = await get_connection()
    job = Job(cnx, 'job-1', 'tests.test_job.async_dummy_job', 1, ['hello'], {})
    result = await job.execute()
    assert result == 'hello'
    await cnx.close()


@pytest.mark.asyncio
async def test_job_success_record_to_db():
    cnx = await get_connection()
    await cnx.execute(
        """
    INSERT INTO modngarn_job(id, fn_name, args) VALUES ('job-1', 'tests.test_job.async_dummy_job', '["hello"]')
    """
    )
    job = Job(cnx, 'job-1', 'tests.test_job.async_dummy_job', 0, ['hello'], {})
    result = await job.execute()
    assert result == 'hello'
    job = await cnx.fetchrow("SELECT * FROM modngarn_job WHERE id=$1", 'job-1')
    assert job['result'] == 'hello'
    await cnx.execute('TRUNCATE TABLE modngarn_job;')
    await cnx.close()


@pytest.mark.asyncio
async def test_job_failed_record_to_db():
    cnx = await get_connection()
    await cnx.execute(
        """
    INSERT INTO modngarn_job(id, fn_name, args) VALUES ('job-2', 'tests.test_job.raise_dummy_job', '["hello"]')
    """
    )
    job = Job(cnx, 'job-2', 'tests.test_job.raise_dummy_job', 0)
    await job.execute()
    job_db = await cnx.fetchrow("SELECT * FROM modngarn_job WHERE id=$1", 'job-2')
    assert job_db['result'] == None
    assert job_db['priority'] == 1
    assert job_db['reason'] == 'KeyError()'

    await job.execute()
    job_db = await cnx.fetchrow("SELECT * FROM modngarn_job WHERE id=$1", 'job-2')
    assert job_db['result'] == None
    assert job_db['priority'] == 2
    assert job_db['reason'] == 'KeyError()'
    await cnx.execute('TRUNCATE TABLE modngarn_job;')
    await cnx.close()


@pytest.mark.asyncio
async def test_job_runner_success_process():
    cnx = await get_connection()
    await cnx.execute(
        """
    INSERT INTO modngarn_job(id, fn_name, args) VALUES ('job-1', 'tests.test_job.async_dummy_job', '["hello"]')
    """
    )
    job_runner = JobRunner()
    await job_runner.run()
    job = await cnx.fetchrow("SELECT * FROM modngarn_job WHERE id=$1", 'job-1')
    assert job['result'] == 'hello'
    await cnx.execute('TRUNCATE TABLE modngarn_job;')
    await cnx.close()