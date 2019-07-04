import asyncio
import os
import re
import sys
from datetime import datetime, timedelta
from inspect import getmembers, getmodule, ismethod
from typing import Callable, Union

from asyncpg.connection import Connection

from .connection import get_connection


class ImportNotFoundException(Exception):
    pass


class ModuleNotfoundException(Exception):
    pass


def sql_table_name(queue_table: str) -> str:
    return (".").join([f'"{x}"' for x in queue_table.replace('"', "").split(".")])


def notify_channel(queue_table: str) -> str:
    return queue_table.replace('"', "").replace(".", "_")


async def get_fn_name(func: Union[str, Callable]) -> str:
    try:
        if isinstance(func, str):
            return func
        if ismethod(func):
            module_name = get_fn_name(dict(getmembers(func))["__self__"])
        else:
            module_name = getmodule(func).__name__
        name = func.__name__
        return ".".join([module_name, name])
    except AttributeError as e:
        raise ModuleNotfoundException(e)


async def import_fn(fn_name) -> Callable:
    access_path = fn_name.split(".")
    module = None
    try:
        for index in range(1, len(access_path)):
            try:
                # import top level module
                module_name = ".".join(access_path[:-index])
                module = __import__(module_name)
            except ImportError:
                continue
            else:
                for step in access_path[1:-1]:  # walk down it
                    module = getattr(module, step)
                break
        if module:
            return getattr(module, access_path[-1])
        else:
            return globals()["__builtins__"][fn_name]
    except KeyError as e:
        raise ImportNotFoundException(e)


async def create_table(name: str):
    print(f"Creating table {name}...")
    cnx = await get_connection()
    async with cnx.transaction():
        await cnx.execute(
            """CREATE TABLE IF NOT EXISTS {queue_table} (
                    id TEXT NOT NULL CHECK (id !~ '\\|/|\u2044|\u2215|\u29f5|\u29f8|\u29f9|\ufe68|\uff0f|\uff3c'),
                    fn_name TEXT NOT NULL,
                    args JSON DEFAULT '[]',
                    kwargs JSON DEFAULT '{{}}',
                    priority INTEGER DEFAULT 0,
                    created TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    scheduled TIMESTAMP WITH TIME ZONE,
                    executed TIMESTAMP WITH TIME ZONE,
                    canceled TIMESTAMP WITH TIME ZONE,
                    result JSON,
                    reason TEXT,
                    processed_time TEXT,
                    PRIMARY KEY (id)
                );
            """.format(
                queue_table=name
            )
        )

        await cnx.execute(
            """CREATE TABLE IF NOT EXISTS {queue_table}_error (
                    id TEXT NOT NULL CHECK (id !~ '\\|/|\u2044|\u2215|\u29f5|\u29f8|\u29f9|\ufe68|\uff0f|\uff3c'),
                    fn_name TEXT NOT NULL,
                    args JSON DEFAULT '[]',
                    kwargs JSON DEFAULT '{{}}',
                    message TEXT NOT NULL,
                    created TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    PRIMARY KEY (id, created)
                );
            """.format(
                queue_table=name
            )
        )

        await cnx.execute(
            f"""CREATE INDEX IF NOT EXISTS idx_pending_jobs ON {name} (executed) WHERE executed IS NULL;"""
        )

        await cnx.execute(
            """
        CREATE OR REPLACE FUNCTION {notify_channel}_notify_job()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        BEGIN
            NOTIFY {notify_channel};
            RETURN NEW;
        END;
        $$;

        DROP TRIGGER IF EXISTS {notify_channel}_notify_job_inserted ON {table_name};
        CREATE TRIGGER {notify_channel}_notify_job_inserted
        AFTER INSERT ON {table_name}
        FOR EACH ROW
        EXECUTE PROCEDURE {notify_channel}_notify_job();
        """.format(
                notify_channel=notify_channel(name), table_name=name
            )
        )
    print(f"Done")

async def wait_for_notify(queue_table: str, q: asyncio.Queue):
    """ Wait for notification and put channel to the Queue """
    notify_ch = notify_channel(queue_table)
    print(f"LISTENING ON {notify_ch}...")
    cnx = await get_connection()

    def notified(cnx: Connection, pid: int, channel: str, payload: str):
        print("Notified, shutting down...")
        asyncio.gather(cnx.close(), q.put(channel))

    await cnx.add_listener(notify_ch, notified)


async def shutdown(q: asyncio.Queue):
    """ Gracefully shutdown when something put to the Queue """
    await q.get()
    sys.exit()


async def delete_executed_job(cnx: Connection, queue_table: str) -> str:
    return await cnx.execute(
            """DELETE from {queue_table} where executed is not null""".format(
                queue_table=queue_table
            )
        )

async def delete_controller(queue_table: str, scheduled_time: int = 0) -> None:
    cnx = await get_connection()
    async with cnx.transaction():
        await delete_executed_job(cnx, queue_table)
        if scheduled_time:
            # scheduled_time = 'days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0'.format()
            print("before")
            import pdb; pdb.set_trace()
            await cnx.execute(
                """UPDATE {queue_table} SET scheduled=$1, executed=NULL WHERE id = $2;""".format(
                    queue_table=queue_table
                ), datetime.now() + timedelta(scheduled_time), 'delete_executed_task'
            )
            print("after")

async def add_delete_job(cnx: Connection, queue_table: str, scheduled_time: int = 0) -> str:
    return await cnx.execute(
            """INSERT INTO {queue_table} (id, fn_name, priority, args) VALUES ($1, $2, $3, $4)""".format(
                queue_table=queue_table
        ), 'delete_executed_task', await get_fn_name(delete_controller), 1, [queue_table, scheduled_time]
    )
