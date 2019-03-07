import asyncio
import functools
import logging
import os
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, List

import asyncpg

from dataclasses import dataclass, field

from .connection import get_connection
from .utils import escape_table_name, import_fn

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="[%(asctime)s] - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("mod_ngarn")


@dataclass
class Job:
    cnx: asyncpg.Connection
    table: str
    id: str
    fn_name: str
    priority: int
    args: List[Any] = field(default_factory=list)
    kwargs: Dict = field(default_factory=dict)

    async def execute(self) -> Any:
        """ Execute the transaction """
        try:
            start_time = time.time()
            func = await import_fn(self.fn_name)
            if asyncio.iscoroutinefunction(func):
                result = await func(*self.args, **self.kwargs)
            else:
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None, functools.partial(func, *self.args, **self.kwargs)
                )
            processing_time = str(Decimal(str(time.time() - start_time)).quantize(Decimal(".001")))
            await self.success(result, processing_time)
            return result
        except Exception as e:
            log.error("Error#{}, {}".format(self.id, e.__repr__()))
            await self.failed(e.__repr__())

    async def success(self, result: Dict, processing_time: Decimal) -> str:
        """ Success execution handler """
        return await self.cnx.execute(
            f'UPDATE "{self.table}" SET result=$1, executed=NOW(), processed_time=$2 WHERE id=$3',
            result,
            processing_time,
            self.id,
        )

    async def failed(self, error: str) -> str:
        """ Failed execution handler """
        delay = 2 ** self.priority
        next_priority = 30 if self.priority >= 30 else self.priority + 1
        # max int = ( 2 ** 31 ) - 1
        next_schedule = datetime.now(timezone.utc) + timedelta(seconds=delay)
        log.error(
            "Rescheduled, delay for {} seconds ({}) ".format(delay, next_schedule.isoformat())
        )
        return await self.cnx.execute(
            f'UPDATE "{self.table}" SET priority=$4, reason=$2, scheduled=$3  WHERE id=$1',
            self.id,
            error,
            next_schedule,
            next_priority
        )


@dataclass
class JobRunner:
    async def fetch_job(
        self,
        cnx: asyncpg.Connection,
        queue_table: str
    ):

        result = await cnx.fetchrow(
            f"""SELECT id, fn_name, args, kwargs, priority FROM "{queue_table}"
            WHERE executed IS NULL
            AND (scheduled IS NULL OR scheduled < NOW())
            AND canceled IS NULL
            ORDER BY priority
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        """
        )

        if result:
            return Job(
                cnx,
                queue_table,
                result["id"],
                result["fn_name"],
                result["priority"],
                result["args"],
                result["kwargs"],
            )

    async def run(
        self, queue_table, limit: int = 300
    ):
        cnx = await get_connection()
        log.info(f"Running mod-ngarn, queue table name: {queue_table}, limit: {limit} jobs")
        for job_number in range(1, limit + 1):
            async with cnx.transaction(isolation="serializable"):
                job = await self.fetch_job(cnx, queue_table)
                if job:
                    log.info(f"Executing#{job_number}: \t{job.id}")
                    result = await job.execute()
                    log.info(f"Executed#{job_number}: \t{result}")
                else:
                    log.info("No job left, exiting...")
                    break
        await cnx.close()
