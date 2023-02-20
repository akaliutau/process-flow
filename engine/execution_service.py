import asyncio
import traceback
from asyncio import Queue, Task
from contextlib import suppress
from typing import Dict, Deque, Set

from engine.constants import TaskStatus, TaskResult
from engine.utils import log
from graph import Graph
from operators import Operator


class ExecutionService:
    MAX_SIZE_QUEUE = 4
    WORKERS = 3

    def __init__(self):
        self.loop = asyncio.get_event_loop()
        self.queue: Queue[Operator] = Queue(maxsize=ExecutionService.MAX_SIZE_QUEUE)
        self.workers = [self.loop.create_task(self._worker()) for _ in range(ExecutionService.WORKERS)]
        self.execution_task_map: Dict[str, Task] = dict()
        self.is_shutdown = False

    def _get_parent_tasks(self, operator: Operator) -> Set[Task]:
        ret = set()
        for parent in operator.parents:
            ret.add(self.execution_task_map.get(parent.task_id))
        return ret

    async def _execute_async(self, operator: Operator) -> None:
        parent_tasks = self._get_parent_tasks(operator)
        log.debug('task %s has %s parents' % (operator.task_id, len(parent_tasks)))
        # wait for parent tasks to complete - use a execution_task_map
        await asyncio.gather(*parent_tasks)
        try:
            operator.set_status(TaskStatus.RUNNING)
            await operator.exec()
            operator.result = TaskResult.SUCCESS
        except Exception as e:
            print(traceback.format_exc())
            self.result = TaskResult.ERROR
            self.error_msg = str(e)
            log.error(e)
        finally:
            operator.set_status(TaskStatus.FINISHED)

        log.info('processed {}'.format(operator.task_id))

    async def _worker(self):
        while not self.is_shutdown:
            op = await self.queue.get()
            task = self.loop.create_task(self._execute_async(op))
            self.execution_task_map[op.task_id] = task
            await task
            self.queue.task_done()

    async def execute_sequence(self, q: Deque[Operator]) -> None:
        while q and not self.is_shutdown:
            op = q.popleft()
            log.info('adding to the queue %s' % op.task_id)
            await self.queue.put(op)

        await self.queue.join()  # wait for all tasks to be processed

    async def async_execute_dag(self, graph: Graph[Operator]) -> None:
        while graph.has_next() and not self.is_shutdown:
            op = graph.next()
            log.info('adding to the queue %s' % op.task_id)
            await self.queue.put(op)

        await self.queue.join()  # wait for all tasks to be processed

    def execute_dag(self, graph: Graph[Operator]) -> None:
        graph.prepare()
        self.loop.run_until_complete(self.async_execute_dag(graph))

    def __enter__(self):
        log.info('started execution service')
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        log.debug('finalising execution service')
        self.is_shutdown = True
        for worker in self.workers:
            worker.cancel()
        try:
            pending = asyncio.all_tasks(self.loop)
            for t in pending:
                with suppress(asyncio.CancelledError):
                    t.cancel()
                    log.debug('waiting for task %s' % t)
                    self.loop.run_until_complete(t)
            log.info('stopping loop')
            self.loop.stop()
        except Exception as e:
            log.error(e)
        log.debug("Shutdown complete ...")
