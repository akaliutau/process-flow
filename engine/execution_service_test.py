import asyncio
import random
import unittest

from engine.utils import log
from graph import Graph
from execution_service import ExecutionService
from operators import Operator


class LogOperator(Operator):

    def __init__(self, task_id: str):
        super().__init__(task_id)

    async def exec(self):
        wait_time = random.randint(1, 30)
        log.info('processing {} will take {} second(s)'.format(self.task_id, wait_time))
        await asyncio.sleep(wait_time)  # I/O, context will switch to main function

        log.info("task %s is done!", self.task_id)


class TestingExecutionService(unittest.TestCase):

    def test_base_functions(self):
        op_09 = LogOperator('task 9')
        op_10 = LogOperator('task 10')
        op_11 = LogOperator('task 11')

        op_09 >> op_10
        op_09 >> op_11

        g = Graph([op_09, op_10,op_11])

        with ExecutionService() as es:
            es.execute_dag(g)


if __name__ == '__main__':
    unittest.main()
